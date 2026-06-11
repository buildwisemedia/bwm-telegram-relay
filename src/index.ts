/**
 * bwm-telegram-relay — Priority ops-event filter + Telegram notification bridge.
 *
 * Routes:
 *   POST /event           — accepts operational_events payload from bwm-event-projector.
 *                           Filters per TELEGRAM_PRIORITY_EVENTS rules below. Sends to
 *                           Telegram if matches. Returns 200 even on filter-skip.
 *   POST /test            — admin-only (HMAC via TELEGRAM_RELAY_ADMIN_HMAC).
 *                           Sends "BWM Telegram Relay is live" test message.
 *   POST /capture-chat-id — Telegram bot webhook bootstrap. On first message from bot,
 *                           stores chat_id in KV under `bootstrap_chat_id`. One-time.
 *   GET  /health          — { status, version, telegram_configured, last_send_at }
 *   POST /send            — legacy: backwards-compat send route (X-BWM-Internal-Key auth).
 *   POST /webhook         — legacy: Telegram update receiver (PROJ-ATTN-ROUTING-001 Phase 6).
 *   scheduled             — emits daemon.heartbeat every 15 min.
 *
 * Token flow: BROKER_BEARER → bwm-cred-broker /mint → TELEGRAM_BOT_TOKEN
 *
 * PROJ-COMMS-CHANNEL-MIGRATION-001
 */

export interface Env {
  /** KV namespace for chat_id + dedup + metadata */
  BWM_TELEGRAM_KV: KVNamespace;
  /** Service binding to bwm-cred-broker */
  CRED_BROKER: Fetcher;
  /** Service binding to bwm-attention-router (PROJ-ATTN-ROUTING-001 Phase 6) */
  ATTENTION_ROUTER: Fetcher;
  /** Service binding to bwm-content-classifier (PROJ-TELEGRAM-MIGRATION-001 Phase 0 / Chip 7b) */
  CONTENT_CLASSIFIER: Fetcher;
  /** Bearer token for authenticating to bwm-cred-broker */
  BROKER_BEARER: string;
  /** Shared key for /send route auth (X-BWM-Internal-Key header) */
  BWM_INTERNAL_KEY: string;
  /** Shared key for calling bwm-attention-router /classify */
  ATTENTION_ROUTER_KEY: string;
  /** Shared key for calling bwm-content-classifier /classify */
  CONTENT_CLASSIFIER_KEY: string;
  /** Secret token Telegram sends in X-Telegram-Bot-Api-Secret-Token header */
  TELEGRAM_WEBHOOK_SECRET: string;
  /** HMAC secret for /test admin route */
  TELEGRAM_RELAY_ADMIN_HMAC: string;
  /** Supabase REST URL (for heartbeat writes) */
  SUPABASE_URL: string;
  /** Supabase service role key (for heartbeat writes) */
  SUPABASE_SERVICE_KEY: string;
  /** Environment tag */
  ENVIRONMENT: string;
  /**
   * Brain Proxy read key — x-brain-key header for brain.buildwisemedia.com.
   * Required for directive auto-handling: read current spec before appending rule.
   */
  BRAIN_KEY?: string;
  /**
   * Brain Proxy write key — x-write-key header for brain.buildwisemedia.com /write.
   * Required for directive auto-handling: append silent_handle rule to Attention-Routing-Spec.
   */
  BRAIN_WRITE_KEY?: string;
}

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const VERSION = "2.1.1";
const BROKER_INTERNAL_URL = "https://internal/mint";
const TELEGRAM_API_BASE = "https://api.telegram.org";
const KV_CHAT_ID_KEY = "robert_chat_id";
const KV_BOOTSTRAP_CHAT_ID_KEY = "bootstrap_chat_id";
const KV_BOOTSTRAP_DONE_KEY = "bootstrap_done";
const KV_LAST_SEND_AT_KEY = "last_send_at";
const KV_DEDUP_PREFIX = "dedup:";
const KV_OUTBOUND_LOG_PREFIX = "outbound:";
const DEDUP_TTL_SECONDS = 86_400; // 24 h
const OUTBOUND_LOG_TTL_SECONDS = 90 * 24 * 60 * 60;
const OUTBOUND_TEXT_MAX = 4_000;

// ─────────────────────────────────────────────────────────────────────────────
// Priority filter rules
// ─────────────────────────────────────────────────────────────────────────────

const SEND_NEVER = new Set([
  "daemon.heartbeat",
  "ad.scored",
  "ad.metric.recorded",
  "cap.observation",
  // Intermediate-state events (recommendations, not decisions). They are
  // unactionable for Robert — system says "should kill X", but no human is
  // expected to act ad-by-ad. These are dashboard data, not Telegram data.
  // Re-route to a daily digest if/when PROJ-META-ADS-AUTOMATION-001 adds one.
  "ad.kill_recommended",
  // Brain-selfheal calibration milestones. Autonomous progress, not Robert-action.
  // Re-add only when a human review gate is meaningful.
  "ad.round_locked",
]);

const SEND_ALWAYS = new Set<string>([
  // Empty: nothing is unconditionally sent. Use SEND_CONDITIONAL or
  // NAMESPACE_ALWAYS for events that should reach Robert.
]);

// Per-event-type rate-limit window (seconds). After firing, the same event_type
// is suppressed for this many seconds. 0 / missing = no rate limit.
// Suppressed events still INSERT into operational_events (Brain has the record);
// only the Telegram surface is rate-limited.
// (Currently empty — SEND_NEVER handles the previous high-volume offenders.
// Add entries here for future event types that should rate-limit rather than
// fully suppress.)
const RATE_LIMIT_SECONDS: Record<string, number> = {};

// Listing-engine + listing-namespace events route through the trg.* / listing.*
// namespace and always send (subject to rate limit). One-shot status events:
// trg.henry_round_complete, trg.tom_round_complete, trg.project_status, etc.
const NAMESPACE_ALWAYS = ["trg.", "listing."];

// For conditional types, the predicate receives the full event payload.
type EventPayload = Record<string, unknown>;

const SEND_CONDITIONAL: Record<string, (p: EventPayload) => boolean> = {
  "narrative": (p) => p["kind"] === "robert_priority" || p["urgency"] === "high",
  "incident.opened": (p) => {
    const sev = String(p["severity"] ?? "");
    // PROJ-APPROVAL-ACTION-001 Phase 3 closeout (2026-05-18): accept P2 so
    // SLA-breach incidents from bwm-sla-monitor surface on Robert's Telegram.
    return sev === "P0" || sev === "P1" || sev === "P2";
  },
  "task.queued": (p) =>
    p["assignee"] === "robert" && p["priority"] === "urgent",
};

function shouldSend(eventType: string, payload: EventPayload): boolean {
  if (SEND_NEVER.has(eventType)) return false;
  if (SEND_ALWAYS.has(eventType)) return true;
  if (NAMESPACE_ALWAYS.some((ns) => eventType.startsWith(ns))) return true;
  const cond = SEND_CONDITIONAL[eventType];
  if (cond) return cond(payload);
  return false;
}

// ─────────────────────────────────────────────────────────────────────────────
// Message formatting (Markdown V2)
// ─────────────────────────────────────────────────────────────────────────────

// Telegram MarkdownV2 requires escaping: _ * [ ] ( ) ~ ` > # + - = | { } . !
function escapeMd(text: string): string {
  return text.replace(/([_*[\]()~`>#+\-=|{}.!])/g, "\\$1");
}

function formatEventMessage(eventType: string, payload: EventPayload): string {
  const etype = escapeMd(eventType);
  const lines: string[] = [];

  // Header — emoji based on event type
  const emoji = emojiFor(eventType);
  lines.push(`${emoji} *${etype}*`);

  // Core payload fields
  const severity = payload["severity"];
  if (severity) lines.push(escapeMd(`Severity: ${severity}`));

  const scope = payload["scope"];
  if (scope) lines.push(escapeMd(`Scope: ${scope}`));

  const symptom = payload["symptom"];
  if (symptom) lines.push(escapeMd(String(symptom).slice(0, 200)));

  const description = payload["description"] ?? payload["message"];
  if (description) lines.push(escapeMd(String(description).slice(0, 300)));

  const daemon = payload["daemon"];
  if (daemon) lines.push(escapeMd(`Daemon: ${daemon}`));

  const kind = payload["kind"];
  if (kind && eventType !== "narrative") lines.push(escapeMd(`Kind: ${kind}`));

  const urgency = payload["urgency"];
  if (urgency) lines.push(escapeMd(`Urgency: ${urgency}`));

  const assignee = payload["assignee"];
  if (assignee) lines.push(escapeMd(`Assignee: ${assignee}`));

  const priority = payload["priority"];
  if (priority) lines.push(escapeMd(`Priority: ${priority}`));

  // Brain path link
  const brainPath = payload["brain_path"] ?? payload["path"];
  if (brainPath) {
    const escapedPath = escapeMd(String(brainPath));
    lines.push(`🔗 Brain: ${escapedPath}`);
  }

  // URL link — MarkdownV2 inline link: [text](url). Inside (...) only ) and \
  // need escaping; escaping dots/hyphens (as escapeMd does) breaks auto-link.
  const url = payload["url"];
  if (url) {
    const urlStr = String(url);
    const linkText = escapeMd(urlStr);
    const linkTarget = urlStr.replace(/[\\)]/g, "\\$&");
    lines.push(`🔗 URL: [${linkText}](${linkTarget})`);
  }

  // Free-form body — used by trg.* / listing.* status events AND narratives
  // (Bob-to-Robert messages). Capped at 8 lines; truncate-with-ellipsis if longer.
  const body = payload["body"];
  if (body && typeof body === "string") {
    const allLines = String(body).split("\n").filter((l) => l.trim());
    const shown = allLines.slice(0, 8);
    if (allLines.length > 0) lines.push("");
    for (const ln of shown) {
      lines.push(escapeMd(ln));
    }
    if (allLines.length > 8) lines.push(escapeMd("…"));
  }

  // Inline key-value metrics for status reports.
  const metrics = payload["metrics"];
  if (metrics && typeof metrics === "object" && !Array.isArray(metrics)) {
    if (Object.keys(metrics).length > 0) lines.push("");
    for (const [k, v] of Object.entries(metrics as Record<string, unknown>)) {
      lines.push(escapeMd(`${k}: ${v}`));
    }
  }

  // Call-to-action footer (for events that ask Robert to react/reply).
  const cta = payload["cta"];
  if (cta) {
    lines.push("");
    lines.push(escapeMd(String(cta)));
  }

  return lines.join("\n");
}

function emojiFor(eventType: string): string {
  if (eventType === "incident.opened") return "🚨";
  if (eventType.startsWith("ad.")) return "📊";
  if (eventType.startsWith("task.")) return "✅";
  if (eventType.startsWith("trg.") || eventType.startsWith("listing.")) return "🏘️";
  if (eventType === "narrative") return "📝";
  return "🔔";
}

// ─────────────────────────────────────────────────────────────────────────────
// Utilities
// ─────────────────────────────────────────────────────────────────────────────

const JSON_HEADERS = { "Content-Type": "application/json" };

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });
}

function supabaseConfigured(env: Env): boolean {
  return !!env.SUPABASE_URL && !!env.SUPABASE_SERVICE_KEY;
}

function supabaseRestUrl(env: Env, path: string): string {
  return `${env.SUPABASE_URL.replace(/\/+$/, "")}/rest/v1/${path}`;
}

function supabaseHeaders(env: Env, prefer?: string): HeadersInit {
  return {
    apikey: env.SUPABASE_SERVICE_KEY,
    Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
    "Content-Type": "application/json",
    ...(prefer ? { Prefer: prefer } : {}),
  };
}

async function sha256Hex(text: string): Promise<string> {
  const bytes = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
}

function redactOutboundText(text: string): string {
  const redacted = text
    .replace(/\b(Bearer|token|secret|password|api[_-]?key)\s*[:=]\s*\S+/gi, "$1=[REDACTED]")
    .replace(/\b[A-Za-z0-9_-]{40,}\b/g, "[REDACTED_TOKEN]");

  if (redacted.length <= OUTBOUND_TEXT_MAX) return redacted;
  return `${redacted.slice(0, OUTBOUND_TEXT_MAX)}\n[truncated]`;
}

type OutboundStatus = "queued" | "sent" | "failed" | "skipped";

interface OutboundLogInput {
  id?: string;
  sourceRoute: string;
  originEventId?: string | null;
  originEventType?: string | null;
  originSessionId?: string | null;
  chatId?: string | number | null;
  parseMode?: string | null;
  text: string;
  dedupeKey?: string | null;
  status?: OutboundStatus;
  error?: string | null;
  metadata?: Record<string, unknown>;
}

async function putOutboundKv(env: Env, id: string, row: Record<string, unknown>): Promise<void> {
  try {
    await env.BWM_TELEGRAM_KV.put(`${KV_OUTBOUND_LOG_PREFIX}${id}`, JSON.stringify(row), {
      expirationTtl: OUTBOUND_LOG_TTL_SECONDS,
    });
  } catch (err) {
    console.error(JSON.stringify({ where: "putOutboundKv", error: String(err), id }));
  }
}

async function createOutboundLog(env: Env, input: OutboundLogInput): Promise<string> {
  const id = input.id ?? ulid();
  const now = new Date().toISOString();
  const status = input.status ?? "queued";
  const row = {
    id,
    source_route: input.sourceRoute,
    origin_event_id: input.originEventId ?? null,
    origin_event_type: input.originEventType ?? null,
    origin_session_id: input.originSessionId ?? null,
    chat_id: input.chatId == null ? null : String(input.chatId),
    parse_mode: input.parseMode ?? null,
    text_sha256: await sha256Hex(input.text),
    text_redacted: redactOutboundText(input.text),
    dedupe_key: input.dedupeKey ?? null,
    status,
    error: input.error ?? null,
    metadata: input.metadata ?? {},
    queued_at: now,
    failed_at: status === "failed" ? now : null,
    sent_at: status === "sent" ? now : null,
    updated_at: now,
  };

  await putOutboundKv(env, id, row);

  if (!supabaseConfigured(env)) {
    console.warn(JSON.stringify({
      where: "createOutboundLog",
      warn: "missing_supabase_env",
      source_route: input.sourceRoute,
      id,
    }));
    return id;
  }

  try {
    const resp = await fetch(supabaseRestUrl(env, "telegram_outbound"), {
      method: "POST",
      headers: supabaseHeaders(env, "return=minimal,resolution=ignore-duplicates"),
      body: JSON.stringify(row),
    });
    if (!resp.ok) {
      console.error(JSON.stringify({
        where: "createOutboundLog",
        status: resp.status,
        detail: (await resp.text().catch(() => "")).slice(0, 300),
        id,
      }));
    }
  } catch (err) {
    console.error(JSON.stringify({ where: "createOutboundLog", error: String(err), id }));
  }

  return id;
}

async function updateOutboundLog(
  env: Env,
  id: string,
  patch: {
    status: OutboundStatus;
    telegramMessageId?: number | null;
    telegramResponse?: unknown;
    error?: string | null;
    metadata?: Record<string, unknown>;
  },
): Promise<void> {
  const now = new Date().toISOString();
  const kvKey = `${KV_OUTBOUND_LOG_PREFIX}${id}`;
  try {
    const existing = await env.BWM_TELEGRAM_KV.get(kvKey);
    const existingRow = existing ? JSON.parse(existing) as Record<string, unknown> : {};
    const nextRow: Record<string, unknown> = {
      ...existingRow,
      status: patch.status,
      updated_at: now,
    };
    if (patch.status === "sent") nextRow.sent_at = now;
    if (patch.status === "failed") nextRow.failed_at = now;
    if (patch.telegramMessageId != null) nextRow.telegram_message_id = patch.telegramMessageId;
    if (patch.telegramResponse !== undefined) nextRow.telegram_response = patch.telegramResponse;
    if (patch.error !== undefined) nextRow.error = patch.error;
    if (patch.metadata !== undefined) nextRow.metadata = patch.metadata;
    await putOutboundKv(env, id, nextRow);
  } catch (err) {
    console.error(JSON.stringify({ where: "updateOutboundLog.kv", error: String(err), id }));
  }

  if (!supabaseConfigured(env)) return;
  const body: Record<string, unknown> = {
    status: patch.status,
    updated_at: now,
  };
  if (patch.status === "sent") body.sent_at = now;
  if (patch.status === "failed") body.failed_at = now;
  if (patch.telegramMessageId != null) body.telegram_message_id = patch.telegramMessageId;
  if (patch.telegramResponse !== undefined) body.telegram_response = patch.telegramResponse;
  if (patch.error !== undefined) body.error = patch.error;
  if (patch.metadata !== undefined) body.metadata = patch.metadata;

  try {
    const resp = await fetch(supabaseRestUrl(env, `telegram_outbound?id=eq.${encodeURIComponent(id)}`), {
      method: "PATCH",
      headers: supabaseHeaders(env, "return=minimal"),
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      console.error(JSON.stringify({
        where: "updateOutboundLog",
        status: resp.status,
        detail: (await resp.text().catch(() => "")).slice(0, 300),
        id,
      }));
    }
  } catch (err) {
    console.error(JSON.stringify({ where: "updateOutboundLog", error: String(err), id }));
  }
}

// Crockford base-32 ULID
const CROCKFORD = "0123456789ABCDEFGHJKMNPQRSTVWXYZ";

function ulid(): string {
  const ts = Date.now();
  let tsStr = "";
  let t = ts;
  for (let i = 9; i >= 0; i--) {
    tsStr = CROCKFORD[t % 32]! + tsStr;
    t = Math.floor(t / 32);
  }
  const rand = new Uint8Array(10);
  crypto.getRandomValues(rand);
  let bits = 0n;
  for (const b of rand) bits = (bits << 8n) | BigInt(b);
  let randStr = "";
  for (let i = 0; i < 16; i++) {
    randStr = CROCKFORD[Number(bits & 31n)]! + randStr;
    bits >>= 5n;
  }
  return tsStr + randStr;
}

// ─────────────────────────────────────────────────────────────────────────────
// HMAC verification (for /test admin route)
// ─────────────────────────────────────────────────────────────────────────────

async function verifyAdminHmac(
  authHeader: string | null,
  secret: string | undefined,
): Promise<boolean> {
  if (!secret || !authHeader) return false;
  // Accepts "Bearer <hmac>" or just "<hmac>"
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : authHeader;
  const keyData = new TextEncoder().encode(secret);
  const key = await crypto.subtle.importKey(
    "raw",
    keyData,
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["verify"],
  );
  // Simple: token IS the pre-computed HMAC hex of the string "bwm-telegram-relay-test"
  const expected = "bwm-telegram-relay-test";
  const msgData = new TextEncoder().encode(expected);
  let tokenBytes: Uint8Array;
  try {
    // Parse hex
    if (token.length % 2 !== 0) return false;
    tokenBytes = new Uint8Array(token.match(/.{2}/g)!.map((h) => parseInt(h, 16)));
  } catch {
    return false;
  }
  return crypto.subtle.verify("HMAC", key, tokenBytes, msgData);
}

// ─────────────────────────────────────────────────────────────────────────────
// Cred-broker mint
// ─────────────────────────────────────────────────────────────────────────────

interface MintResponse {
  secret: string;
}

async function mintToken(env: Env, secretName: string): Promise<string> {
  const res = await env.CRED_BROKER.fetch(BROKER_INTERNAL_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.BROKER_BEARER}`,
      "Content-Type": "application/json",
      "User-Agent": `bwm-telegram-relay/${VERSION}`,
    },
    body: JSON.stringify({ name: secretName }),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Broker mint failed ${res.status}: ${body.slice(0, 200)}`);
  }

  const data = (await res.json()) as MintResponse;
  if (!data.secret) throw new Error("Broker returned empty secret");
  return data.secret;
}

// ─────────────────────────────────────────────────────────────────────────────
// Telegram send helper
// ─────────────────────────────────────────────────────────────────────────────

interface TelegramSendResult {
  ok: boolean;
  status: number;
  error?: string;
  telegramMessageId?: number;
  response?: unknown;
}

async function sendTelegramMessage(
  botToken: string,
  chatId: string | number,
  text: string,
  parseMode?: string,
  replyToMessageId?: number,
): Promise<TelegramSendResult> {
  const body: Record<string, unknown> = { chat_id: chatId, text };
  if (parseMode) body.parse_mode = parseMode;
  if (replyToMessageId) body.reply_to_message_id = replyToMessageId;

  const res = await fetch(`${TELEGRAM_API_BASE}/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const data = (await res.json().catch(() => ({
    ok: false,
    description: "telegram_non_json_response",
  }))) as {
    ok: boolean;
    description?: string;
    result?: { message_id?: number };
  };

  if (!res.ok || !data.ok) {
    return {
      ok: false,
      status: res.status,
      error: data.description ?? `HTTP ${res.status}`,
      response: data,
    };
  }
  return {
    ok: true,
    status: res.status,
    telegramMessageId: data.result?.message_id,
    response: data,
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// Route: GET /health
// ─────────────────────────────────────────────────────────────────────────────

async function handleHealth(env: Env): Promise<Response> {
  const chatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  const lastSendAt = await env.BWM_TELEGRAM_KV.get(KV_LAST_SEND_AT_KEY);
  const attentionRouterConfigured = !!env.ATTENTION_ROUTER && !!env.ATTENTION_ROUTER_KEY;
  const checks = {
    telegram_configured: !!chatId,
    supabase_configured: supabaseConfigured(env),
    attention_router_configured: attentionRouterConfigured,
    content_classifier_configured: !!env.CONTENT_CLASSIFIER && !!env.CONTENT_CLASSIFIER_KEY,
  };
  const ok = checks.telegram_configured && checks.supabase_configured && checks.attention_router_configured;
  return json({
    status: ok ? "ok" : "degraded",
    version: VERSION,
    ...checks,
    last_send_at: lastSendAt ?? null,
  }, ok ? 200 : 503);
}

// ─────────────────────────────────────────────────────────────────────────────
// Route: GET /audit/outbound (internal-key protected KV fallback audit)
// ─────────────────────────────────────────────────────────────────────────────

async function handleOutboundAudit(request: Request, env: Env): Promise<Response> {
  const key = request.headers.get("X-BWM-Internal-Key") ?? "";
  if (!env.BWM_INTERNAL_KEY || key !== env.BWM_INTERNAL_KEY) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }

  const url = new URL(request.url);
  const requestedLimit = Number(url.searchParams.get("limit") ?? "50");
  const limit = Number.isFinite(requestedLimit)
    ? Math.max(1, Math.min(Math.floor(requestedLimit), 200))
    : 50;

  const listed = await env.BWM_TELEGRAM_KV.list({ prefix: KV_OUTBOUND_LOG_PREFIX, limit });
  const rows = await Promise.all(
    listed.keys.map(async (item) => {
      const raw = await env.BWM_TELEGRAM_KV.get(item.name);
      if (!raw) return null;
      try {
        return JSON.parse(raw) as Record<string, unknown>;
      } catch {
        return { key: item.name, parse_error: true };
      }
    }),
  );

  return json({
    ok: true,
    source: "kv",
    count: rows.filter(Boolean).length,
    rows: rows
      .filter((row): row is Record<string, unknown> => !!row)
      .sort((a, b) => String(b.created_at ?? "").localeCompare(String(a.created_at ?? ""))),
  });
}

// ─────────────────────────────────────────────────────────────────────────────
// Route: POST /event
// ─────────────────────────────────────────────────────────────────────────────

interface OperationalEvent {
  id?: string;
  event_type?: string;
  payload?: EventPayload;
  client_id?: string | null;
  occurred_at?: string;
  session_id?: string;
}

async function handleEvent(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  let event: OperationalEvent;
  try {
    event = (await request.json()) as OperationalEvent;
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }

  const eventType = event.event_type ?? "";
  const payload = (event.payload ?? {}) as EventPayload;
  const eventId = event.id ?? "";

  // Filter check
  if (!shouldSend(eventType, payload)) {
    return json({ ok: true, action: "filtered", event_type: eventType });
  }

  const text = formatEventMessage(eventType, payload);
  const dedupeKey = eventId ? `${KV_DEDUP_PREFIX}${eventId}` : null;

  // Dedup check (24h TTL per event_id)
  if (dedupeKey) {
    const alreadySent = await env.BWM_TELEGRAM_KV.get(dedupeKey);
    if (alreadySent) {
      await createOutboundLog(env, {
        sourceRoute: "/event",
        originEventId: eventId,
        originEventType: eventType,
        originSessionId: event.session_id ?? null,
        parseMode: "MarkdownV2",
        text,
        dedupeKey,
        status: "skipped",
        metadata: { reason: "dedup_skip" },
      });
      return json({ ok: true, action: "dedup_skip", event_id: eventId });
    }
  }

  // Per-event-type rate limit (separate from per-event_id dedup).
  // Suppresses repeats of high-volume event types so the channel stays scannable.
  // Suppressed events still INSERT into operational_events; only the Telegram
  // surface is rate-limited.
  const rateLimitWindow = RATE_LIMIT_SECONDS[eventType];
  if (rateLimitWindow) {
    const rateLimitKey = `ratelimit:${eventType}`;
    const lastFiredStr = await env.BWM_TELEGRAM_KV.get(rateLimitKey);
    if (lastFiredStr) {
      const elapsedMs = Date.now() - parseInt(lastFiredStr, 10);
      if (elapsedMs < rateLimitWindow * 1000) {
        await createOutboundLog(env, {
          sourceRoute: "/event",
          originEventId: eventId || null,
          originEventType: eventType,
          originSessionId: event.session_id ?? null,
          parseMode: "MarkdownV2",
          text,
          dedupeKey,
          status: "skipped",
          metadata: {
            reason: "rate_limit_skip",
            retry_after_seconds: rateLimitWindow - Math.floor(elapsedMs / 1000),
          },
        });
        return json({
          ok: true,
          action: "rate_limit_skip",
          event_type: eventType,
          retry_after_seconds: rateLimitWindow - Math.floor(elapsedMs / 1000),
        });
      }
    }
    // Stamp now BEFORE the send (so concurrent calls don't both pass the gate).
    await env.BWM_TELEGRAM_KV.put(rateLimitKey, String(Date.now()), {
      expirationTtl: rateLimitWindow,
    });
  }

  // Get chat_id
  const chatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!chatId) {
    // No chat registered yet — log but return 200 to not block projector
    console.warn(JSON.stringify({ where: "handleEvent", warn: "no chat_id captured yet", event_type: eventType }));
    await createOutboundLog(env, {
      sourceRoute: "/event",
      originEventId: eventId || null,
      originEventType: eventType,
      originSessionId: event.session_id ?? null,
      parseMode: "MarkdownV2",
      text,
      dedupeKey,
      status: "skipped",
      metadata: { reason: "missing_chat_id" },
    });
    return json({ ok: true, action: "skipped_no_chat_id", event_type: eventType });
  }

  const outboundId = await createOutboundLog(env, {
    sourceRoute: "/event",
    originEventId: eventId || null,
    originEventType: eventType,
    originSessionId: event.session_id ?? null,
    chatId,
    parseMode: "MarkdownV2",
    text,
    dedupeKey,
    status: "queued",
  });

  // Mint token and send — fire-and-forget via waitUntil to not block response
  ctx.waitUntil(
    (async () => {
      let botToken: string;
      try {
        botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
      } catch (e) {
        console.error(JSON.stringify({ where: "handleEvent.mintToken", error: String(e) }));
        await updateOutboundLog(env, outboundId, {
          status: "failed",
          error: `token_mint_failed: ${String(e).slice(0, 200)}`,
        });
        return;
      }

      const result = await sendTelegramMessage(botToken, chatId, text, "MarkdownV2");

      if (!result.ok) {
        // If MarkdownV2 formatting caused a parse error, retry as plain text
        console.warn(JSON.stringify({ where: "handleEvent.send", warn: "MarkdownV2 failed, retrying plain", error: result.error }));
        const plainResult = await sendTelegramMessage(botToken, chatId, `[${eventType}] ${JSON.stringify(payload).slice(0, 400)}`);
        if (!plainResult.ok) {
          console.error(JSON.stringify({ where: "handleEvent.send.plain", error: plainResult.error }));
          await updateOutboundLog(env, outboundId, {
            status: "failed",
            error: plainResult.error ?? result.error ?? "telegram_send_failed",
            telegramResponse: { markdown: result.response, plain: plainResult.response },
            metadata: { fallback: "plain_failed" },
          });
          return;
        }
        await updateOutboundLog(env, outboundId, {
          status: "sent",
          telegramMessageId: plainResult.telegramMessageId,
          telegramResponse: plainResult.response,
          error: null,
          metadata: { fallback: "plain" },
        });
      } else {
        await updateOutboundLog(env, outboundId, {
          status: "sent",
          telegramMessageId: result.telegramMessageId,
          telegramResponse: result.response,
          error: null,
        });
      }

      // Mark as sent in dedup store
      if (eventId) {
        await env.BWM_TELEGRAM_KV.put(`${KV_DEDUP_PREFIX}${eventId}`, "1", {
          expirationTtl: DEDUP_TTL_SECONDS,
        });
      }
      await env.BWM_TELEGRAM_KV.put(KV_LAST_SEND_AT_KEY, new Date().toISOString());
    })(),
  );

  return json({ ok: true, action: "send_queued", event_type: eventType });
}

// ─────────────────────────────────────────────────────────────────────────────
// Route: POST /test (admin-only, HMAC-protected)
// ─────────────────────────────────────────────────────────────────────────────

async function handleTest(request: Request, env: Env): Promise<Response> {
  const authHeader = request.headers.get("Authorization");
  const valid = await verifyAdminHmac(authHeader, env.TELEGRAM_RELAY_ADMIN_HMAC);
  if (!valid) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }

  const chatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!chatId) {
    return json({ ok: false, error: "chat_id not captured yet — send /start to the bot first" }, 400);
  }

  const text = "✅ BWM Telegram Relay is live\n\nOps alerts will route here.";
  const outboundId = await createOutboundLog(env, {
    sourceRoute: "/test",
    chatId,
    text,
    status: "queued",
  });

  let botToken: string;
  try {
    botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
  } catch (e) {
    await updateOutboundLog(env, outboundId, {
      status: "failed",
      error: `token_mint_failed: ${String(e).slice(0, 200)}`,
    });
    return json({ ok: false, error: "token_mint_failed", detail: String(e).slice(0, 200) }, 502);
  }

  const result = await sendTelegramMessage(
    botToken,
    chatId,
    text,
  );

  if (!result.ok) {
    await updateOutboundLog(env, outboundId, {
      status: "failed",
      error: result.error ?? "telegram_send_failed",
      telegramResponse: result.response,
    });
    return json({ ok: false, error: "send_failed", detail: result.error }, 502);
  }

  await updateOutboundLog(env, outboundId, {
    status: "sent",
    telegramMessageId: result.telegramMessageId,
    telegramResponse: result.response,
    error: null,
  });

  return json({ ok: true, chat_id: chatId });
}

// ─────────────────────────────────────────────────────────────────────────────
// Route: POST /capture-chat-id (Telegram webhook bootstrap)
// ─────────────────────────────────────────────────────────────────────────────

async function handleCaptureChatId(request: Request, env: Env): Promise<Response> {
  // Check if bootstrap already done
  const done = await env.BWM_TELEGRAM_KV.get(KV_BOOTSTRAP_DONE_KEY);
  if (done) {
    return json({ ok: true, action: "already_captured", note: "bootstrap complete; this route is deactivated" });
  }

  let update: TelegramUpdate;
  try {
    update = (await request.json()) as TelegramUpdate;
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }

  const chatId = update.message?.chat?.id;
  if (!chatId) {
    return json({ ok: false, error: "no chat_id in update" }, 400);
  }

  // Store in both bootstrap_chat_id and robert_chat_id (primary send key)
  await env.BWM_TELEGRAM_KV.put(KV_BOOTSTRAP_CHAT_ID_KEY, String(chatId));
  await env.BWM_TELEGRAM_KV.put(KV_CHAT_ID_KEY, String(chatId));
  await env.BWM_TELEGRAM_KV.put(KV_BOOTSTRAP_DONE_KEY, "1");

  console.log(JSON.stringify({ where: "capture-chat-id", chat_id: chatId, event: "bootstrap_complete" }));

  return json({ ok: true, action: "captured", chat_id: chatId });
}

// ─────────────────────────────────────────────────────────────────────────────
// Route: POST /send (legacy backwards-compat)
// ─────────────────────────────────────────────────────────────────────────────

async function handleSend(request: Request, env: Env): Promise<Response> {
  const key = request.headers.get("X-BWM-Internal-Key") ?? "";
  if (!env.BWM_INTERNAL_KEY || key !== env.BWM_INTERNAL_KEY) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }

  let body: { text?: string; parse_mode?: string };
  try {
    body = (await request.json()) as { text?: string; parse_mode?: string };
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }

  const text = typeof body.text === "string" ? body.text.trim() : "";
  if (!text) {
    return json({ ok: false, error: "text is required" }, 400);
  }

  const chatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!chatId) {
    await createOutboundLog(env, {
      sourceRoute: "/send",
      text,
      parseMode: body.parse_mode ?? null,
      status: "skipped",
      metadata: { reason: "missing_chat_id" },
    });
    return json({ ok: false, error: "chat_id not captured yet — send /start to the bot to register" }, 400);
  }

  const outboundId = await createOutboundLog(env, {
    sourceRoute: "/send",
    chatId,
    text,
    parseMode: body.parse_mode ?? null,
    status: "queued",
  });

  let botToken: string;
  try {
    botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
  } catch (e) {
    console.error(JSON.stringify({ where: "handleSend.mintToken", error: String(e) }));
    await updateOutboundLog(env, outboundId, {
      status: "failed",
      error: `token_mint_failed: ${String(e).slice(0, 200)}`,
    });
    return json({ ok: false, error: "token_mint_failed", detail: String(e).slice(0, 200) }, 502);
  }

  const result = await sendTelegramMessage(botToken, chatId, text, body.parse_mode);
  if (!result.ok) {
    console.error(JSON.stringify({ where: "handleSend.sendMessage", error: result.error }));
    await updateOutboundLog(env, outboundId, {
      status: "failed",
      error: result.error ?? "telegram_send_failed",
      telegramResponse: result.response,
    });
    return json({ ok: false, error: "telegram_send_failed", detail: result.error }, 502);
  }

  await updateOutboundLog(env, outboundId, {
    status: "sent",
    telegramMessageId: result.telegramMessageId,
    telegramResponse: result.response,
    error: null,
  });
  return json({ ok: true, chat_id: chatId });
}

// ─────────────────────────────────────────────────────────────────────────────
// Route: POST /webhook (Telegram update receiver — PROJ-ATTN-ROUTING-001)
// ─────────────────────────────────────────────────────────────────────────────

async function handleWebhook(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
): Promise<Response> {
  const secretHeader = request.headers.get("X-Telegram-Bot-Api-Secret-Token") ?? "";
  if (!env.TELEGRAM_WEBHOOK_SECRET || secretHeader !== env.TELEGRAM_WEBHOOK_SECRET) {
    return json({ ok: false, error: "forbidden" }, 403);
  }

  let update: TelegramUpdate;
  try {
    update = (await request.json()) as TelegramUpdate;
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }

  // Emoji reactions arrive as message_reaction updates (not message).
  // Treat a reaction on one of our outbound messages as Robert's acknowledgment.
  if (update.message_reaction) {
    ctx.waitUntil(
      handleReactionUpdate(env, update).catch((e) =>
        console.error(JSON.stringify({ where: "webhook.reaction", error: String(e) })),
      ),
    );
    return json({ ok: true });
  }

  const message = update.message;
  if (!message) return json({ ok: true });

  const chatId = message.chat?.id;
  const fromId = message.from?.id;
  if (!chatId) return json({ ok: true });

  const inboundEventId = ulid();

  // Store chat_id on first contact
  const existingChatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!existingChatId) {
    await env.BWM_TELEGRAM_KV.put(KV_CHAT_ID_KEY, String(chatId));
    console.log(JSON.stringify({ where: "webhook", event: "chat_id_captured", chat_id: chatId, from_id: fromId }));
  }

  const text = message.text ?? "";
  ctx.waitUntil(
    (async () => {
      await persistInboundMessage(env, inboundEventId, update);
      let botToken = "";
      try {
        botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
      } catch (e) {
        console.error(JSON.stringify({ where: "webhook.mintToken", error: String(e) }));
      }

      // Receipt reaction: 👍 on Robert's message so he can SEE the system
      // received + logged it (Robert directive 2026-06-11 — emoji acks both
      // directions). Registered chat only; fail-soft.
      const registeredChat = existingChatId ?? String(chatId);
      if (botToken && String(chatId) === registeredChat) {
        try {
          const r = await fetch(`https://api.telegram.org/bot${botToken}/setMessageReaction`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              chat_id: chatId,
              message_id: message.message_id,
              reaction: [{ type: "emoji", emoji: "👍" }],
            }),
          });
          if (!r.ok) {
            console.warn(JSON.stringify({
              where: "webhook.receiptReaction", status: r.status,
              body: (await r.text().catch(() => "")).slice(0, 150),
            }));
          }
        } catch (e) {
          console.error(JSON.stringify({ where: "webhook.receiptReaction", error: String(e) }));
        }
      }

      let acted = false;
      if (botToken) {
        try {
          acted = await processTelegramReply(env, inboundEventId, message, text, chatId, botToken);
        } catch (e) {
          console.error(JSON.stringify({ where: "webhook.processTelegramReply", error: String(e) }));
        }
      }

      if (acted) {
        await updateInboundMessage(env, inboundEventId, {
          forward_status: "acted",
          forward_error: null,
        });
      } else {
        await routeInboundMessage(env, inboundEventId, message, text, String(fromId ?? chatId));
      }
    })().catch((e) => console.error(JSON.stringify({ where: "webhook.routeInboundMessage", error: String(e) }))),
  );

  // Respond to /start
  if (text.startsWith("/start")) {
    let botToken: string | null = null;
    try {
      botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
    } catch (e) {
      console.error(JSON.stringify({ where: "webhook./start.mintToken", error: String(e) }));
    }

    if (botToken) {
      const startReply = `✅ Connected. BWM ops alerts will route to this chat.\n\nchat_id: ${chatId}\nReady to receive.`;
      const outboundId = await createOutboundLog(env, {
        sourceRoute: "/webhook_reply",
        originEventId: inboundEventId,
        chatId,
        text: startReply,
        status: "queued",
      });
      ctx.waitUntil(
        sendTelegramMessage(botToken, chatId, startReply)
          .then((result) => updateOutboundLog(env, outboundId, result.ok
            ? {
              status: "sent",
              telegramMessageId: result.telegramMessageId,
              telegramResponse: result.response,
              error: null,
            }
            : {
              status: "failed",
              error: result.error ?? "telegram_send_failed",
              telegramResponse: result.response,
            }))
          .catch((e) => {
            console.error(JSON.stringify({ where: "webhook./start.reply", error: String(e) }));
            return updateOutboundLog(env, outboundId, {
              status: "failed",
              error: String(e).slice(0, 200),
            });
          }),
      );
    }
  }

  return json({ ok: true });
}

// ─────────────────────────────────────────────────────────────────────────────
// Inbound persistence + fanout status
// ─────────────────────────────────────────────────────────────────────────────

async function updateInboundMessage(
  env: Env,
  eventId: string,
  patch: Record<string, unknown>,
): Promise<void> {
  if (!supabaseConfigured(env)) return;

  const url = supabaseRestUrl(env, `telegram_inbound?event_id=eq.${encodeURIComponent(eventId)}`);
  try {
    const resp = await fetch(url, {
      method: "PATCH",
      headers: supabaseHeaders(env, "return=minimal"),
      body: JSON.stringify(patch),
    });
    if (!resp.ok) {
      const detail = (await resp.text().catch(() => "")).slice(0, 300);
      const basePatch: Record<string, unknown> = {};
      if ("forward_status" in patch) basePatch.forward_status = patch.forward_status;
      if ("forwarded_to_router_at" in patch) {
        basePatch.forwarded_to_router_at = patch.forwarded_to_router_at;
      }
      if (Object.keys(basePatch).length > 0 && Object.keys(basePatch).length < Object.keys(patch).length) {
        const retry = await fetch(url, {
          method: "PATCH",
          headers: supabaseHeaders(env, "return=minimal"),
          body: JSON.stringify(basePatch),
        });
        if (retry.ok) {
          console.warn(JSON.stringify({
            where: "updateInboundMessage",
            eventId,
            warn: "diagnostic_columns_unavailable_base_patch_applied",
          }));
          return;
        }
        console.error(JSON.stringify({
          where: "updateInboundMessage",
          eventId,
          status: retry.status,
          detail: (await retry.text().catch(() => "")).slice(0, 300),
        }));
        return;
      }
      console.error(JSON.stringify({
        where: "updateInboundMessage",
        eventId,
        status: resp.status,
        detail,
      }));
    }
  } catch (err) {
    console.error(JSON.stringify({ where: "updateInboundMessage", eventId, error: String(err) }));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Directive classification — matches Robert's natural "you handle it" language
// ─────────────────────────────────────────────────────────────────────────────

const DIRECTIVE_PATTERNS: Array<RegExp> = [
  /you\s+handle\s+(?:this|it|that)/i,
  /(?:not|isn'?t)\s+a\s+me\s+thing/i,
  /auto[- ]?handle/i,
  /handle\s+(?:this|it|that)\s+(?:silently|without\s+me|yourself)/i,
  /your\s+(?:call|decision|thing)(?:\s+(?:on|here))?/i,
  /you\s+own\s+(?:this|it|that)/i,
  /(?:update|update the)\s+brain/i,
  /system\s+should\s+handle/i,
  /(?:not|isn'?t)\s+on\s+my\s+radar/i,
  /(?:don'?t|stop)\s+surface/i,
];

const RESOLVE_CMDS = new Set([
  "resolve", "resolved", "done", "completed", "close", "approve", "approved",
]);

type ReplyIntent =
  | { kind: "resolve" }
  | { kind: "directive"; scope: string }
  | { kind: "unknown" };

function classifyReplyIntent(text: string): ReplyIntent {
  const trimmed = text.trim();
  if (RESOLVE_CMDS.has(trimmed.toLowerCase())) return { kind: "resolve" };
  for (const pat of DIRECTIVE_PATTERNS) {
    if (pat.test(trimmed)) {
      const match = trimmed.match(pat);
      return { kind: "directive", scope: match ? match[0] : trimmed.slice(0, 60) };
    }
  }
  return { kind: "unknown" };
}

// ─────────────────────────────────────────────────────────────────────────────
// Brain Proxy helpers (brain.buildwisemedia.com)
// ─────────────────────────────────────────────────────────────────────────────

const BRAIN_BASE_URL = "https://brain.buildwisemedia.com";
const ATTN_SPEC_PATH = "reference/Attention-Routing-Spec.md";

async function readFromBrain(env: Env, path: string): Promise<string | null> {
  if (!env.BRAIN_KEY) return null;
  try {
    const resp = await fetch(`${BRAIN_BASE_URL}/read?path=${encodeURIComponent(path)}`, {
      headers: {
        "x-brain-key": env.BRAIN_KEY,
        "User-Agent": "bwm-telegram-relay/directive-handler",
      },
    });
    if (!resp.ok) return null;
    const data = (await resp.json()) as { content?: string };
    return data.content ?? null;
  } catch {
    return null;
  }
}

async function writeToBrain(
  env: Env,
  path: string,
  content: string,
  message: string,
): Promise<{ ok: boolean; error?: string }> {
  if (!env.BRAIN_KEY || !env.BRAIN_WRITE_KEY) {
    return { ok: false, error: "brain keys not configured" };
  }
  try {
    const resp = await fetch(`${BRAIN_BASE_URL}/write`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-brain-key": env.BRAIN_KEY,
        "x-write-key": env.BRAIN_WRITE_KEY,
        "User-Agent": "bwm-telegram-relay/directive-handler",
      },
      body: JSON.stringify({ path, content, message }),
    });
    if (!resp.ok) {
      const detail = await resp.text().catch(() => "");
      return { ok: false, error: `brain write ${resp.status}: ${detail.slice(0, 200)}` };
    }
    return { ok: true };
  } catch (err) {
    return { ok: false, error: String(err) };
  }
}

/**
 * Appends a new silent_handle override entry to Attention-Routing-Spec.md.
 * Non-fatal if Brain keys are absent — Supabase log is still written.
 */
async function appendDirectiveOverride(
  env: Env,
  eventType: string,
  triggerText: string,
  scope: string,
  originEventId: string,
): Promise<{ ok: boolean; error?: string }> {
  const current = await readFromBrain(env, ATTN_SPEC_PATH);
  if (current === null) {
    console.warn("appendDirectiveOverride: could not read Attention-Routing-Spec — Supabase log still written");
    return { ok: false, error: "brain read failed" };
  }

  const ts = new Date().toISOString().slice(0, 10);
  const entry = [
    ``,
    `### Override \u2014 ${ts}`,
    `- **trigger:** "${triggerText}"`,
    `- **event_type:** ${eventType}`,
    `- **scope:** ${scope}`,
    `- **action:** silent_handle`,
    `- **origin_event_id:** ${originEventId}`,
    `- **locked:** ${new Date().toISOString()}`,
  ].join("\n");

  const OVERRIDES_HEADER = "## Directive Overrides";
  const nextContent = current.includes(OVERRIDES_HEADER)
    ? current.replace(OVERRIDES_HEADER, `${OVERRIDES_HEADER}\n${entry}`)
    : `${current.trimEnd()}\n\n${OVERRIDES_HEADER}\n${entry}\n`;

  return writeToBrain(
    env,
    ATTN_SPEC_PATH,
    nextContent,
    `directive override: ${scope} \u2192 silent_handle (triggered by Telegram reply on ${originEventId})`,
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared Supabase emit helper
// ─────────────────────────────────────────────────────────────────────────────

async function emitOperationalEvent(
  env: Env,
  eventType: string,
  payload: Record<string, unknown>,
  sessionId: string,
): Promise<boolean> {
  try {
    const resp = await fetch(`${env.SUPABASE_URL.replace(/\/+$/, "")}/rest/v1/operational_events`, {
      method: "POST",
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "return=minimal",
      },
      body: JSON.stringify({
        id: ulid(),
        event_type: eventType,
        client_id: null,
        payload,
        occurred_at: new Date().toISOString(),
        session_id: sessionId,
      }),
    });
    return resp.ok;
  } catch {
    return false;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Core reply processor
// ─────────────────────────────────────────────────────────────────────────────

async function processTelegramReply(
  env: Env,
  inboundEventId: string,
  message: TelegramMessage,
  text: string,
  chatId: string | number,
  botToken: string,
): Promise<boolean> {
  const replyTo = message.reply_to_message;
  if (!replyTo) return false;

  const replyToMessageId = replyTo.message_id;
  if (!replyToMessageId) return false;

  // Authorization gate: only the registered operator chat may trigger reply
  // processing — any other Telegram user who finds the bot must not be able
  // to act on (or probe) Robert-owned outbound messages.
  const registeredChatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!registeredChatId || String(chatId) !== registeredChatId) {
    console.warn(JSON.stringify({
      where: "processTelegramReply", phase: "unauthorized_chat",
      chat_id: String(chatId), from_id: message.from?.id ?? null,
    }));
    return false;
  }

  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_KEY) {
    console.warn("processTelegramReply: Supabase not configured");
    return false;
  }

  // 1. Look up origin event from outbound message log (scoped to this chat so
  // a colliding message_id from a foreign chat can never resolve).
  const lookupUrl =
    `${env.SUPABASE_URL.replace(/\/+$/, "")}/rest/v1/telegram_outbound` +
    `?telegram_message_id=eq.${replyToMessageId}` +
    `&chat_id=eq.${encodeURIComponent(String(chatId))}` +
    `&select=origin_event_id,origin_event_type&limit=1`;
  let originEventId = "";
  let originEventType = "";

  try {
    const resp = await fetch(lookupUrl, {
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        Accept: "application/json",
      },
    });
    if (!resp.ok) {
      console.error(`processTelegramReply: query failed ${resp.status} ${await resp.text().catch(() => "")}`);
      return false;
    }
    const data = (await resp.json()) as Array<{
      origin_event_id: string | null;
      origin_event_type: string | null;
    }>;
    if (!data || data.length === 0) {
      console.log(`processTelegramReply: no outbound log for message_id=${replyToMessageId}`);
      return false;
    }
    originEventId = data[0].origin_event_id ?? "";
    originEventType = data[0].origin_event_type ?? "";
  } catch (err) {
    console.error("processTelegramReply: query threw", err);
    return false;
  }

  if (!originEventId || !originEventType) {
    console.log("processTelegramReply: missing origin event details");
    return false;
  }

  // 2. Classify the reply intent
  const intent = classifyReplyIntent(text);
  if (intent.kind === "unknown") return false;

  const sessionId = `telegram-reply-${inboundEventId}`;
  let actionTaken = false;
  const confirmationLines: string[] = [];

  // ── DIRECTIVE PATH ──────────────────────────────────────────────────────────
  // "You handle this" / "not a me thing" / "update the brain" etc.
  // (a) write rule to Brain → (b) log to Supabase → (c) auto-resolve → (d) confirm
  if (intent.kind === "directive") {
    const { scope } = intent;

    // (a) Append override to Attention-Routing-Spec in Brain
    const brainResult = await appendDirectiveOverride(env, originEventType, text.trim(), scope, originEventId);
    if (!brainResult.ok) {
      console.warn(`processTelegramReply: brain write failed — ${brainResult.error}`);
    }

    // (b) Log directive override to Supabase for dashboard + EA pattern-detection
    await emitOperationalEvent(
      env,
      "narrative",
      {
        source: "telegram-reply",
        kind: "directive_override",
        event_type: originEventType,
        origin_event_id: originEventId,
        scope,
        trigger_text: text.trim(),
        action: "silent_handle",
        brain_write_ok: brainResult.ok,
        note: `Robert replied "${text.trim()}" \u2014 marked silent_handle. Brain spec updated: ${brainResult.ok ? "\u2713" : "FAILED"}.`,
      },
      sessionId,
    );

    // (c) Auto-resolve the underlying event
    if (originEventType === "incident.opened") {
      const ok = await emitOperationalEvent(
        env,
        "narrative",
        {
          source: "telegram-directive",
          kind: "incident-resolved",
          closes_incident_id: originEventId,
          note: `Auto-resolved via directive: "${text.trim()}"`,
        },
        sessionId,
      );
      actionTaken = ok;
      confirmationLines.push(
        "\u2705 *Directive applied*",
        `Brain rule added: \`${originEventType}\` \u2192 silent\\_handle`,
        "Incident auto\\-resolved\\.",
        brainResult.ok ? "\uD83D\uDD17 Attention\\-Routing\\-Spec updated\\." : "\u26A0\uFE0F Brain write failed \\(Supabase log preserved\\)\\.",
      );
    } else if (originEventType === "task.queued") {
      const ok = await emitOperationalEvent(
        env,
        "task.resolved",
        {
          source: "telegram-directive",
          task_id: originEventId,
          outcome: "auto-handled",
          resolution: `Auto-resolved via directive: "${text.trim()}"`,
        },
        sessionId,
      );
      actionTaken = ok;
      confirmationLines.push(
        "\u2705 *Directive applied*",
        `Brain rule added: \`${originEventType}\` \u2192 silent\\_handle`,
        "Task auto\\-resolved\\.",
        brainResult.ok ? "\uD83D\uDD17 Attention\\-Routing\\-Spec updated\\." : "\u26A0\uFE0F Brain write failed \\(Supabase log preserved\\)\\.",
      );
    } else if (originEventType === "client_state.transition") {
      const ok = await emitOperationalEvent(
        env,
        "narrative",
        {
          source: "telegram-directive",
          kind: "directive_override",
          closes_event_id: originEventId,
          action: "silent_handle",
          note: `Auto-handled via directive: "${text.trim()}"`,
        },
        sessionId,
      );
      actionTaken = ok;
      confirmationLines.push(
        "\u2705 *Directive applied*",
        `Brain rule added: \`${originEventType}\` \u2192 silent\\_handle`,
        "System will handle this class of notification automatically going forward\\.",
        brainResult.ok ? "\uD83D\uDD17 Attention\\-Routing\\-Spec updated\\." : "\u26A0\uFE0F Brain write failed \\(Supabase log preserved\\)\\.",
      );
    } else {
      // Unknown event type — directive still logged
      actionTaken = true;
      confirmationLines.push(
        "\u2705 *Directive logged*",
        `Event type \`${originEventType}\` noted \u2014 will not surface again\\.`,
        brainResult.ok ? "\uD83D\uDD17 Attention\\-Routing\\-Spec updated\\." : "\u26A0\uFE0F Brain write failed \\(Supabase log preserved\\)\\.",
      );
    }
  }

  // ── RESOLVE PATH ────────────────────────────────────────────────────────────
  if (intent.kind === "resolve") {
    if (originEventType === "incident.opened") {
      const ok = await emitOperationalEvent(
        env,
        "narrative",
        {
          source: "telegram-reply",
          kind: "incident-resolved",
          closes_incident_id: originEventId,
          note: `Incident resolved by Robert via Telegram reply: "${text}"`,
        },
        sessionId,
      );
      actionTaken = ok;
      confirmationLines.push("\u2705 Incident resolved successfully\\.");
    } else if (originEventType === "task.queued") {
      const ok = await emitOperationalEvent(
        env,
        "task.resolved",
        {
          task_id: originEventId,
          outcome: "done",
          resolution: `Resolved via Telegram reply: "${text}"`,
        },
        sessionId,
      );
      actionTaken = ok;
      confirmationLines.push("\u2705 Task resolved successfully\\.");
    }
  }

  // 3. Send confirmation back to Robert
  if (actionTaken && confirmationLines.length > 0) {
    try {
      await sendTelegramMessage(
        botToken,
        chatId,
        confirmationLines.join("\n"),
        "MarkdownV2",
        message.message_id,
      );
    } catch (err) {
      console.error("processTelegramReply: send confirmation threw", err);
    }
  }

  return actionTaken;
}

async function routeInboundMessage(
  env: Env,
  eventId: string,
  message: TelegramMessage,
  text: string,
  userId: string,
): Promise<void> {
  const trimmed = text.trim();

  if (!trimmed) {
    await updateInboundMessage(env, eventId, {
      forward_status: "skipped_empty",
      forward_error: null,
    });
    return;
  }

  if (trimmed.startsWith("/")) {
    await updateInboundMessage(env, eventId, {
      forward_status: "skipped_command",
      forward_error: null,
    });
    return;
  }

  if (!env.ATTENTION_ROUTER || !env.ATTENTION_ROUTER_KEY) {
    const error = "attention_router_not_configured";
    console.error(JSON.stringify({ where: "routeInboundMessage.attention_router", eventId, error }));
    await updateInboundMessage(env, eventId, {
      forward_status: "error",
      forward_error: error,
    });
    return;
  }

  try {
    const routerResp = await env.ATTENTION_ROUTER.fetch("https://internal/classify", {
      method: "POST",
      headers: {
        "X-BWM-Internal-Key": env.ATTENTION_ROUTER_KEY,
        "Content-Type": "application/json",
        "User-Agent": `bwm-telegram-relay/${VERSION}`,
      },
      body: JSON.stringify({
        source: "telegram",
        raw_text: trimmed,
        message_id: String(message.message_id),
        inbound_event_id: eventId,
        user_id: userId,
      }),
    });

    if (!routerResp.ok) {
      const detail = (await routerResp.text().catch(() => "")).slice(0, 300);
      console.error(JSON.stringify({
        where: "routeInboundMessage.attention_router",
        eventId,
        status: routerResp.status,
        detail,
      }));
      await updateInboundMessage(env, eventId, {
        forward_status: "error",
        router_status: routerResp.status,
        forward_error: detail || `attention_router_http_${routerResp.status}`,
      });
      return;
    }

    await updateInboundMessage(env, eventId, {
      forward_status: "forwarded",
      forwarded_to_router_at: new Date().toISOString(),
      router_status: routerResp.status,
      forward_error: null,
    });
  } catch (err) {
    const error = String(err).slice(0, 300);
    console.error(JSON.stringify({ where: "routeInboundMessage.attention_router", eventId, error }));
    await updateInboundMessage(env, eventId, {
      forward_status: "error",
      forward_error: error,
    });
    return;
  }

  // Forward non-command text to bwm-content-classifier (sibling of attention-router).
  // Classifier failures should not downgrade the attention-router forward status.
  if (!env.CONTENT_CLASSIFIER || !env.CONTENT_CLASSIFIER_KEY) {
    await updateInboundMessage(env, eventId, {
      classifier_error: "content_classifier_not_configured",
    });
    return;
  }

  try {
    const classifierResp = await env.CONTENT_CLASSIFIER.fetch("https://internal/classify", {
      method: "POST",
      headers: {
        "X-BWM-Internal-Key": env.CONTENT_CLASSIFIER_KEY,
        "Content-Type": "application/json",
        "User-Agent": `bwm-telegram-relay/${VERSION}`,
      },
      body: JSON.stringify({
        source: "telegram",
        raw_text: trimmed,
        message_id: String(message.message_id),
        event_id: eventId,
        user_id: userId,
      }),
    });

    if (!classifierResp.ok) {
      const detail = (await classifierResp.text().catch(() => "")).slice(0, 300);
      console.error(JSON.stringify({
        where: "routeInboundMessage.content_classifier",
        eventId,
        status: classifierResp.status,
        detail,
      }));
      await updateInboundMessage(env, eventId, {
        classifier_status: classifierResp.status,
        classifier_error: detail || `content_classifier_http_${classifierResp.status}`,
      });
      return;
    }

    await updateInboundMessage(env, eventId, {
      forwarded_to_classifier_at: new Date().toISOString(),
      classifier_status: classifierResp.status,
      classifier_error: null,
    });
  } catch (err) {
    const error = String(err).slice(0, 300);
    console.error(JSON.stringify({ where: "routeInboundMessage.content_classifier", eventId, error }));
    await updateInboundMessage(env, eventId, {
      classifier_error: error,
    });
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Emoji-ack handling (message_reaction updates)
// ─────────────────────────────────────────────────────────────────────────────

/** Persist a reaction to telegram_inbound + emit a telegram-ack narrative so
 *  downstream loops (feedback sweeps, follow-up watchers, sessions) can see
 *  that Robert acknowledged a specific outbound message. */
async function handleReactionUpdate(env: Env, update: TelegramUpdate): Promise<void> {
  const reaction = update.message_reaction;
  if (!reaction) return;

  // Authorization gate: acks only count from the registered operator chat.
  const registeredChatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!registeredChatId || String(reaction.chat.id) !== registeredChatId) {
    console.warn(JSON.stringify({
      where: "handleReactionUpdate", phase: "unauthorized_chat",
      chat_id: String(reaction.chat.id), from_id: reaction.user?.id ?? null,
    }));
    return;
  }

  const emojis = (reaction.new_reaction ?? [])
    .map((r) => r.emoji)
    .filter((e): e is string => !!e);
  const removed = emojis.length === 0;
  const eventId = ulid();
  const ackText = removed
    ? `(reaction removed from message ${reaction.message_id})`
    : `${emojis.join(" ")} (reaction to message ${reaction.message_id})`;

  if (supabaseConfigured(env)) {
    // Catch-all persistence — same table as text replies so telegram-recent.sh
    // and session cold-start loaders see acks inline.
    try {
      const resp = await fetch(supabaseRestUrl(env, "telegram_inbound"), {
        method: "POST",
        headers: supabaseHeaders(env, "return=minimal"),
        body: JSON.stringify({
          event_id: eventId,
          chat_id: reaction.chat.id,
          from_id: reaction.user?.id ?? null,
          message_id: reaction.message_id,
          text: ackText,
          entities: null,
          raw_update: update,
          forward_status: removed ? "dropped" : "acked",
        }),
      });
      if (!resp.ok) {
        console.error(JSON.stringify({
          where: "handleReactionUpdate.persist",
          status: resp.status,
          body: (await resp.text().catch(() => "")).slice(0, 200),
        }));
      }
    } catch (e) {
      console.error(JSON.stringify({ where: "handleReactionUpdate.persist", error: String(e) }));
    }
  }

  if (removed) return;

  // Resolve which outbound message was acked (telegram_outbound audit log).
  let origin: { origin_event_id?: string; origin_event_type?: string; source_route?: string;
                text_redacted?: string } | null = null;
  if (supabaseConfigured(env)) {
    try {
      const q = `telegram_outbound?chat_id=eq.${encodeURIComponent(String(reaction.chat.id))}` +
        `&telegram_message_id=eq.${reaction.message_id}` +
        `&select=origin_event_id,origin_event_type,source_route,text_redacted&limit=1`;
      const resp = await fetch(supabaseRestUrl(env, q), { headers: supabaseHeaders(env) });
      if (resp.ok) {
        const rows = (await resp.json()) as Array<Record<string, string>>;
        origin = rows[0] ?? null;
      }
    } catch (e) {
      console.error(JSON.stringify({ where: "handleReactionUpdate.lookup", error: String(e) }));
    }
  }

  // Narrative so the ack is visible in operational_events (Bob/Sarah/sweeps).
  if (supabaseConfigured(env)) {
    try {
      const preview = (origin?.text_redacted ?? "").slice(0, 160);
      const resp = await fetch(supabaseRestUrl(env, "operational_events"), {
        method: "POST",
        headers: supabaseHeaders(env, "return=minimal"),
        body: JSON.stringify({
          id: ulid(),
          event_type: "narrative",
          client_id: null,
          occurred_at: new Date().toISOString(),
          session_id: "daemon:bwm-telegram-relay",
          payload: {
            source: "bwm-telegram-relay webhook.reaction",
            kind: "telegram-ack",
            title: `Robert acked via ${emojis.join(" ")}`,
            body: origin
              ? `Reaction ${emojis.join(" ")} on outbound msg ${reaction.message_id}` +
                ` (route ${origin.source_route ?? "?"}` +
                `${origin.origin_event_id ? `, origin ${origin.origin_event_id}` : ""}): ${preview}`
              : `Reaction ${emojis.join(" ")} on msg ${reaction.message_id} (no outbound audit match)`,
            emoji: emojis,
            reacted_message_id: reaction.message_id,
            origin_event_id: origin?.origin_event_id ?? null,
            origin_event_type: origin?.origin_event_type ?? null,
            inbound_event_id: eventId,
          },
        }),
      });
      if (!resp.ok) {
        console.error(JSON.stringify({
          where: "handleReactionUpdate.narrative",
          status: resp.status,
        }));
      } else {
        console.log(JSON.stringify({
          where: "handleReactionUpdate", phase: "acked",
          emojis, message_id: reaction.message_id,
          origin_event_id: origin?.origin_event_id ?? null,
        }));
      }
    } catch (e) {
      console.error(JSON.stringify({ where: "handleReactionUpdate.narrative", error: String(e) }));
    }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Route: POST /admin/refresh-webhook (internal-key protected)
// Re-registers the Telegram webhook with allowed_updates including
// message_reaction — required once, and again after any webhook change,
// because Telegram only delivers reaction updates when explicitly subscribed.
// ─────────────────────────────────────────────────────────────────────────────

async function handleRefreshWebhook(request: Request, env: Env): Promise<Response> {
  const key = request.headers.get("X-BWM-Internal-Key") ?? "";
  if (!env.BWM_INTERNAL_KEY || key !== env.BWM_INTERNAL_KEY) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }
  if (!env.TELEGRAM_WEBHOOK_SECRET) {
    return json({ ok: false, error: "webhook_secret_not_configured" }, 500);
  }

  let botToken = "";
  try {
    botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
  } catch (e) {
    return json({ ok: false, error: "token_mint_failed", detail: String(e).slice(0, 200) }, 502);
  }

  const webhookUrl = `${new URL(request.url).origin}/webhook`;
  const resp = await fetch(`https://api.telegram.org/bot${botToken}/setWebhook`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      url: webhookUrl,
      secret_token: env.TELEGRAM_WEBHOOK_SECRET,
      allowed_updates: ["message", "message_reaction"],
    }),
  });
  const result = (await resp.json().catch(() => ({}))) as Record<string, unknown>;
  const info = await fetch(`https://api.telegram.org/bot${botToken}/getWebhookInfo`)
    .then((r) => r.json() as Promise<Record<string, unknown>>)
    .catch(() => ({} as Record<string, unknown>));
  // Strip nothing sensitive: webhook info contains the URL + counts only.
  return json({ ok: !!result.ok, set_webhook: result, webhook_info: info.result ?? info });
}

async function persistInboundMessage(
  env: Env,
  eventId: string,
  update: TelegramUpdate,
): Promise<void> {
  console.log(JSON.stringify({ where: "persistInboundMessage", phase: "enter", eventId }));
  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_KEY) {
    console.warn(JSON.stringify({
      where: "persistInboundMessage",
      phase: "missing_env",
      has_url: !!env.SUPABASE_URL,
      has_key: !!env.SUPABASE_SERVICE_KEY,
    }));
    return;
  }

  const message = update.message;
  if (!message) {
    console.log(JSON.stringify({ where: "persistInboundMessage", phase: "no_message" }));
    return;
  }

  const row = {
    event_id: eventId,
    chat_id: message.chat.id,
    from_id: message.from?.id ?? null,
    message_id: message.message_id,
    text: message.text ?? null,
    entities: (message as { entities?: unknown }).entities ?? null,
    raw_update: update,
    forward_status: "pending",
  };

  try {
    console.log(JSON.stringify({ where: "persistInboundMessage", phase: "fetch_start", eventId, message_id: row.message_id }));
    const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/telegram_inbound`, {
      method: "POST",
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "return=minimal,resolution=ignore-duplicates",
      },
      body: JSON.stringify(row),
    });
    if (!resp.ok) {
      console.error(JSON.stringify({
        where: "persistInboundMessage",
        phase: "non_2xx",
        status: resp.status,
        detail: (await resp.text().catch(() => "")).slice(0, 300),
      }));
    } else {
      console.log(JSON.stringify({ where: "persistInboundMessage", phase: "ok", eventId, status: resp.status }));
    }
  } catch (err) {
    console.error(JSON.stringify({ where: "persistInboundMessage", phase: "throw", error: String(err) }));
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Heartbeat (cron)
// ─────────────────────────────────────────────────────────────────────────────

async function emitHeartbeat(
  env: Env,
  sendCount: number,
  filterCount: number,
): Promise<void> {
  if (!env.SUPABASE_URL || !env.SUPABASE_SERVICE_KEY) {
    console.warn("heartbeat: missing SUPABASE_URL or SUPABASE_SERVICE_KEY; skipping");
    return;
  }

  const id = ulid();
  const occurred_at = new Date().toISOString();
  const payload = {
    source: "bwm-telegram-relay cron.heartbeat",
    kind: "daemon.heartbeat",
    daemon: "bwm-telegram-relay",
    cron_label: "bwm-telegram-relay",
    run_kind: "cron",
    rows_seen: sendCount,
    rows_skipped: filterCount,
    triggered_by: "cron:*/15",
  };

  try {
    const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/operational_events`, {
      method: "POST",
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "return=minimal",
      },
      body: JSON.stringify({
        id,
        event_type: "daemon.heartbeat",
        client_id: null,
        payload,
        occurred_at,
        session_id: "daemon:bwm-telegram-relay",
      }),
    });
    if (!resp.ok) {
      console.error(`heartbeat insert failed: ${resp.status} ${await resp.text().catch(() => "")}`);
    } else {
      console.log(JSON.stringify({ where: "heartbeat", id, occurred_at }));
    }
  } catch (err) {
    console.error(`heartbeat threw: ${(err as Error)?.message ?? err}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Telegram update types (minimal)
// ─────────────────────────────────────────────────────────────────────────────

interface TelegramUpdate {
  update_id: number;
  message?: TelegramMessage;
  message_reaction?: MessageReactionUpdated;
}

/** Telegram MessageReactionUpdated — only delivered when setWebhook
 *  allowed_updates includes "message_reaction" (see POST /admin/refresh-webhook). */
interface MessageReactionUpdated {
  chat: { id: number; type: string };
  message_id: number;
  user?: { id: number; first_name?: string; username?: string };
  date: number;
  old_reaction: Array<{ type: string; emoji?: string }>;
  new_reaction: Array<{ type: string; emoji?: string }>;
}

interface TelegramMessage {
  message_id: number;
  from?: { id: number; first_name?: string; username?: string };
  chat: { id: number; type: string };
  text?: string;
  date: number;
  reply_to_message?: TelegramMessage;
}

// ─────────────────────────────────────────────────────────────────────────────
// Worker export
// ─────────────────────────────────────────────────────────────────────────────

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";
    const method = request.method;

    try {
      if (method === "GET" && path === "/health") {
        return handleHealth(env);
      }
      if (method === "GET" && path === "/audit/outbound") {
        return handleOutboundAudit(request, env);
      }
      if (method === "POST" && path === "/event") {
        return handleEvent(request, env, ctx);
      }
      if (method === "POST" && path === "/test") {
        return handleTest(request, env);
      }
      if (method === "POST" && path === "/capture-chat-id") {
        return handleCaptureChatId(request, env);
      }
      // Legacy routes preserved for backwards compat
      if (method === "POST" && path === "/send") {
        return handleSend(request, env);
      }
      if (method === "POST" && path === "/webhook") {
        return handleWebhook(request, env, ctx);
      }
      if (method === "POST" && path === "/admin/refresh-webhook") {
        return handleRefreshWebhook(request, env);
      }
    } catch (err) {
      console.error(JSON.stringify({ where: "fetch.top", error: String(err), path, method }));
      return json({ ok: false, error: "internal_error", detail: String(err).slice(0, 200) }, 500);
    }

    return json({ ok: false, error: "not_found", path, method }, 404);
  },

  async scheduled(
    _event: ScheduledController,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<void> {
    // Emit daemon.heartbeat — counts pulled from KV (best-effort; 0 on cold start)
    ctx.waitUntil(
      emitHeartbeat(env, 0, 0).catch((e) =>
        console.error("scheduled heartbeat failed:", (e as Error)?.message ?? e),
      ),
    );
  },
} satisfies ExportedHandler<Env>;
