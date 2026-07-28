/**
 * bwm-telegram-relay — Priority ops-event filter + Telegram notification bridge.
 *
 * Routes:
 *   POST /notify          — One Wire typed gate (X-BWM-Internal-Key auth). Body:
 *                           {type: fire|call|signoff|fyi, punchline, stakes?, rec?,
 *                           ask?, options?, link?, key?, expires_at?, origin?,
 *                           judgment?}. fyi → Day Done digest queue (never live).
 *                           call/signoff → quiet-hours/Wednesday/budget gates. fire →
 *                           edit-in-place per key. Attention-Routing-Spec v2.0.0.
 *                           `judgment` (call/signoff only) opts the decision into
 *                           PROJ-UPLIFT-001 judgment capture: it is validated, stamped
 *                           into telegram_outbound metadata.wire.judgment, and a LOCAL
 *                           sweeper (bwm-ops-events bin/bwm-judgment-wire-sweep) seals
 *                           the row after Robert answers. The relay NEVER captures —
 *                           an invalid judgment object is dropped (reported in the
 *                           response) and the wire send proceeds unaffected (fail-safe
 *                           per reference/Judgment-Capture-Contract.md).
 *   POST /digest/flush    — compose + send the Day Done digest now (internal-key).
 *   POST /digest/day-ahead — compose + send the Day Ahead morning digest now
 *                           (internal-key). Redelivers deferred decisions first.
 *   POST /scorecard/run   — compute the 7-day comms scorecard, emit narrative
 *                           kind=comms-slo, return metrics (internal-key).
 *   POST /event           — accepts operational_events payload from bwm-event-projector.
 *                           Filters per TELEGRAM_PRIORITY_EVENTS rules below. Sends to
 *                           Telegram if matches; incident.opened routes through the
 *                           One Wire FIRE machinery (P-7 scopes → digest).
 *   POST /test            — admin-only (HMAC via TELEGRAM_RELAY_ADMIN_HMAC).
 *                           Sends "BWM Telegram Relay is live" test message.
 *   POST /capture-chat-id — Telegram bot webhook bootstrap. On first message from bot,
 *                           stores chat_id in KV under `bootstrap_chat_id`. One-time.
 *   GET  /health          — { status, version, telegram_configured, last_send_at }
 *   POST /send            — legacy compatibility input; hard-wraps to the FYI digest
 *                           and suppresses test/no-action payloads. Never sends live.
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
  /**
   * Deployed commit SHA, injected by scripts/deploy-verified.sh as
   * `--var GIT_SHA:$(git rev-parse HEAD)`. Surfaced on /health so a deploy can be
   * PROVEN to carry the intended code instead of merely reported as "deployed".
   */
  GIT_SHA?: string;
  /**
   * "true" ONLY in the [env.capture] worker. Diverts every operational-event emission
   * to KV `capture:events:*` and stamps `metadata.capture=true` on outbound rows, so
   * acceptance drills can exercise production-shaped code paths without writing a
   * single row into the production substrate (plan v6 §2.10).
   */
  CAPTURE_MODE?: string;
}

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const VERSION = "2.6.1";
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

// ── One Wire (Attention-Routing v2.0.0, Robert GO 2026-07-12) ────────────────
// Typed Robert-facing message contract. Four types exist: fire | call | signoff
// | fyi. fyi NEVER sends live — it queues for the daily digest. call/signoff
// respect quiet hours, Wednesdays, and the daily interrupt budget. fire always
// sends, and repeats with the same `key` EDIT the original message in place.
// Legacy /send is hard-wrapped to FYI here; there is no freeform live-send
// allowlist. Known smoke/no-action payloads are dropped before the digest queue.
const KV_WIRE_FIRE_PREFIX = "wire:fire:"; // wire:fire:<hash> → live incident msg
const KV_WIRE_OPEN_PREFIX = "wire:open:"; // wire:open:<ref> → unanswered call/signoff
const KV_WIRE_DIGESTQ_PREFIX = "wire:digestq:"; // wire:digestq:<ulid> → queued digest item
const KV_WIRE_BUDGET_PREFIX = "wire:budget:"; // wire:budget:<ET date> → live interrupts today
// The fire registry is RESOLUTION-bound, not time-bound (plan v6 §2.5). There is no
// TTL: an unresolved incident that aged out of a 24h window was free to open a NEW
// message, which is the "no re-pings" promise breaking itself. Anything still open
// past this staleness goes to the TEAM as a task via the collapse sweep — Robert is
// never pinged a second time for the same fingerprint.
const WIRE_FIRE_STALE_MS = 7 * 24 * 60 * 60 * 1000;
const WIRE_OPEN_TTL_SECONDS = 7 * 24 * 60 * 60; // unanswered items resurface via digest
const WIRE_DIGESTQ_TTL_SECONDS = 3 * 24 * 60 * 60; // queue survives a missed digest run
const WIRE_BUDGET_TTL_SECONDS = 48 * 60 * 60;
// Robert-directed raise 2026-07-26 ("we run into that 3 limit a lot"): up to
// FOUR unsolicited CALL/SIGNOFF live interrupts per ET day. Combined-lane
// acceptance target moves ≤3 → ≤6/day (fires exempt + rare; replies free;
// overflow still defers to the Day Done digest). Spec §12 amended same day.
export const WIRE_INTERRUPT_HARD_CAP = 4;
export const WIRE_DIGEST_LIVE_REDELIVERY = false;
const WIRE_QUIET_START_HOUR = 21; // ET; call/signoff queue to digest 21:00–08:00
const WIRE_QUIET_END_HOUR = 8;
const WIRE_FIRE_MAX_UPDATES = 6; // most-recent update lines kept in the edited message
// Set when bwm-board deploys (due 2026-07-17); empty = omit board links.
const BOARD_URL = "";

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
    // P2 is team-owned operational work. If a P2 genuinely needs Robert's
    // judgment, its producer must emit an explicit CALL/SIGNOFF instead of a
    // raw incident dump. This keeps technical failures out of the live lane.
    return sev === "P0" || sev === "P1";
  },
  "task.queued": (p) =>
    p["assignee"] === "robert" && p["priority"] === "urgent",
};

export function shouldSend(eventType: string, payload: EventPayload): boolean {
  if (SEND_NEVER.has(eventType)) return false;
  if (SEND_ALWAYS.has(eventType)) return true;
  if (NAMESPACE_ALWAYS.some((ns) => eventType.startsWith(ns))) return true;
  const cond = SEND_CONDITIONAL[eventType];
  if (cond) return cond(payload);
  return false;
}

// Resolve the Telegram-surface rate-limit for an event: a { window, key } or null.
// The event row is ALWAYS persisted upstream (Brain/operational_events has the
// record); this ONLY throttles the Telegram surface so a storm stays scannable.
//
// incident.opened is keyed by (kind, scope) with a 1h window rather than by bare
// event_type: a repeated security-lane storm (e.g. a watcher emitting one
// unauthorized-secret-write per file — 2026-07-05, 3000+/day) coalesces to ONE
// Telegram alert/hour, while the FIRST incident of any distinct kind still fires
// immediately (first-of-kind always surfaces). This preserves migration 118's
// intent — real P0 secret events must not be buried — while killing the flood.
// Synchronous non-crypto 64-bit string digest (djb2 ⊕ sdbm → 16 hex chars). Used
// only to bound rate-limit KV keys to a fixed, UTF-8-safe length. 64 bits keeps
// accidental collisions negligible at incident volume (a 32-bit hash collides
// around ~77k keys); a collision would put two distinct incidents in one bucket
// and suppress the later one. Two independent hashes so a collision needs both to
// collide at once. Not security-grade — incident emitters are internal/authed —
// but wide enough that a same-bucket suppression won't happen by accident.
function shortHash(s: string): string {
  let h1 = 5381;
  let h2 = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    h1 = ((h1 << 5) + h1 + c) | 0; // djb2
    h2 = (c + (h2 << 6) + (h2 << 16) - h2) | 0; // sdbm
  }
  return (
    (h1 >>> 0).toString(16).padStart(8, "0") +
    (h2 >>> 0).toString(16).padStart(8, "0")
  );
}

function rateLimitFor(
  eventType: string,
  payload: EventPayload,
  clientId?: string | null,
): { window: number; key: string } | null {
  if (eventType === "incident.opened") {
    // kind falls back to symptom, which can be long free-form text (the emitter
    // truncates symptom to 1500 chars) — and scope is free-form too. Workers KV
    // keys are capped at 512 bytes, so hash the discriminator to keep the key
    // bounded; an oversized key would make KV.get/put THROW and fail the alert
    // before it is logged or sent (codex review 2026-07-05). Same discriminator →
    // same hash → coalesces; a distinct one → distinct hash → still fires.
    //
    // SEVERITY is part of the key so a P0 escalation of a kind that already fired
    // at a lower severity within the window is NOT buried — it hashes to a
    // different key and surfaces immediately. The same-severity storm (e.g. the
    // all-P0 secret-write flood) still coalesces to one alert/window.
    // client is part of the key so a per-client incident is never suppressed by a
    // matching incident from a different client (client_id is a TOP-LEVEL event
    // field; also accept a payload copy). null/absent client = the workspace lane
    // (e.g. the secret-write storm), which coalesces among itself.
    const client = String(clientId ?? payload["client_id"] ?? "");
    const severity = String(payload["severity"] ?? "");
    const kind = String(payload["kind"] ?? payload["symptom"] ?? "unknown");
    const scope = String(payload["scope"] ?? "");
    return {
      window: 3600,
      key: `ratelimit:incident.opened:${shortHash(`${client}\x00${severity}\x00${kind}\x00${scope}`)}`,
    };
  }
  const window = RATE_LIMIT_SECONDS[eventType];
  return window ? { window, key: `ratelimit:${eventType}` } : null;
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

  // Whole unit or omit it (plan v6 §2.6) — a symptom cut at 200 chars is the
  // "…and producti" fragment class Robert rejected. Over-long values are dropped
  // rather than mangled; the full value stays in operational_events.
  const symptom = payload["symptom"];
  if (symptom && String(symptom).length <= 400) lines.push(escapeMd(String(symptom)));

  const description = payload["description"] ?? payload["message"];
  if (description && String(description).length <= 600) lines.push(escapeMd(String(description)));

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

/** Guard for the THREE remaining direct operational_events writers (the reaction
 *  wire-decision, the reaction ack narrative, and the heartbeat). emitOperationalEvent
 *  already diverts to the KV sink in capture mode; these three predate it and still
 *  POST directly, so without this they would write straight into the production
 *  substrate from a drill worker. §2.9 consolidates them onto the single hardened
 *  helper in Phase 3 — until then, this is the seal.
 *
 *  Note this is deliberately NOT folded into supabaseConfigured(): telegram_outbound
 *  audit rows MUST still be written in capture mode (stamped metadata.capture=true) —
 *  they are the evidence the acceptance drills read. Only fan-out-bearing
 *  operational_events writes are blocked. */
function canWriteOperationalEventsDirectly(env: Env): boolean {
  return supabaseConfigured(env) && !isCaptureMode(env);
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
  /** Live delivery must not proceed if its durable SQL audit row is missing. */
  requireSupabase?: boolean;
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

export async function createOutboundLog(env: Env, input: OutboundLogInput): Promise<string> {
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
    if (input.requireSupabase) {
      throw new Error(`outbound_audit_required_but_unconfigured:${id}`);
    }
    return id;
  }

  try {
    const resp = await fetch(supabaseRestUrl(env, "telegram_outbound"), {
      method: "POST",
      headers: supabaseHeaders(env, "return=minimal,resolution=ignore-duplicates"),
      body: JSON.stringify(row),
    });
    if (!resp.ok) {
      const detail = (await resp.text().catch(() => "")).slice(0, 300);
      console.error(JSON.stringify({
        where: "createOutboundLog",
        status: resp.status,
        detail,
        id,
      }));
      if (input.requireSupabase) {
        throw new Error(`outbound_audit_insert_failed:${resp.status}:${detail}`);
      }
    }
  } catch (err) {
    console.error(JSON.stringify({ where: "createOutboundLog", error: String(err), id }));
    if (input.requireSupabase) throw err;
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

/** Sentinel returned by mintToken in the capture worker. A real bot token can never
 *  equal this literal, so the short-circuit below cannot fire in production. */
const CAPTURE_BOT_TOKEN = "capture-mode:no-telegram-io";

async function mintToken(env: Env, secretName: string): Promise<string> {
  // CAPTURE (§2.10): never mint a real credential in the drill worker, and never let
  // a drill reach the Telegram API. Robert's phone is not a test fixture.
  if (isCaptureMode(env)) return CAPTURE_BOT_TOKEN;
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

/** Synthetic Telegram message ids for capture drills — monotonic within an isolate so
 *  a send/edit pair is observably ONE message, which is exactly what flap-replay
 *  asserts. */
let captureMessageSeq = 9_000_000;

async function sendTelegramMessage(
  botToken: string,
  chatId: string | number,
  text: string,
  parseMode?: string,
  replyToMessageId?: number,
): Promise<TelegramSendResult> {
  // CAPTURE short-circuit (§2.10): record the send, contact nobody.
  if (botToken === CAPTURE_BOT_TOKEN) {
    const id = ++captureMessageSeq;
    return { ok: true, status: 200, telegramMessageId: id, response: { capture: true, message_id: id } };
  }
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
    // The deployed commit. scripts/deploy-verified.sh asserts this equals the SHA it
    // deployed — "deployed" is a claim, a matching git_sha is the proof.
    git_sha: env.GIT_SHA ?? null,
    capture_mode: isCaptureMode(env),
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

  // One Wire: incidents route through the FIRE machinery — same key discriminator
  // as the old rate limiter (client+severity+kind+scope), but a repeat EDITS the
  // original message instead of being dropped or re-pinged. P-7 (LOCKED
  // 2026-06-24): bob-orchestrator hygiene incidents below P0 are Bob-owned —
  // they queue for the Day Done digest, never a live ping.
  if (eventType === "incident.opened") {
    if (eventId) {
      // Projector-retry dedup, matching the legacy path: checked here, marked
      // ONLY after successful dispatch (below) so a failed delivery never
      // suppresses the retry (codex review 2026-07-12).
      const alreadySent = await env.BWM_TELEGRAM_KV.get(`${KV_DEDUP_PREFIX}${eventId}`);
      if (alreadySent) return json({ ok: true, action: "dedup_skip", event_id: eventId });
    }
    const severity = String(payload["severity"] ?? "");
    const scope = String(payload["scope"] ?? "");
    const kind = String(payload["kind"] ?? payload["symptom"] ?? "unknown");
    const client = String(event.client_id ?? payload["client_id"] ?? "");
    if (scope === "bob-orchestrator" && severity !== "P0") {
      await enqueueDigestItem(env, {
        wire_type: "fyi",
        punchline: `${severity} ${scope}: ${kind}`,
        origin: "incident.opened",
        reason: "p7_autonomous",
      });
      await createOutboundLog(env, {
        sourceRoute: "/event",
        originEventId: eventId || null,
        originEventType: eventType,
        originSessionId: event.session_id ?? null,
        parseMode: "HTML",
        text: `${severity} ${scope}: ${kind}`,
        status: "skipped",
        metadata: { reason: "p7_digest", wire: { type: "fyi", queued: "digest" } },
      });
      // Queued successfully — mark the retry dedup key so a projector re-send
      // doesn't stack duplicate digest notes (codex review 2026-07-12 r2).
      if (eventId) {
        await env.BWM_TELEGRAM_KV.put(`${KV_DEDUP_PREFIX}${eventId}`, "1", {
          expirationTtl: DEDUP_TTL_SECONDS,
        });
      }
      return json({ ok: true, action: "queued_digest_p7", event_type: eventType });
    }
    // STRUCTURAL identity (plan v6 §2.5): evt|client_id|scope|kind. SEVERITY is
    // deliberately absent — a P1 that escalates to P0 is the SAME unresolved condition
    // and must EDIT the open message rather than open a second one. The old key
    // carried severity, so every escalation opened a fresh ping.
    // The .slice() calls on punchline/stakes are gone: source units are carried whole
    // and the renderer drops a whole line it cannot fit (§2.6).
    const fireInput: WireInput = {
      type: "fire",
      punchline: `${severity ? `${severity} ` : ""}${client || scope || "incident"} — ${kind}`,
      stakes: String(payload["symptom"] ?? payload["description"] ?? "") || undefined,
      key: `${EVENT_IDENTITY_PREFIX}${client}|${scope}|${kind}`,
      origin: `event:${eventType}${eventId ? `:${eventId}` : ""}`,
      session_id: event.session_id,
    };
    ctx.waitUntil(
      dispatchWire(env, fireInput, "/event", {
        originEventId: eventId || null,
        originEventType: eventType,
      }).then(async (r) => {
        if (!r.ok) {
          console.error(JSON.stringify({ where: "handleEvent.wireFire", error: r.error ?? r.action }));
          return;
        }
        // A claim-pending skip is NOT a delivery: leave the dedup key unset so
        // a projector retry can land after the winning send resolves (codex r5;
        // the hourly incident cadence also re-surfaces open incidents).
        if (r.action === "skipped_claim_pending") return;
        // Delivery succeeded — NOW mark the projector-retry dedup key.
        if (eventId) {
          await env.BWM_TELEGRAM_KV.put(`${KV_DEDUP_PREFIX}${eventId}`, "1", {
            expirationTtl: DEDUP_TTL_SECONDS,
          });
        }
      }).catch((e) => console.error(JSON.stringify({ where: "handleEvent.wireFire", error: String(e) }))),
    );
    return json({ ok: true, action: "wire_fire_queued", event_type: eventType });
  }

  const text = formatEventMessage(eventType, payload);
  // Kill list, layer 1 on the legacy /event lane (plan v6 §2.8). dispatchWire guards
  // the wire routes; this guards the one route that does not pass through it, so the
  // prohibition holds at EVERY door rather than most of them.
  if (isEmailAsRobert(text)) {
    await createOutboundLog(env, {
      sourceRoute: "/event",
      originEventId: eventId || null,
      originEventType: eventType,
      originSessionId: event.session_id ?? null,
      parseMode: "MarkdownV2",
      text,
      status: "skipped",
      metadata: { reason: "email_as_robert_disabled", killed_at: "ingress", ...(isCaptureMode(env) ? { capture: true } : {}) },
    });
    console.warn(JSON.stringify({ where: "handleEvent.killList", warn: "email_as_robert_blocked_at_ingress", event_type: eventType }));
    return json({ ok: true, action: "skipped_email_as_robert", event_type: eventType });
  }
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

  // Rate limit (separate from per-event_id dedup). Suppresses repeats of
  // high-volume events so the channel stays scannable. Suppressed events still
  // INSERT into operational_events; only the Telegram surface is rate-limited.
  // Key/window resolved per-event (incident.opened keys by kind+scope) — see
  // rateLimitFor().
  const rl = rateLimitFor(eventType, payload, event.client_id);
  // Hoisted so the delivery-failure paths below can CLEAR the window: the stamp
  // is written before send (to gate concurrent calls), so if nothing is actually
  // delivered we must release it or the next same-key event is wrongly suppressed
  // for the full window (codex review 2026-07-05).
  const activeRateLimitKey = rl ? rl.key : null;
  if (rl) {
    // NOTE (known limitation): this is a best-effort check-then-set on Workers KV,
    // which has no atomic compare-and-set. A truly concurrent same-key burst could
    // all read no-stamp before any put lands and each send once. We accept that:
    // the real storms this coalesces (file-watcher / startup) arrive SEQUENTIALLY
    // (~1-2/sec via the DB fanout), so the race window (KV get→put latency) is far
    // smaller than the gap between events, and worst case is a small burst instead
    // of an unbounded flood. An atomic claim would require a Durable Object —
    // disproportionate for a best-effort alert throttle (codex review 2026-07-05).
    const rateLimitWindow = rl.window;
    const rateLimitKey = rl.key;
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
    // No chat registered yet — log but return 200 to not block projector.
    // Release the rate-limit window: nothing was delivered.
    if (activeRateLimitKey) await env.BWM_TELEGRAM_KV.delete(activeRateLimitKey);
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
    requireSupabase: true,
  });

  // Mint token and send — fire-and-forget via waitUntil to not block response
  ctx.waitUntil(
    (async () => {
      // `delivered` gates the rate-limit window: it was stamped BEFORE delivery
      // (to gate concurrent calls), so unless a Telegram message actually went
      // out we must release it in `finally` — this covers non-ok returns AND
      // thrown fetch/network failures on mint or either send (codex review
      // 2026-07-05). If it stayed set, the next same-key incident would be
      // suppressed for the full window with nothing delivered.
      let delivered = false;
      try {
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
          delivered = true;
        } else {
          await updateOutboundLog(env, outboundId, {
            status: "sent",
            telegramMessageId: result.telegramMessageId,
            telegramResponse: result.response,
            error: null,
          });
          delivered = true;
        }

        // Mark as sent in dedup store
        if (eventId) {
          await env.BWM_TELEGRAM_KV.put(`${KV_DEDUP_PREFIX}${eventId}`, "1", {
            expirationTtl: DEDUP_TTL_SECONDS,
          });
        }
        await env.BWM_TELEGRAM_KV.put(KV_LAST_SEND_AT_KEY, new Date().toISOString());
      } finally {
        if (!delivered && activeRateLimitKey) {
          await env.BWM_TELEGRAM_KV.delete(activeRateLimitKey);
        }
      }
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
// Route: POST /send (legacy hard-wrap — Phase 3 containment)
// ─────────────────────────────────────────────────────────────────────────────

function decodeLegacyHtml(value: string): string {
  return value
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&quot;/gi, '"')
    .replace(/&#0*39;|&apos;/gi, "'")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&nbsp;/gi, " ")
    .replace(/\s+/g, " ")
    .trim();
}

export function legacySuppressionReason(text: string): string | null {
  const plain = decodeLegacyHtml(text).toLowerCase();
  if (plain === "wrap-check") return "phase3_wrap_check";
  if (/\bno action (?:is )?needed\b|\bnothing (?:is )?(?:needed|required) from (?:you|robert)\b|\bfor visibility only\b/i.test(plain)) {
    return "explicit_no_action";
  }
  if (/\[[^\]]{0,100}(?:smoke|test|gate)[^\]]{0,100}\]|\b(?:smoke test|test notification|test alert|ignore this test)\b|\b(?:close-sweep gate|bob-opt-final-gate)\b/i.test(plain)) {
    return "test_or_smoke";
  }
  return null;
}

export function legacyDigestPunchline(text: string): string {
  const plain = decodeLegacyHtml(text);
  if (/^bob needs a human reply\b/i.test(plain)) {
    return "A client reply is waiting for human review in the work queue.";
  }
  if (/^human triage\b/i.test(plain)) {
    return "Bob has items waiting for team triage in the work queue.";
  }
  if (/lane capacity/i.test(plain)) {
    return "The team's workload changed; details are in the work queue.";
  }
  // Whole unit or nothing (§2.6): a legacy note cut at 300 chars produced the
  // dangling-clause notes on the reviewed digest.
  return plain.length <= 300 ? plain : "";
}

async function handleSend(request: Request, env: Env): Promise<Response> {
  const key = request.headers.get("X-BWM-Internal-Key") ?? "";
  if (!env.BWM_INTERNAL_KEY || key !== env.BWM_INTERNAL_KEY) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }

  let body: { text?: string; parse_mode?: string; origin?: string; session_id?: string };
  try {
    body = (await request.json()) as { text?: string; parse_mode?: string; origin?: string; session_id?: string };
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }

  const text = typeof body.text === "string" ? body.text.trim() : "";
  if (!text) {
    return json({ ok: false, error: "text is required" }, 400);
  }

  const suppressionReason = legacySuppressionReason(text);
  if (suppressionReason) {
    await createOutboundLog(env, {
      sourceRoute: "/send",
      text,
      parseMode: body.parse_mode ?? null,
      status: "skipped",
      metadata: {
        reason: suppressionReason,
        wire: { type: "fyi", queued: "digest", hard_wrapped: true, suppressed: true },
      },
    });
    return json({
      ok: true,
      action: suppressionReason === "phase3_wrap_check" ? "queued_digest" : "suppressed",
      hard_wrapped: true,
      suppressed: true,
      reason: suppressionReason,
    });
  }

  // legacyDigestPunchline returns "" when the source unit is too long to render
  // WHOLE (§2.6). Queueing that would put an empty bullet on the digest — worse than
  // the clipped one it replaces. Skip the item and say so, auditably.
  const legacyPunchline = legacyDigestPunchline(text);
  if (!legacyPunchline) {
    await createOutboundLog(env, {
      sourceRoute: "/send", text, parseMode: body.parse_mode ?? null, status: "skipped",
      metadata: {
        reason: "source_unit_too_long_to_render_whole",
        wire: { type: "fyi", hard_wrapped: true, suppressed: true },
        ...(isCaptureMode(env) ? { capture: true } : {}),
      },
    });
    return json({ ok: true, action: "suppressed", hard_wrapped: true, suppressed: true, reason: "source_unit_too_long_to_render_whole" });
  }

  const result = await dispatchWire(env, {
    type: "fyi",
    punchline: legacyPunchline,
    origin: typeof body.origin === "string" && body.origin.trim()
      ? body.origin.trim().slice(0, 120)
      : "legacy:/send",
    session_id: typeof body.session_id === "string" ? body.session_id.trim().slice(0, 120) : undefined,
  }, "/send");
  return json({
    ...result,
    hard_wrapped: true,
    legacy_parse_mode_ignored: Boolean(body.parse_mode),
  }, result.ok ? 200 : 502);
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

/** Insert one operational event. Returns the event's ULID on success (Phase 2:
 *  task.queued fan-out rows are keyed by source_event_id = this id, so callers
 *  that later emit task.resolved need it back), or null on failure — truthiness
 *  is unchanged from the old boolean contract. */
async function emitOperationalEvent(
  env: Env,
  eventType: string,
  payload: Record<string, unknown>,
  sessionId: string,
): Promise<string | null> {
  try {
    const id = ulid();
    // CAPTURE (plan v6 §2.10): every emission is diverted to KV `capture:events:*`.
    // Nothing reaches operational_events, so an acceptance drill exercises the real
    // code path without writing one row into the production substrate. The pollution
    // check fails on ANY production hit AND on any query failure.
    if (isCaptureMode(env)) {
      await env.BWM_TELEGRAM_KV.put(
        `capture:events:${id}`,
        JSON.stringify({ id, event_type: eventType, payload, session_id: sessionId, occurred_at: new Date().toISOString() }),
        { expirationTtl: 7 * 24 * 60 * 60 },
      );
      return id;
    }
    const resp = await fetch(`${env.SUPABASE_URL.replace(/\/+$/, "")}/rest/v1/operational_events`, {
      method: "POST",
      headers: {
        apikey: env.SUPABASE_SERVICE_KEY,
        Authorization: `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        "Content-Type": "application/json",
        Prefer: "return=minimal",
      },
      body: JSON.stringify({
        id,
        event_type: eventType,
        client_id: null,
        payload,
        occurred_at: new Date().toISOString(),
        session_id: sessionId,
      }),
    });
    return resp.ok ? id : null;
  } catch {
    return null;
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
    `&select=origin_event_id,origin_event_type,metadata&order=queued_at.desc&limit=1`;
  let originEventId = "";
  let originEventType = "";
  let wireMeta: { type?: string; ref?: string; task_event_id?: string } | null = null;

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
      metadata: Record<string, unknown> | null;
    }>;
    if (!data || data.length === 0) {
      console.log(`processTelegramReply: no outbound log for message_id=${replyToMessageId}`);
      return false;
    }
    originEventId = data[0].origin_event_id ?? "";
    originEventType = data[0].origin_event_type ?? "";
    const metaWire = (data[0].metadata ?? {})["wire"];
    if (metaWire && typeof metaWire === "object") wireMeta = metaWire as { type?: string; ref?: string; task_event_id?: string };
  } catch (err) {
    console.error("processTelegramReply: query threw", err);
    return false;
  }

  // A resolve-reply on a FIRE clears its edit-in-place registry so the incident
  // leaves "Open fires" and a later same-key event starts a fresh message.
  // Captured here, deleted ONLY after the resolution event persists (codex r3
  // — a Supabase outage must not vanish an unresolved fire from the digest).
  let fireRegistryKeyToClear: string | null = null;
  if (wireMeta?.type === "fire") {
    // Captured for ANY reply to a fire — resolve commands AND directives
    // ("you handle this") both end in an incident-resolved emit, and both must
    // clear the registry on success (codex r4). Non-resolving replies never
    // reach the gated delete.
    // Prefer the STORED fingerprint (metadata.wire.fingerprint) — never re-derive it
    // from the raw key, because re-hashing silently misses whenever the fingerprint
    // recipe changes, and a resolved fire that never clears re-renders in every later
    // digest.
    //
    // MIGRATION FALLBACK: fires opened BEFORE the fingerprint deploy have no stored
    // fingerprint, and there are live ones in the registry right now. For those, and
    // only for DELETION of an already-existing entry, fall back to the legacy
    // shortHash(rawKey) slot. This mints no identity — it just lets Robert resolve an
    // in-flight incident instead of stranding it.
    const storedFp = (wireMeta as { fingerprint?: string }).fingerprint;
    const fireRawKey = (wireMeta as { key?: string }).key;
    if (storedFp) {
      fireRegistryKeyToClear = `${KV_WIRE_FIRE_PREFIX}${storedFp}`;
    } else if (fireRawKey) {
      fireRegistryKeyToClear = `${KV_WIRE_FIRE_PREFIX}${shortHash(fireRawKey)}`;
    }
    // ORIGINLESS fires (created via /notify, no operational-event identity)
    // would dead-end at the missing-origin guard below — resolve them inline
    // (codex r6): persist the decision, clear the registry, confirm.
    if (!originEventId && fireRegistryKeyToClear && RESOLVE_CMDS.has(text.trim().toLowerCase())) {
      const logged = await emitOperationalEvent(env, "narrative", {
        source: "telegram-reply",
        kind: "wire-decision",
        wire_ref: (wireMeta as { ref?: string }).ref ?? null,
        wire_type: "fire",
        choice_raw: text.trim().slice(0, 200),
        note: `Robert resolved ${(wireMeta as { ref?: string }).ref ?? "a fire"} via direct reply: "${text.trim().slice(0, 120)}"`,
      }, `telegram-reply-${inboundEventId}`);
      if (logged) {
        try {
          await env.BWM_TELEGRAM_KV.delete(fireRegistryKeyToClear);
        } catch (e) {
          console.error(JSON.stringify({ where: "processTelegramReply.originlessFireClear", error: String(e) }));
        }
      }
      try {
        await sendTelegramMessage(botToken, chatId,
          logged ? "✅ Resolved." : "⚠️ Couldn't log that — try again in a minute.",
          undefined, message.message_id);
      } catch { /* fail-soft */ }
      return logged !== null;
    }
  }

  // ── ONE WIRE DECISION PATH ─────────────────────────────────────────────────
  // A reply to a CALL/SIGNOFF is Robert's decision: clear the waiting-on-you
  // registry, log the decision as a narrative (sessions + Bob read
  // operational_events), confirm receipt — then CONTINUE normal routing
  // (return false) so the responder pipeline still acts on free-text
  // instructions exactly like any other inbound message.
  // Replying to the DIGEST with "C-8KQ2M: go" answers that ref — the only
  // path for decisions that were deferred (never sent live) (codex r2).
  if (wireMeta && (wireMeta as { type?: string }).type === "digest") {
    const m = /^\s*([FCS]-[A-Z0-9]{3,6})\s*[:,-]?\s*(.*)$/i.exec(text.trim());
    if (m) {
      const ref = m[1].toUpperCase();
      const answer = (m[2] || "").trim();
      // FIRE refs live in the fire registry, not wire:open (codex r3): find by
      // ref, persist the resolution, then clear the registry.
      if (ref.startsWith("F-")) {
        const fireList = await env.BWM_TELEGRAM_KV.list({ prefix: KV_WIRE_FIRE_PREFIX, limit: 50 });
        let cleared = false;
        for (const k of fireList.keys) {
          const raw = await env.BWM_TELEGRAM_KV.get(k.name);
          if (!raw) continue;
          try {
            const reg = JSON.parse(raw) as { ref?: string; base?: { origin?: string }; fingerprint?: string };
            if (reg.ref !== ref) continue;
            // Same rule as the direct-reply path: the key to delete is the registry
            // entry we actually found (k.name), which IS the fingerprint slot.
            const originStr = reg.base?.origin ?? "";
            const idMatch = /^event:incident\.opened:(.+)$/.exec(originStr);
            const logged = await emitOperationalEvent(env, "narrative", {
              source: "telegram-reply",
              kind: idMatch ? "incident-resolved" : "wire-decision",
              ...(idMatch ? { closes_incident_id: idMatch[1] } : { wire_ref: ref, wire_type: "fire", choice_raw: answer.slice(0, 200) }),
              note: `Robert resolved ${ref} via digest reply: "${answer.slice(0, 200)}"`,
            }, `telegram-reply-${inboundEventId}`);
            if (logged) {
              await env.BWM_TELEGRAM_KV.delete(k.name);
              cleared = true;
            }
            break;
          } catch { /* skip malformed */ }
        }
        try {
          await sendTelegramMessage(botToken, chatId,
            cleared ? `✅ ${ref} resolved.` : `⚠️ Couldn't resolve ${ref} just now — try again in a minute.`,
            undefined, message.message_id);
        } catch { /* fail-soft */ }
        return false;
      }
      const openRaw = await env.BWM_TELEGRAM_KV.get(`${KV_WIRE_OPEN_PREFIX}${ref}`);
      if (openRaw) {
        let openType = "call";
        let digestTaskEventId: string | null = null;
        let digestJudgment: Record<string, unknown> | undefined;
        try {
          const openReg = JSON.parse(openRaw) as Record<string, unknown>;
          openType = String(openReg["type"] ?? "call");
          digestTaskEventId = (openReg["task_event_id"] as string | null | undefined) ?? null;
          digestJudgment = registryJudgmentMeta(openReg);
        } catch { /* default */ }
        const choiceNum = /^([1-9])\b/.exec(answer)?.[1] ?? null;
        const logged = await emitOperationalEvent(env, "narrative", {
          source: "telegram-reply",
          kind: "wire-decision",
          wire_ref: ref,
          wire_type: openType,
          option: choiceNum,
          choice_raw: answer.slice(0, 500) || "(ack)",
          inbound_event_id: inboundEventId,
          note: `Robert answered ${ref} via digest reply${choiceNum ? ` with option ${choiceNum}` : ""}: "${answer.slice(0, 200)}"`,
          ...(digestJudgment ? { judgment: digestJudgment } : {}),
        }, `telegram-reply-${inboundEventId}`);
        if (logged) {
          await env.BWM_TELEGRAM_KV.delete(`${KV_WIRE_OPEN_PREFIX}${ref}`);
          await emitWireTaskResolved(env, digestTaskEventId, ref, answer || "(ack)", `telegram-reply-${inboundEventId}`);
        }
        try {
          await sendTelegramMessage(botToken, chatId,
            logged ? `✅ ${ref} logged. On it.` : `⚠️ Couldn't log ${ref} just now — it stays on your list; try again in a minute.`,
            undefined, message.message_id);
        } catch (e) {
          console.error(JSON.stringify({ where: "processTelegramReply.digestConfirm", error: String(e) }));
        }
      } else {
        // Same TTL-expiry hedge as the direct-reply path, same ordering:
        // classify via the mirror, persist the decision narrative, only then
        // close the task (codex r7+r8).
        let confirmMsg = `${m[1].toUpperCase()} isn't open (already answered or expired).`;
        if (!ref.startsWith("F-")) {
          const mirrorHit = await findQueuedWireMirror(env, null, ref);
          if (mirrorHit === "unavailable") {
            confirmMsg = `⚠️ Couldn't check ${ref} just now — try again in a minute.`;
          } else if (mirrorHit) {
            const narrativeId = await emitOperationalEvent(env, "narrative", {
              source: "telegram-reply",
              kind: "wire-decision",
              wire_ref: ref,
              // Post-expiry the registry no longer knows call vs signoff —
              // "unknown" keeps the CALL latency metric unpolluted.
              wire_type: "unknown",
              option: /^([1-9])\b/.exec(answer)?.[1] ?? null,
              choice_raw: (answer || "(late answer)").slice(0, 500),
              late: true,
              inbound_event_id: inboundEventId,
              note: `Robert answered ${ref} via digest reply after its ref expired: "${answer.slice(0, 200)}"`,
            }, `telegram-reply-${inboundEventId}`);
            if (narrativeId) {
              await emitWireTaskResolved(env, mirrorHit.id, ref, answer || "(late answer)", `telegram-reply-${inboundEventId}`);
              confirmMsg = `✅ ${ref} logged (late — the ref had expired). On it.`;
            } else {
              confirmMsg = `⚠️ Couldn't log ${ref} just now — try again in a minute.`;
            }
          }
        }
        try {
          await sendTelegramMessage(botToken, chatId, confirmMsg, undefined, message.message_id);
        } catch { /* fail-soft */ }
      }
      // Continue normal routing either way — responder still sees the text.
      return false;
    }
  }

  if (wireMeta?.ref && (wireMeta.type === "call" || wireMeta.type === "signoff")) {
    // Idempotency parity with the reaction + digest paths (codex r5): a second
    // reply to an already-answered decision is a follow-up, not a new decision.
    const openRawReply = await env.BWM_TELEGRAM_KV.get(`${KV_WIRE_OPEN_PREFIX}${wireMeta.ref}`);
    const stillOpen = openRawReply !== null;
    let replyTaskEventId: string | null = wireMeta.task_event_id ?? null;
    let replyJudgment: Record<string, unknown> | undefined;
    if (openRawReply) {
      try {
        const replyReg = JSON.parse(openRawReply) as Record<string, unknown>;
        if (!replyTaskEventId) {
          replyTaskEventId = (replyReg["task_event_id"] as string | null | undefined) ?? null;
        }
        replyJudgment = registryJudgmentMeta(replyReg);
      } catch { /* pre-Phase-2 registry rows have no task mirror */ }
    }
    if (!stillOpen) {
      // Registry gone = EITHER already answered (follow-up) OR expired
      // unanswered (7-day TTL) — the mirror's status tells them apart (codex
      // r7). Ordering (codex r8): the wire-decision narrative persists FIRST
      // (decisions are data; Bob + the scorecard read them), only then does
      // the task close — a lost narrative must not leave a closed task with
      // a "logged" confirmation.
      const mirrorHit = await findQueuedWireMirror(env, wireMeta.task_event_id ?? null, wireMeta.ref);
      let confirmMsg = `ℹ️ ${wireMeta.ref} was already answered — treating this as a follow-up note.`;
      if (mirrorHit === "unavailable") {
        confirmMsg = `⚠️ Couldn't check ${wireMeta.ref} just now — try again in a minute.`;
      } else if (mirrorHit) {
        const narrativeId = await emitOperationalEvent(env, "narrative", {
          source: "telegram-reply",
          kind: "wire-decision",
          wire_ref: wireMeta.ref,
          wire_type: wireMeta.type,
          option: /^\s*([1-9])\b/.exec(text)?.[1] ?? null,
          choice_raw: text.trim().slice(0, 500),
          late: true,
          inbound_event_id: inboundEventId,
          note: `Robert answered ${wireMeta.ref} after its ref expired: "${text.trim().slice(0, 200)}"`,
        }, `telegram-reply-${inboundEventId}`);
        if (narrativeId) {
          await emitWireTaskResolved(env, mirrorHit.id, wireMeta.ref, text.trim(), `telegram-reply-${inboundEventId}`);
          confirmMsg = `✅ ${wireMeta.ref} logged (late — the ref had expired). On it.`;
        } else {
          confirmMsg = `⚠️ Couldn't log ${wireMeta.ref} just now — try again in a minute.`;
        }
      }
      try {
        await sendTelegramMessage(botToken, chatId, confirmMsg, undefined, message.message_id);
      } catch { /* fail-soft */ }
      return false; // normal routing still delivers the text to the responder
    }
    const choiceNum = /^\s*([1-9])\b/.exec(text)?.[1] ?? null;
    // Persist the decision FIRST; only then clear the waiting-on-you entry and
    // confirm. If the insert fails (Supabase outage) the item stays open and
    // the confirmation says so — never claim "logged" on a lost write (codex
    // review 2026-07-12).
    const decisionLogged = await emitOperationalEvent(
      env,
      "narrative",
      {
        source: "telegram-reply",
        kind: "wire-decision",
        wire_ref: wireMeta.ref,
        wire_type: wireMeta.type,
        option: choiceNum,
        choice_raw: text.trim().slice(0, 500),
        inbound_event_id: inboundEventId,
        note: `Robert answered ${wireMeta.ref}${choiceNum ? ` with option ${choiceNum}` : ""}: "${text.trim().slice(0, 200)}"`,
        ...(replyJudgment ? { judgment: replyJudgment } : {}),
      },
      `telegram-reply-${inboundEventId}`,
    );
    if (decisionLogged) {
      try {
        await env.BWM_TELEGRAM_KV.delete(`${KV_WIRE_OPEN_PREFIX}${wireMeta.ref}`);
      } catch (e) {
        console.error(JSON.stringify({ where: "processTelegramReply.wireClear", error: String(e) }));
      }
      await emitWireTaskResolved(env, replyTaskEventId, wireMeta.ref, text.trim(), `telegram-reply-${inboundEventId}`);
    }
    try {
      await sendTelegramMessage(
        botToken,
        chatId,
        decisionLogged
          ? `✅ ${wireMeta.ref} logged${choiceNum ? ` — option ${choiceNum}` : ""}. On it.`
          : `⚠️ Couldn't log ${wireMeta.ref} just now — it stays on your list; try again in a minute.`,
        undefined,
        message.message_id,
      );
    } catch (e) {
      console.error(JSON.stringify({ where: "processTelegramReply.wireConfirm", error: String(e) }));
    }
    // fall through to normal routing (responder still executes the instruction)
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
      actionTaken = ok !== null;
      if (ok && fireRegistryKeyToClear) {
        try {
          await env.BWM_TELEGRAM_KV.delete(fireRegistryKeyToClear);
        } catch (e) {
          console.error(JSON.stringify({ where: "processTelegramReply.fireRegistryClear.directive", error: String(e) }));
        }
      }
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
      actionTaken = ok !== null;
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
      actionTaken = ok !== null;
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
      actionTaken = ok !== null;
      if (ok && fireRegistryKeyToClear) {
        try {
          await env.BWM_TELEGRAM_KV.delete(fireRegistryKeyToClear);
        } catch (e) {
          console.error(JSON.stringify({ where: "processTelegramReply.fireRegistryClear", error: String(e) }));
        }
      }
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
      actionTaken = ok !== null;
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
  interface AckOrigin {
    origin_event_id?: string;
    origin_event_type?: string;
    source_route?: string;
    text_redacted?: string;
    metadata?: Record<string, unknown>;
  }
  let origin: AckOrigin | null = null;
  if (supabaseConfigured(env)) {
    try {
      const q = `telegram_outbound?chat_id=eq.${encodeURIComponent(String(reaction.chat.id))}` +
        `&telegram_message_id=eq.${reaction.message_id}` +
        `&select=origin_event_id,origin_event_type,source_route,text_redacted,metadata&limit=1`;
      const resp = await fetch(supabaseRestUrl(env, q), { headers: supabaseHeaders(env) });
      if (resp.ok) {
        const rows = (await resp.json()) as Array<AckOrigin>;
        origin = rows[0] ?? null;
      }
    } catch (e) {
      console.error(JSON.stringify({ where: "handleReactionUpdate.lookup", error: String(e) }));
    }
  }

  // One Wire: 👍 on a CALL/SIGNOFF = "take my rec / approved". ONLY 👍 carries
  // decision semantics — 👎/❤️/anything else stays a plain ack and the item
  // stays open (codex review 2026-07-12). The narrative is written FIRST; the
  // waiting-on-you entry is cleared only after it persists, so a Supabase
  // outage never silently swallows a decision.
  const wireOnAck = (origin?.metadata ?? {})["wire"] as { type?: string; ref?: string } | undefined;
  const ackOpenRaw = wireOnAck?.ref
    ? await env.BWM_TELEGRAM_KV.get(`${KV_WIRE_OPEN_PREFIX}${wireOnAck.ref}`)
    : null;
  const ackRefStillOpen = ackOpenRaw !== null;
  let ackTaskEventId: string | null = null;
  let ackJudgment: Record<string, unknown> | undefined;
  if (ackOpenRaw) {
    try {
      const ackReg = JSON.parse(ackOpenRaw) as Record<string, unknown>;
      ackTaskEventId = (ackReg["task_event_id"] as string | null | undefined) ?? null;
      ackJudgment = registryJudgmentMeta(ackReg);
    } catch { /* pre-Phase-2 registry rows have no task mirror */ }
  }
  // Only an OPEN ref accepts a reaction-decision — a late/accidental 👍 on an
  // already-answered item stays a plain ack (codex r2, idempotency).
  if (wireOnAck?.ref && (wireOnAck.type === "call" || wireOnAck.type === "signoff") && emojis.includes("👍") && ackRefStillOpen) {
    let decisionLogged = false;
    if (canWriteOperationalEventsDirectly(env)) {
      try {
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
              kind: "wire-decision",
              wire_ref: wireOnAck.ref,
              wire_type: wireOnAck.type,
              option: null,
              choice_raw: `reaction:${emojis.join(" ")}`,
              note: `Robert acked ${wireOnAck.ref} via 👍 — take the recommendation.`,
              reacted_message_id: reaction.message_id,
              ...(ackJudgment ? { judgment: ackJudgment } : {}),
            },
          }),
        });
        decisionLogged = resp.ok;
      } catch (e) {
        console.error(JSON.stringify({ where: "handleReactionUpdate.wireNarrative", error: String(e) }));
      }
    }
    if (decisionLogged) {
      try {
        await env.BWM_TELEGRAM_KV.delete(`${KV_WIRE_OPEN_PREFIX}${wireOnAck.ref}`);
      } catch (e) {
        console.error(JSON.stringify({ where: "handleReactionUpdate.wireClear", error: String(e) }));
      }
      await emitWireTaskResolved(env, ackTaskEventId, wireOnAck.ref, "👍 take the rec", "daemon:bwm-telegram-relay");
    } else {
      console.error(JSON.stringify({
        where: "handleReactionUpdate.wireDecision",
        warn: "decision_not_persisted_item_stays_open",
        ref: wireOnAck.ref,
      }));
    }
  }

  // Narrative so the ack is visible in operational_events (Bob/Sarah/sweeps).
  if (canWriteOperationalEventsDirectly(env)) {
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

  if (!canWriteOperationalEventsDirectly(env)) {
    console.log(JSON.stringify({ where: "heartbeat", skipped: "capture_mode" }));
    return;
  }
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
// One Wire — typed Robert-facing message gate (POST /notify + Day Done digest)
// Attention-Routing-Spec v2.0.0. Robert GO 2026-07-12.
// ─────────────────────────────────────────────────────────────────────────────

function escapeHtml(text: string): string {
  return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// ─────────────────────────────────────────────────────────────────────────────
// Email-as-Robert kill list (plan v6 §2.8) — THREE layers, active in EVERY mode
//
// Robert's standing capability prohibition: "I told you I don't want any more
// motherfucking automatic replies… that's not a capability we need to focus on for
// the time being." The queue did not enforce it, so auto-drafted email "as you" kept
// consuming his attention (canonical finding 4; rule 5).
//
// Layer 1 — INGRESS: /notify and /event drop a matching item before it can be queued
//           or sent. Layer 2 — SOURCE: the Day Ahead ea_drafts section is gone (query
//           and render both). Layer 3 — FINAL RENDER: the composed payload is scrubbed
//           immediately before send, so a signature arriving by a path nobody
//           enumerated still cannot reach the phone.
//
// Evaluation FAILS CLOSED: a throw anywhere in matching is treated as a match.
// Re-enabling this capability is a reviewed PR that deletes these patterns, never a
// config flag.
// ─────────────────────────────────────────────────────────────────────────────

// The signatures are deliberately TIGHT and literal. An earlier version used general
// patterns ("send/reply … as you", "approve and send this draft") and an adversarial
// review showed it killing 9 of 10 ordinary operational sentences — including
// "Approve and send this draft blog post to Cathryn's list?", which is a legitimate
// SIGNOFF and one of the system's core use cases. A kill list that eats real decisions
// is a worse failure than the one it prevents: the sender gets HTTP 200 and Robert gets
// silence. Match the capability's actual output, nothing broader.
const EMAIL_AS_ROBERT_SIGNATURES: RegExp[] = [
  /\bemail\s+ready\s+to\s+send\s+as\s+you\b/i,          // the f05 punchline, verbatim
  /\bready\s+to\s+send\s+as\s+(?:you|robert)\s*[→>]/i,    // same shape, addressee arrow
  /\btap\s+the\s+link\s+to\s+send\s+it\s+now\b/i,        // the f05 rec, verbatim
  /\bdrafts?\s+ready\s+to\s+send\s*:/i,                   // the retired Day Ahead line
  /\bthe\s+draft\s+expires\s+in\s+\d+\s+days?\b/i,        // the f05 discard instruction
];

/** Origins whose ENTIRE output is the disabled capability. Blocking by producer is the
 *  precise mechanism — the capability has a name, so it does not need to be inferred
 *  from prose. Text matching above is the backstop for anything that slips the origin. */
const EMAIL_AS_ROBERT_ORIGINS = /^bwm-gmail-send\b|^bwm-ea-drafts\b/i;

/** True when any part of an item carries an email-as-Robert signature.
 *  FAILS CLOSED — an exception counts as a match (plan v6 §2.1 typed failure posture). */
export function isEmailAsRobert(...parts: Array<string | null | undefined>): boolean {
  try {
    const hay = parts.filter(Boolean).join("\n");
    if (!hay) return false;
    return EMAIL_AS_ROBERT_SIGNATURES.some((re) => re.test(hay));
  } catch {
    return true;
  }
}

/** True when the item comes from a producer whose whole job is the disabled capability. */
export function isEmailAsRobertOrigin(origin: string | null | undefined): boolean {
  try {
    return !!origin && EMAIL_AS_ROBERT_ORIGINS.test(origin);
  } catch {
    return true;
  }
}

/** Layer 3. Drop every LINE carrying a signature from an already-composed payload,
 *  then REPAIR what the removal left behind.
 *
 *  Removing a line is not enough on its own. A scrubbed item can leave:
 *    · a section header with nothing under it, and
 *    · a reply footer naming a ref that is no longer on screen —
 *  which is finding 12 ("the reply instruction demonstrates a reference that does not
 *  exist") reintroduced by the very guard meant to clean the surface. Observed while
 *  rendering a real pre-deploy wire:open entry through the composer, so it is repaired
 *  here rather than left for Robert to find.
 *
 *  Returns the finished text plus how many signature lines were removed — a nonzero
 *  count means an un-enumerated ingress path exists and must be found. */
export function scrubEmailAsRobert(text: string): { text: string; removed: number } {
  const kept: string[] = [];
  let removed = 0;
  for (const line of text.split("\n")) {
    if (isEmailAsRobert(line)) { removed += 1; continue; }
    kept.push(line);
  }
  if (removed === 0) return { text: kept.join("\n"), removed };

  // Drop a footer whose named ref no longer appears anywhere else on screen.
  const isFooter = (l: string) => /^<i>Reply by ref/.test(l);
  const refIn = (l: string) => (/"([FCS]-[0-9A-Z]{3,6}):/.exec(l) ?? [])[1];
  const repaired = kept.filter((l) => {
    if (!isFooter(l)) return true;
    const ref = refIn(l);
    if (!ref) return true;
    return kept.some((other) => other !== l && other.includes(ref));
  });

  // Drop a section header with no bullet under it.
  const isHeader = (l: string) => /^<b>[^<]+:<\/b>$/.test(l);
  const isBullet = (l: string) => l.startsWith("•");
  const final = repaired.filter((l, i) => {
    if (!isHeader(l)) return true;
    for (let j = i + 1; j < repaired.length; j++) {
      if (isBullet(repaired[j])) return true;
      if (isHeader(repaired[j])) break;
    }
    return false;
  });
  return { text: final.join("\n"), removed };
}

// ─────────────────────────────────────────────────────────────────────────────
// Capture environment (plan v6 §2.10)
// ─────────────────────────────────────────────────────────────────────────────

/** True only inside the [env.capture] worker. Never infer capture from anything
 *  else — a false positive here would silently stop production event emission. */
function isCaptureMode(env: Env): boolean {
  return String(env.CAPTURE_MODE ?? "") === "true";
}

// ─────────────────────────────────────────────────────────────────────────────
// Source-unit preservation (plan v6 §2.6)
//
// Finding 11: "built, merged, deployed and producti", "now to fix t",
// "2=Wait for Sol's cross-che". Fixed character slicing cut source fields mid-word.
// The rule that replaces it: INCLUDE THE WHOLE UNIT OR DROP THE WHOLE UNIT. A unit is
// one source field (a punchline, a rec, a note). It is never cut, ever. If it does not
// fit the budget, the whole unit is omitted and the message stays honest.
// ─────────────────────────────────────────────────────────────────────────────

/** Assemble lines under a character budget WITHOUT ever cutting one. Returns the
 *  lines that fit whole, plus the ones dropped (for telemetry / rollover). */
export function fitWholeUnits(units: string[], budget: number): { kept: string[]; dropped: string[] } {
  const kept: string[] = [];
  const dropped: string[] = [];
  let used = 0;
  for (const u of units) {
    const cost = u.length + 1;
    if (used + cost > budget) { dropped.push(u); continue; }
    kept.push(u);
    used += cost;
  }
  return { kept, dropped };
}

/** Every included source unit must survive VERBATIM into the rendered output.
 *  Throwing here is correct: a violated preservation invariant means the renderer is
 *  about to show Robert a fragment, and a fragment is what he rejected. Callers run
 *  this on the FINAL payload, after all length handling. */
export function assertSourcePreservation(rendered: string, units: string[]): void {
  const missing = units.filter((u) => u && !rendered.includes(u));
  if (missing.length > 0) {
    throw new Error(
      `source_unit_preservation_violated: ${missing.length} unit(s) were altered or cut; ` +
      `first=${JSON.stringify(missing[0].slice(0, 60))}`,
    );
  }
}

/** Truncate rendered Telegram HTML without cutting inside an entity or tag.
 *
 *  TRIPWIRE (plan v6 §2.6): this is no longer a rendering strategy — it is the guard
 *  of last resort. Every composer above it must already have fitted whole units into
 *  the budget, so reaching the truncating branch means a composer has a bug. When that
 *  happens we still return valid HTML (a REJECTED message helps nobody) but we log
 *  loudly, and the unit tests assert the branch is never taken. */
function safeHtmlTruncate(text: string, max: number): string {
  if (text.length <= max) return text;
  console.error(JSON.stringify({
    where: "safeHtmlTruncate",
    error: "truncation_tripwire_fired",
    detail: "a composer emitted over-budget text; source units may have been cut",
    length: text.length,
    max,
  }));
  let cut = text.slice(0, max);
  cut = cut.replace(/&[a-zA-Z]{0,6}$/, ""); // trailing partial entity
  cut = cut.replace(/<[^>]*$/, ""); // trailing partial tag (any)
  const stack: string[] = [];
  const tagRe = /<(\/?)(b|i|a)(?:\s[^>]*)?>/gi;
  let m: RegExpExecArray | null;
  while ((m = tagRe.exec(cut)) !== null) {
    const name = m[2].toLowerCase();
    if (m[1]) {
      const idx = stack.lastIndexOf(name);
      if (idx !== -1) stack.splice(idx, 1);
    } else {
      stack.push(name);
    }
  }
  for (const name of stack.reverse()) cut += `</${name}>`;
  return `${cut}…`;
}

/** Current time in Robert's timezone (America/New_York). Digest labels, quiet
 *  hours, the Wednesday rule, and the daily budget key all run on ET. */
function etNow(d = new Date()): { date: string; hour: number; weekday: string; label: string; hhmm: string } {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour12: false,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", weekday: "short",
  }).formatToParts(d);
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? "";
  const labelFmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York", weekday: "short", month: "short", day: "numeric",
  });
  return {
    date: `${get("year")}-${get("month")}-${get("day")}`,
    hour: parseInt(get("hour"), 10) % 24,
    weekday: get("weekday"),
    label: labelFmt.format(d),
    hhmm: `${get("hour")}:${get("minute")}`,
  };
}

type WireType = "fire" | "call" | "signoff" | "fyi";

/** ET calendar date of an expires_at value. Bare dates ("2026-07-16") pass
 *  through as authored; ISO DATETIMES convert to America/New_York first — a
 *  UTC-date slice can differ from the ET date and hold an urgent Wednesday
 *  decision past its deadline (post-deploy codex round). */
function etDateOf(iso: string): string {
  if (!iso.includes("T")) return iso.slice(0, 10);
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return iso.slice(0, 10);
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit",
  }).format(new Date(t));
}

/** PROJ-UPLIFT-001 judgment-capture opt-in on a call/signoff (Judgment-Capture-
 *  Contract "One Wire CALL source"). Senders set this ONLY for genuine business
 *  forks with a checkable future outcome — the default is NO capture; nothing
 *  here classifies or guesses. The relay just validates + stamps it into
 *  telegram_outbound metadata.wire.judgment; the local wire-sweep seals the
 *  judgment_predictions row after Robert's answer lands as a wire-decision. */
interface WireJudgment {
  /** Contract §2 domain taxonomy (matches the judgment_predictions check). */
  domain: string;
  /** Contract §3: the condition that makes the outcome knowable, plain words. */
  outcome_knowable_by: string;
  /** YYYY-MM-DD when derivable — lets the resurface scheduler find it. */
  outcome_knowable_at?: string;
  /** Claude's independent forecast. Defaults to the call's rec at sweep time. */
  claude_prediction?: string;
  /** 0–1. */
  claude_confidence?: number;
  /** A | B | both (sweep defaults to both). */
  loop?: string;
  /** Contract §9 machine-scoreable resolver spec — passthrough, never sealed. */
  resolution?: Record<string, unknown>;
}

const JUDGMENT_DOMAINS = ["pricing", "offer", "client_select", "copy", "hiring", "strategy", "ops", "other"];

/** Validate the judgment opt-in. Returns the sanitized object, or a drop
 *  reason. NEVER throws and NEVER fails the send — capture failure must not
 *  block the wire (contract: fails safe). */
function parseWireJudgment(
  raw: unknown,
  type: WireType,
  hasRec: boolean,
): { judgment?: WireJudgment; dropped?: string } {
  if (raw === undefined || raw === null) return {};
  try {
    if (typeof raw !== "object" || Array.isArray(raw)) return { dropped: "judgment must be an object" };
    if (type !== "call" && type !== "signoff") return { dropped: "judgment only applies to call/signoff" };
    const j = raw as Record<string, unknown>;
    // Machine fields (domain/date/loop) validate on the FULL trimmed value —
    // truncate-then-validate silently accepted "2026-07-13oops" as a date and
    // "both-extra" as a loop (codex r2). Free-text fields cap after.
    const full = (k: string) => {
      const v = j[k];
      return typeof v === "string" && v.trim() ? v.trim() : undefined;
    };
    // Present-but-wrong-typed optionals must DROP, not silently vanish — a
    // numeric claude_prediction would otherwise report "accepted" and seal
    // the rec instead of the submitted forecast (codex r6). null = absent.
    for (const k of ["claude_prediction", "outcome_knowable_at", "loop", "outcome_knowable_by", "domain"]) {
      const v = j[k];
      if (v !== undefined && v !== null && (typeof v !== "string" || !v.trim())) {
        return { dropped: `judgment.${k} must be a non-empty string` };
      }
    }
    const domain = full("domain");
    if (!domain || !JUDGMENT_DOMAINS.includes(domain)) {
      return { dropped: `judgment.domain must be one of ${JUDGMENT_DOMAINS.join("|")}` };
    }
    const outcomeBy = full("outcome_knowable_by")?.slice(0, 300);
    if (!outcomeBy) return { dropped: "judgment.outcome_knowable_by is required" };
    if (!hasRec) {
      // §12 gives every call/signoff a recommendation; the sweep can only
      // resolve a 👍/(ack) answer by adopting it. Accepting a rec-less
      // judgment would report "accepted" and then silently never capture on
      // an ack (codex r2) — an explicit claude_prediction does not fix that,
      // because it is Claude's forecast, not Robert's adopted one.
      return { dropped: "judgment requires a rec on the call" };
    }
    const claudePrediction = full("claude_prediction")?.slice(0, 400);
    const outcomeAt = full("outcome_knowable_at");
    if (outcomeAt !== undefined) {
      // Must be a REAL calendar date, not just regex-shaped — "2026-02-30"
      // would pass /notify as "accepted" and then be rejected by the capture
      // CLI at sweep time, silently never capturing (codex P2). Year 0000 is
      // excluded: JS Date accepts it, Python fromisoformat downstream does
      // not (codex r4).
      const roundTrip = /^(?!0000)\d{4}-\d{2}-\d{2}$/.test(outcomeAt)
        && new Date(`${outcomeAt}T00:00:00Z`).toISOString().slice(0, 10) === outcomeAt;
      if (!roundTrip) return { dropped: "judgment.outcome_knowable_at must be a valid YYYY-MM-DD date" };
    }
    let confidence: number | undefined;
    // null = "not provided" (absent); anything else non-number is a sender
    // bug — Number() coercion would seal a confidence the caller never gave
    // (false→0, "0.7"→0.7; codex P2).
    if (j["claude_confidence"] !== undefined && j["claude_confidence"] !== null) {
      const c = j["claude_confidence"];
      if (typeof c !== "number" || !Number.isFinite(c) || c < 0 || c > 1) {
        return { dropped: "judgment.claude_confidence must be a JSON number 0–1" };
      }
      confidence = c;
    }
    const loop = full("loop");
    if (loop !== undefined && !["A", "B", "both"].includes(loop)) {
      return { dropped: "judgment.loop must be A|B|both" };
    }
    let resolution: Record<string, unknown> | undefined;
    // null = absent, matching claude_confidence and the capture CLI (codex r7).
    if (j["resolution"] !== undefined && j["resolution"] !== null) {
      if (typeof j["resolution"] !== "object" || j["resolution"] === null || Array.isArray(j["resolution"])) {
        return { dropped: "judgment.resolution must be an object" };
      }
      if (JSON.stringify(j["resolution"]).length > 2000) return { dropped: "judgment.resolution too large (>2000 chars)" };
      resolution = j["resolution"] as Record<string, unknown>;
    }
    return {
      judgment: {
        domain,
        outcome_knowable_by: outcomeBy,
        ...(outcomeAt ? { outcome_knowable_at: outcomeAt } : {}),
        ...(claudePrediction ? { claude_prediction: claudePrediction } : {}),
        ...(confidence !== undefined ? { claude_confidence: confidence } : {}),
        ...(loop ? { loop } : {}),
        ...(resolution ? { resolution } : {}),
      },
    };
  } catch (e) {
    return { dropped: `judgment parse error: ${String(e).slice(0, 100)}` };
  }
}

interface WireInput {
  type: WireType;
  punchline: string;
  stakes?: string;
  rec?: string;
  ask?: string;
  options?: string[];
  link?: string;
  /** Suppression/edit-in-place key for fires. Same key within 24h = edit, not re-ping. */
  key?: string;
  /** ISO date/datetime; a call expiring today bypasses the Wednesday queue rule. */
  expires_at?: string;
  origin?: string;
  session_id?: string;
  /** Judgment-capture opt-in (call/signoff). Absent = no capture, ever. */
  judgment?: WireJudgment;
}

export function isConversationalReply(input: Pick<WireInput, "origin">): boolean {
  return /^telegram-responder(?:-|$)/i.test(input.origin ?? "");
}

// ─────────────────────────────────────────────────────────────────────────────
// Incident identity (plan v6 §2.5)
//
// Finding 10: the same unresolved LaunchAgent alert posted twice ("no re-pings")
// because its sender appended secrets.token_hex(4) to the notification key, so each
// occurrence hashed to a different registry slot. Live proof, telegram_outbound:
//   launchagent-health-4126371745a9-b7be76f9   (Jul 27 14:07)
//   launchagent-health-4126371745a9-d1db1829   (Jul 28 00:36)
//   launchagent-health-4126371745a9-ce77dd6d   (Jul 28 09:05)
// Same 12-char condition signature, three random suffixes, three separate pings.
//
// Two — and only two — identity sources are permitted:
//   1. A SENDER-SUPPLIED key, bound to its sender:  ntf|<origin>|<key>
//   2. The STRUCTURAL /event identity:              evt|<client_id>|<scope>|<kind>
//      Severity is deliberately NOT in it: a P1 that escalates to P0 is the SAME
//      unresolved condition and must EDIT the open message, not open a second one.
//
// NO PROSE FOLDING. The old `shortHash(input.key ?? input.punchline)` fallback meant
// two unrelated fires with one shared sentence collapsed into each other, while one
// condition whose wording drifted split into two. A fire with no key gets no
// fingerprint and therefore no edit-in-place — honest, and non-regressive: every fire
// sender observed in the last 30 days supplies a key.
// ─────────────────────────────────────────────────────────────────────────────

/** Marker prefix for the structural /event identity. */
const EVENT_IDENTITY_PREFIX = "evt|";

/** Stable incident fingerprint, or null when the item has no admissible identity.
 *  Contains no random, timestamped, or prose-derived material by construction. */
export function fireFingerprint(input: Pick<WireInput, "key" | "origin">): string | null {
  const key = input.key;
  if (!key) return null;
  if (key.startsWith(EVENT_IDENTITY_PREFIX)) {
    // Structural identity is already fully qualified — binding it to the /event
    // origin would be WRONG: that origin carries the per-event ULID.
    return shortHash(key);
  }
  // Sender-bound. A null origin degrades to a shared unattributed namespace rather
  // than losing edit-in-place for the senders that do not declare one yet (13 of 52
  // observed fire rows). The Phase 4 SENDER_REGISTRY closes that namespace.
  return shortHash(`ntf|${input.origin ?? ""}|${key}`);
}

/** KV registry key for a fingerprint. */
function fireRegistryKey(fingerprint: string): string {
  return `${KV_WIRE_FIRE_PREFIX}${fingerprint}`;
}

/** Per-field hard limits. A unit is included WHOLE or not at all — these are the
 *  boundary at which "not at all" begins, never a slice point. Every value sits far
 *  above the 90-day observed maximum (punchline max 304, rec max 300) so the change
 *  rejects nothing that production actually sends. */
const WIRE_LIMITS = {
  punchline: 1000,
  stakes: 1200,
  rec: 1000,
  ask: 1000,
  option: 120,
  link: 500,
  key: 200,
  origin: 120,
  session_id: 120,
} as const;

export function parseWireInput(
  raw: Record<string, unknown>,
): { ok: true; input: WireInput; judgmentDropped?: string; droppedFields?: string[] } | { ok: false; error: string } {
  const type = String(raw["type"] ?? "").toLowerCase();
  if (!["fire", "call", "signoff", "fyi"].includes(type)) {
    return { ok: false, error: "type must be one of fire|call|signoff|fyi" };
  }
  const punchline = decodeLegacyHtml(String(raw["punchline"] ?? ""));
  if (!punchline) return { ok: false, error: "punchline is required" };
  const str = (k: string) => {
    const v = raw[k];
    return typeof v === "string" && v.trim() ? v.trim() : undefined;
  };
  const plainStr = (k: string) => {
    const v = str(k);
    return v ? decodeLegacyHtml(v) : undefined;
  };
  // ── Source-unit preservation at INGRESS (plan v6 §2.6) ────────────────────
  // The old caps silently sliced: a 304-char punchline became 300 chars with the last
  // word cut in half, and 4 of 50 real punchlines over 90 days hit that cap. Nothing
  // is cut here any more.
  //   · the REQUIRED field (punchline) over its limit is an explicit 400 — the sender
  //     learns immediately instead of Robert receiving a fragment;
  //   · an OPTIONAL field over its limit is DROPPED WHOLE and reported, so the item
  //     still reaches Robert, minus the field nobody could render honestly.
  // Limits sit far above every observed value (max punchline 304 over 90 days), so no
  // production sender is rejected by this change.
  if (punchline.length > WIRE_LIMITS.punchline) {
    return { ok: false, error: `punchline exceeds ${WIRE_LIMITS.punchline} chars (${punchline.length}); shorten it at the source — the relay will not cut it` };
  }
  const droppedFields: string[] = [];
  const whole = (k: string, limit: number, decode: boolean): string | undefined => {
    const v = decode ? plainStr(k) : str(k);
    if (v === undefined) return undefined;
    if (v.length > limit) { droppedFields.push(`${k}:${v.length}>${limit}`); return undefined; }
    return v;
  };
  let options: string[] | undefined;
  const rawOpts = raw["options"];
  if (Array.isArray(rawOpts)) {
    options = rawOpts
      .map((o) => (typeof o === "string" ? o : String((o as Record<string, unknown>)?.["label"] ?? "")))
      .map((s) => decodeLegacyHtml(s))
      .filter(Boolean)
      .slice(0, 4);
    // Options are one SET, not independent units: the numbering IS the reply contract.
    // Dropping option 2 of 3 silently renumbers option 3 into slot 2, so Robert's "2"
    // resolves to a choice he never saw (adversarial review, P2). Include-whole-or-
    // drop-whole therefore applies to the WHOLE set — if any option cannot render, the
    // item falls back to free-text reply rather than offering mislabelled numbers.
    const over = options.filter((o) => o.length > WIRE_LIMITS.option).length;
    if (over > 0) {
      droppedFields.push(`options:whole_set_dropped_${over}_over_${WIRE_LIMITS.option}`);
      options = undefined;
    } else if (options.length === 0) {
      options = undefined;
    }
  }
  const jr = parseWireJudgment(raw["judgment"], type as WireType, Boolean(str("rec")));
  return {
    ok: true,
    input: {
      type: type as WireType,
      punchline,
      stakes: whole("stakes", WIRE_LIMITS.stakes, true),
      rec: whole("rec", WIRE_LIMITS.rec, true),
      ask: whole("ask", WIRE_LIMITS.ask, true),
      options,
      link: whole("link", WIRE_LIMITS.link, false),
      key: whole("key", WIRE_LIMITS.key, false),
      expires_at: str("expires_at"),
      origin: whole("origin", WIRE_LIMITS.origin, false),
      session_id: whole("session_id", WIRE_LIMITS.session_id, false),
      judgment: jr.judgment,
    },
    judgmentDropped: jr.dropped,
    droppedFields: droppedFields.length ? droppedFields : undefined,
  };
}

/** metadata.wire.judgment payload for telegram_outbound — the opt-in fields
 *  plus a snapshot of the call content the sweep needs to build the sealed
 *  judgment row (decision = punchline; rec = default claude_prediction;
 *  options resolve numeric answers). Only present when the sender opted in. */
function wireJudgmentMeta(input: WireInput): Record<string, unknown> | undefined {
  if (!input.judgment) return undefined;
  // A signoff without explicit options still ADVERTISES numbered choices
  // ("1 = approve · 2 = changes" — see renderWire); materialize them here so
  // the sweep resolves a bare "2" to "changes" instead of mis-sealing the rec
  // (codex P1).
  const options = input.options
    ?? (input.type === "signoff" ? ["approve", "changes (say what)"] : null);
  return {
    ...input.judgment,
    punchline: input.punchline,
    stakes: input.stakes ?? null,
    rec: input.rec ?? null,
    options,
  };
}

/** Wire refs are unique BY CONSTRUCTION: prefix + the last 5 Crockford chars of
 *  a fresh ULID (25 bits of randomness at millisecond granularity). Workers KV
 *  has no compare-and-set, so any counter scheme can double-allocate under
 *  concurrency and collide `wire:open:<ref>` state (codex review 2026-07-12,
 *  twice) — random short refs end that class outright. Still short + typeable
 *  ("C-8KQ2M"); sequence aesthetics traded for correctness. */
function nextWireRef(type: WireType): string {
  const prefix = type === "fire" ? "F" : type === "call" ? "C" : "S";
  return `${prefix}-${ulid().slice(-5)}`;
}

const WIRE_EMOJI: Record<WireType, string> = { fire: "🔴", call: "🟡", signoff: "🔵", fyi: "🟢" };

/** Telegram's own hard ceiling is 4096; 3800 leaves headroom for the edit path's
 *  appended update lines. Lines are fitted WHOLE to this budget — never sliced. */
const WIRE_RENDER_BUDGET = 3800;
/** Digest budget. Same rule: whole lines in, or the line is omitted. */
const DIGEST_RENDER_BUDGET = 2200;

/** Render the wire format (Telegram HTML parse mode). One shape per type so
 *  Robert's eye learns it: tag+ref+punchline / stakes / rec / reply protocol / link. */
export function renderWire(input: WireInput, ref: string, updates: string[] = []): string {
  const label: Record<WireType, string> = {
    fire: "Urgent",
    call: "Decision",
    signoff: "Approval",
    fyi: "Update",
  };
  // Each rendered line carries the source units it must reproduce VERBATIM. Tracking
  // the pairing explicitly (rather than inferring it from the finished string) is what
  // makes the preservation assertion exact: a DROPPED line takes its units out of the
  // assertion set, while a MUTATED line keeps them in and trips.
  type Line = { text: string; units: string[] };
  const lines: Line[] = [];
  const plain = (t: string, units: string[] = []) => lines.push({ text: t, units });

  // The wire ref remains in stored metadata for direct-reply resolution. It is
  // intentionally hidden from a live message: Robert replies to the message,
  // not to an internal code. Digests still show refs where one message contains
  // multiple answerable decisions.
  plain(`${WIRE_EMOJI[input.type]} <b>${label[input.type]}: ${escapeHtml(input.punchline)}</b>`, [escapeHtml(input.punchline)]);
  if (input.stakes) plain(escapeHtml(input.stakes), [escapeHtml(input.stakes)]);
  if (input.rec) plain(`<b>My rec: ${escapeHtml(input.rec)}</b>`, [escapeHtml(input.rec)]);
  if (input.ask) plain(escapeHtml(input.ask), [escapeHtml(input.ask)]);
  if (input.type === "call" || input.type === "signoff") {
    if (input.options && input.options.length > 0) {
      plain(
        `Reply: ${input.options.map((o, i) => `${i + 1} = ${escapeHtml(o)}`).join(" · ")}`,
        input.options.map((o) => escapeHtml(o)),
      );
    } else if (input.type === "signoff") {
      plain("Reply: 1 = approve · 2 = changes (say what)");
    } else {
      plain("Reply with your call — or 👍 to take my rec.");
    }
  }
  if (input.link) plain(escapeHtml(input.link), [escapeHtml(input.link)]);
  if (input.type === "fire") {
    if (updates.length > 0) {
      plain("<b>Updates:</b>");
      for (const u of updates.slice(-WIRE_FIRE_MAX_UPDATES)) plain(escapeHtml(u), [escapeHtml(u)]);
    } else {
      plain("<i>Updates will edit this message — no re-pings.</i>");
    }
  }

  // Fit WHOLE lines to the Telegram budget instead of slicing the joined string.
  // The header (which carries the punchline — the required field) is never a drop
  // candidate: an item that cannot show its punchline is deferred by the caller, never
  // half-rendered.
  const header = lines[0];
  const rest = lines.slice(1);
  const budget = WIRE_RENDER_BUDGET - header.text.length - 1;
  const kept: Line[] = [];
  let used = 0;
  let droppedCount = 0;
  for (const l of rest) {
    const cost = l.text.length + 1;
    if (used + cost > budget) { droppedCount += 1; continue; }
    kept.push(l);
    used += cost;
  }
  if (droppedCount > 0) {
    console.warn(JSON.stringify({
      where: "renderWire", warn: "units_dropped_whole", ref, type: input.type, dropped: droppedCount,
    }));
  }
  const text = [header, ...kept].map((l) => l.text).join("\n");
  assertSourcePreservation(text, [header, ...kept].flatMap((l) => l.units));
  return safeHtmlTruncate(text, WIRE_RENDER_BUDGET);
}

async function editTelegramMessage(
  botToken: string,
  chatId: string | number,
  messageId: number,
  text: string,
): Promise<TelegramSendResult> {
  // CAPTURE short-circuit (§2.10): the edit is recorded against the SAME message id
  // the capture send returned, which is what makes "one post, then edits" observable.
  if (botToken === CAPTURE_BOT_TOKEN) {
    return { ok: true, status: 200, telegramMessageId: messageId, response: { capture: true, edited: true, message_id: messageId } };
  }
  const res = await fetch(`${TELEGRAM_API_BASE}/bot${botToken}/editMessageText`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, message_id: messageId, text, parse_mode: "HTML" }),
  });
  const data = (await res.json().catch(() => ({ ok: false, description: "telegram_non_json_response" }))) as {
    ok: boolean;
    description?: string;
    result?: { message_id?: number };
  };
  // Telegram rejects a no-op edit; for our purposes an identical message IS current.
  if (!res.ok || !data.ok) {
    if ((data.description ?? "").includes("message is not modified")) {
      return { ok: true, status: res.status, telegramMessageId: messageId, response: data };
    }
    return { ok: false, status: res.status, error: data.description ?? `HTTP ${res.status}`, response: data };
  }
  return { ok: true, status: res.status, telegramMessageId: data.result?.message_id ?? messageId, response: data };
}

export async function enqueueDigestItem(
  env: Env,
  item: { wire_type: WireType; punchline: string; link?: string; origin?: string; reason: string; key?: string },
): Promise<void> {
  // A typed FYI key is a merge key, not merely metadata. Repeated stages of
  // the same team-owned incident overwrite the queued note so the digest shows
  // only the latest state. Unkeyed notes retain append-only behavior.
  const queueKey = item.key
    ? `${KV_WIRE_DIGESTQ_PREFIX}merge:${shortHash(item.key)}`
    : `${KV_WIRE_DIGESTQ_PREFIX}${ulid()}`;
  await env.BWM_TELEGRAM_KV.put(
    queueKey,
    JSON.stringify({ ...item, ts: new Date().toISOString() }),
    { expirationTtl: WIRE_DIGESTQ_TTL_SECONDS },
  );
}

/** Judgment stamp recovered from a wire:open registry entry's stored input —
 *  carried into the wire-decision narrative itself so the capture join
 *  survives even when the outbound audit row's best-effort Supabase insert
 *  failed (codex r3: covers every path that emits a wire-decision, incl.
 *  deferred items answered by digest ref before redelivery). KNOWN BOUNDARY
 *  (codex r7, accepted): direct replies/reactions resolve via the Supabase
 *  outbound row (v2.2 property, judgment-independent) — if that row's insert
 *  failed, the direct answer doesn't register AT ALL (no decision, so no
 *  capture either); the item stays open and resolves via the digest-by-ref
 *  path, which reads this registry and carries the stamp. Hardening that
 *  decision path (message-id→ref KV index) is wire-system work outside the
 *  judgment wiring. Never throws — a malformed entry just means no stamp. */
function registryJudgmentMeta(
  reg: Record<string, unknown> | null | undefined,
): Record<string, unknown> | undefined {
  if (!reg) return undefined;
  try {
    const input = reg["input"] as WireInput | undefined;
    if (!input || !input.judgment) return undefined;
    return wireJudgmentMeta(input);
  } catch {
    return undefined;
  }
}

interface WireResult {
  ok: boolean;
  action: string;
  ref?: string;
  error?: string;
}

/** Guarded entry point (plan v6 §2.1 typed failure posture).
 *
 *  assertSourcePreservation THROWS by design — a violated invariant means the renderer
 *  is about to show Robert a fragment, and a fragment is the defect. But an uncaught
 *  throw here would surface as a request-wide 500 from the top-level fetch handler, and
 *  the relay is a production surface Robert depends on: it must never go dark.
 *
 *  So the invariant fails CLOSED rather than loudly: the item is NOT sent (no fragment
 *  reaches the phone), a typed result comes back, and the failure is logged for repair.
 *  Unit tests still call renderWire directly and still get the throw. */
async function dispatchWire(
  env: Env,
  input: WireInput,
  sourceRoute: string,
  origin?: { originEventId?: string | null; originEventType?: string | null },
): Promise<WireResult> {
  try {
    return await dispatchWireInner(env, input, sourceRoute, origin);
  } catch (e) {
    const detail = String(e).slice(0, 300);
    console.error(JSON.stringify({
      where: "dispatchWire.guard", error: "dispatch_failed_closed", detail,
      source_route: sourceRoute, type: input.type, origin: input.origin ?? null,
    }));
    try {
      await createOutboundLog(env, {
        sourceRoute,
        originEventId: origin?.originEventId ?? null,
        originEventType: origin?.originEventType ?? null,
        originSessionId: input.session_id ?? null,
        parseMode: "HTML",
        text: input.punchline,
        status: "skipped",
        metadata: {
          reason: "dispatch_failed_closed", detail,
          wire: { type: input.type, origin: input.origin ?? null },
          ...(isCaptureMode(env) ? { capture: true } : {}),
        },
      });
    } catch { /* audit is best-effort here; never re-throw out of the guard */ }
    return { ok: false, action: "deferred_dispatch_failed_closed", error: detail };
  }
}

/** Core wire dispatch. Self-contained (mints its own token) so it can run
 *  synchronously from /notify or inside waitUntil from /event. `origin` carries
 *  the operational-event identity for /event fires so replies like "resolved"
 *  keep resolving the underlying incident (codex review 2026-07-12). */
async function dispatchWireInner(
  env: Env,
  input: WireInput,
  sourceRoute: string,
  origin?: { originEventId?: string | null; originEventType?: string | null },
): Promise<WireResult> {
  const et = etNow();
  const conversationalReply = isConversationalReply(input);
  const interactionMeta = conversationalReply
    ? { interaction: "conversational_reply" }
    : {};

  // ── Kill list, layer 1: INGRESS (plan v6 §2.8) ────────────────────────────
  // Active in EVERY mode and on EVERY route that reaches dispatchWire, so there is
  // exactly one door. A match is dropped here — never queued, never sent, never
  // resurfaced by a later digest — and leaves an auditable skipped row.
  if (isEmailAsRobertOrigin(input.origin)
      || isEmailAsRobert(input.punchline, input.stakes, input.rec, input.ask, ...(input.options ?? []))) {
    await createOutboundLog(env, {
      sourceRoute,
      originEventId: origin?.originEventId ?? null,
      originEventType: origin?.originEventType ?? null,
      originSessionId: input.session_id ?? null,
      parseMode: "HTML",
      text: input.punchline,
      status: "skipped",
      metadata: {
        reason: "email_as_robert_disabled",
        wire: { type: input.type, origin: input.origin ?? null, killed_at: "ingress" },
        ...(isCaptureMode(env) ? { capture: true } : {}),
      },
    });
    console.warn(JSON.stringify({
      where: "dispatchWire.killList", warn: "email_as_robert_blocked_at_ingress",
      origin: input.origin ?? null, type: input.type, source_route: sourceRoute,
    }));
    return { ok: true, action: "skipped_email_as_robert" };
  }
  let budgetReservedKey: string | null = null;
  const releaseBudget = async () => {
    if (!budgetReservedKey) return;
    try {
      const n = parseInt((await env.BWM_TELEGRAM_KV.get(budgetReservedKey)) ?? "0", 10) || 0;
      await env.BWM_TELEGRAM_KV.put(budgetReservedKey, String(Math.max(0, n - 1)), {
        expirationTtl: WIRE_BUDGET_TTL_SECONDS,
      });
    } catch (e) {
      console.error(JSON.stringify({ where: "dispatchWire.releaseBudget", error: String(e) }));
    }
  };

  // FYI never interrupts — straight to the digest queue.
  if (input.type === "fyi") {
    await enqueueDigestItem(env, {
      wire_type: "fyi", punchline: input.punchline, link: input.link, origin: input.origin, reason: "fyi", key: input.key,
    });
    await createOutboundLog(env, {
      sourceRoute,
      originEventId: origin?.originEventId ?? null,
      originEventType: origin?.originEventType ?? null,
      originSessionId: input.session_id ?? null, parseMode: "HTML",
      text: renderWire(input, "FYI"), status: "queued",
      metadata: { wire: { type: "fyi", queued: "digest", origin: input.origin ?? null, key: input.key ?? null } },
    });
    return { ok: true, action: "queued_digest" };
  }

  const chatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!chatId) return { ok: false, action: "skipped_no_chat_id", error: "chat_id not captured yet" };

  // call/signoff gates: quiet hours, Wednesday (Robert's no-meeting day), budget.
  if ((input.type === "call" || input.type === "signoff") && !conversationalReply) {
    const expiresToday = input.expires_at ? etDateOf(input.expires_at) === et.date : false;
    const quiet = et.hour >= WIRE_QUIET_START_HOUR || et.hour < WIRE_QUIET_END_HOUR;
    const wednesday = et.weekday === "Wed" && !expiresToday;
    let deferReason: string | null = quiet ? "quiet_hours" : wednesday ? "wednesday" : null;
    const budgetKey = `${KV_WIRE_BUDGET_PREFIX}${et.date}`;
    const spent = parseInt((await env.BWM_TELEGRAM_KV.get(budgetKey)) ?? "0", 10) || 0;
    if (!deferReason && spent >= WIRE_INTERRUPT_HARD_CAP) deferReason = "budget";
    if (!deferReason) {
      // Reserve the interrupt-budget slot BEFORE the send (KV has no CAS; the
      // stamp-then-release pattern matches the rate limiter — codex r2). The
      // slot is released on mint/send failure below.
      // ACCEPTED TRADEOFF (codex r3 re-flag, rejected): true atomicity needs a
      // Durable Object. Callers are sequential crons/CLIs at low live volume;
      // the worst concurrent race yields cap+1 — bounded and harmless next to
      // the 33/day baseline this gate replaces. Same reasoning the 2026-07-05
      // review accepted for the incident rate limiter.
      await env.BWM_TELEGRAM_KV.put(budgetKey, String(spent + 1), { expirationTtl: WIRE_BUDGET_TTL_SECONDS });
      budgetReservedKey = budgetKey;
    }
    if (deferReason) {
      // A deferred decision stays a DECISION: allocate its ref now and register
      // the full input in wire:open so the digest lists it under "Waiting on
      // you" (answerable by ref) instead of flattening it to a note that gets
      // flushed and lost (codex review 2026-07-12).
      const ref = nextWireRef(input.type);
      await env.BWM_TELEGRAM_KV.put(`${KV_WIRE_OPEN_PREFIX}${ref}`, JSON.stringify({
        type: input.type, punchline: input.punchline, rec: input.rec ?? null,
        link: input.link ?? null, options: input.options ?? null,
        message_id: null, deferred: deferReason, input,
        ts: new Date().toISOString(),
      }), { expirationTtl: WIRE_OPEN_TTL_SECONDS });
      const deferredJudgment = wireJudgmentMeta(input);
      await createOutboundLog(env, {
        sourceRoute,
        originEventId: origin?.originEventId ?? null,
        originEventType: origin?.originEventType ?? null,
        originSessionId: input.session_id ?? null, parseMode: "HTML",
        text: renderWire(input, ref), status: "queued",
        metadata: { wire: { type: input.type, ref, queued: "digest", reason: deferReason, origin: input.origin ?? null, ...interactionMeta, ...(deferredJudgment ? { judgment: deferredJudgment } : {}) } },
      });
      return { ok: true, action: `queued_digest_${deferReason}`, ref };
    }
  }

  let botToken: string;
  try {
    botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
  } catch (e) {
    await releaseBudget();
    return { ok: false, action: "failed", error: `token_mint_failed: ${String(e).slice(0, 200)}` };
  }

  // FIRE edit-in-place, keyed on the STABLE FINGERPRINT (plan v6 §2.5). The registry
  // entry is resolution-bound: no TTL, so an unresolved incident can never age out and
  // re-ping. A fire with no admissible identity gets no registry entry at all.
  const fingerprint = input.type === "fire" ? fireFingerprint(input) : null;
  let fireClaimKey: string | null = null;
  if (input.type === "fire" && fingerprint) {
    const fireKey = fireRegistryKey(fingerprint);
    let existingRaw = await env.BWM_TELEGRAM_KV.get(fireKey);
    // ONE-TIME MIGRATION LOOKUP (adversarial review, P0). Six fires were open in the
    // live registry when the fingerprint landed, stored under the OLD slot
    // shortHash(key ?? punchline). Checking only the new slot would find nothing on
    // recurrence and open a SECOND message for an already-open incident — precisely the
    // re-ping this change exists to remove. So: if the new slot is empty, look in the
    // legacy slot, and on a hit adopt the entry into the new slot and delete the old one
    // (migrate-on-touch). Once the pre-deploy entries drain this branch is dead weight
    // and can be removed; it mints no identity and never runs when the new slot hits.
    if (!existingRaw && input.key) {
      const legacyKey = `${KV_WIRE_FIRE_PREFIX}${shortHash(input.key)}`;
      if (legacyKey !== fireKey) {
        const legacyRaw = await env.BWM_TELEGRAM_KV.get(legacyKey);
        if (legacyRaw) {
          existingRaw = legacyRaw;
          try {
            await env.BWM_TELEGRAM_KV.put(fireKey, legacyRaw);
            await env.BWM_TELEGRAM_KV.delete(legacyKey);
            console.log(JSON.stringify({
              where: "dispatchWire.fireLegacyMigrate", from: legacyKey, to: fireKey, fingerprint,
            }));
          } catch (e) {
            // Migration is best-effort: we already HAVE the entry, so the edit proceeds
            // either way. A failed move just means we migrate again next time.
            console.warn(JSON.stringify({ where: "dispatchWire.fireLegacyMigrate", warn: String(e) }));
          }
        }
      }
    }
    if (existingRaw) {
      try {
        const reg = JSON.parse(existingRaw) as {
          message_id: number; ref: string; base: WireInput; updates: string[]; count: number; first_at: string;
          pending?: boolean;
        };
        if (reg.pending) {
          await createOutboundLog(env, {
            sourceRoute, originEventId: origin?.originEventId ?? null, originEventType: origin?.originEventType ?? null,
            originSessionId: input.session_id ?? null, parseMode: "HTML",
            text: renderWire(input, reg.ref ?? "F-?"), status: "skipped",
            metadata: { reason: "fire_claim_pending", wire: { type: "fire", key: input.key ?? null, origin: input.origin ?? null, fingerprint }, ...(isCaptureMode(env) ? { capture: true } : {}) },
          });
          return { ok: true, action: "skipped_claim_pending", ref: reg.ref };
        }
        reg.count += 1;
        // Whole unit or nothing: a sliced update line is exactly the mid-word
        // fragment finding 11 is about. "repeat ×N" is a COUNT and is banned from
        // Robert-visible text (§2.3) — an unchanged repeat renders a plain timestamp.
        const changed = input.punchline !== reg.base.punchline;
        const detail = changed ? input.punchline : (input.stakes ?? "");
        const updateLine = detail && detail.length <= 300
          ? `↻ ${et.hhmm} ET — ${detail}`
          : `↻ ${et.hhmm} ET — still open`;
        reg.updates = [...reg.updates, updateLine].slice(-WIRE_FIRE_MAX_UPDATES);
        // Latest coalesced incident wins for BOTH resolve paths — digest
        // (base.origin) and direct reply (outbound row) stay consistent (r5).
        if (input.origin) reg.base.origin = input.origin;
        const text = renderWire(reg.base, reg.ref, reg.updates);
        const edit = await editTelegramMessage(botToken, chatId, reg.message_id, text);
        if (edit.ok) {
          // NO expirationTtl: the registry entry is RESOLUTION-bound (§2.5). A TTL
          // is a timer that silently re-arms the re-ping this whole change removes.
          await env.BWM_TELEGRAM_KV.put(fireKey, JSON.stringify({ ...reg, fingerprint, last_at: new Date().toISOString() }));
          const editLogId = await createOutboundLog(env, {
            sourceRoute,
            originEventId: origin?.originEventId ?? null,
            originEventType: origin?.originEventType ?? null,
            originSessionId: input.session_id ?? null, chatId, parseMode: "HTML", text,
            status: "sent",
            metadata: { wire: { type: "fire", ref: reg.ref, action: "edited", count: reg.count, key: input.key ?? null, origin: input.origin ?? null, fingerprint }, ...(isCaptureMode(env) ? { capture: true } : {}) },
          });
          // Stamp the edited row with the message_id so reply lookup (newest-
          // first) resolves the LATEST coalesced incident (codex r2).
          await updateOutboundLog(env, editLogId, {
            status: "sent",
            telegramMessageId: reg.message_id,
          });
          return { ok: true, action: "edited", ref: reg.ref };
        }
        // Only PERMANENT edit failures (message deleted / uneditable) fall
        // through to a fresh send. Transient ones (429/5xx/network) return
        // handled-with-warning — the event row is persisted and the next
        // same-key occurrence retries the edit (codex r5: a storm of 429s
        // must not recreate the re-ping flood).
        const permanent = /message to edit not found|message can't be edited|MESSAGE_ID_INVALID/i.test(edit.error ?? "");
        if (!permanent) {
          console.warn(JSON.stringify({ where: "dispatchWire.fireEdit", warn: "transient_edit_failure", detail: edit.error, ref: reg.ref }));
          await createOutboundLog(env, {
            sourceRoute, originEventId: origin?.originEventId ?? null, originEventType: origin?.originEventType ?? null,
            originSessionId: input.session_id ?? null, parseMode: "HTML", text, status: "failed",
            error: `edit_transient: ${(edit.error ?? "").slice(0, 150)}`,
            metadata: { wire: { type: "fire", ref: reg.ref, action: "edit_failed_transient", key: input.key ?? null, origin: input.origin ?? null, fingerprint }, ...(isCaptureMode(env) ? { capture: true } : {}) },
          });
          return { ok: false, action: "edit_failed_transient", ref: reg.ref, error: edit.error };
        }
        console.warn(JSON.stringify({ where: "dispatchWire.fireEdit", warn: edit.error, ref: reg.ref }));
      } catch (e) {
        // A THROW anywhere in the edit path (network failure, or bookkeeping
        // after Telegram already accepted the edit) is retryable — falling
        // through to a fresh send here is exactly the duplicate-ping this
        // branch exists to suppress (codex r6).
        console.error(JSON.stringify({ where: "dispatchWire.fireRegistry", error: String(e) }));
        return { ok: false, action: "edit_failed_transient", error: String(e).slice(0, 200) };
      }
    }
  }

  const ref = nextWireRef(input.type);
  if (input.type === "fire" && fingerprint) {
    // First send of this FINGERPRINT: claim before Telegram I/O so a concurrent
    // same-fingerprint request skips instead of double-pinging; released on failure.
    // The short TTL here is on the CLAIM only (a crashed request must not wedge the
    // fingerprint forever) — the registry entry that replaces it carries no TTL.
    fireClaimKey = fireRegistryKey(fingerprint);
    await env.BWM_TELEGRAM_KV.put(fireClaimKey, JSON.stringify({ pending: true, ref, fingerprint, ts: new Date().toISOString() }), {
      expirationTtl: 120,
    });
  }
  const text = renderWire(input, ref);
  // metadata.wire.judgment must survive EVERY later metadata rewrite on this
  // row — updateOutboundLog PATCHes metadata wholesale, so each subsequent
  // composition below re-includes it (the wire-sweep joins on it).
  const judgmentMeta = wireJudgmentMeta(input);
  let outboundId: string;
  try {
    outboundId = await createOutboundLog(env, {
      sourceRoute,
      originEventId: origin?.originEventId ?? null,
      originEventType: origin?.originEventType ?? null,
      originSessionId: input.session_id ?? null, chatId, parseMode: "HTML", text, status: "queued",
      metadata: { wire: { type: input.type, ref, key: input.key ?? null, ...(fingerprint ? { fingerprint } : {}), origin: input.origin ?? null, ...interactionMeta, ...(judgmentMeta ? { judgment: judgmentMeta } : {}) }, ...(isCaptureMode(env) ? { capture: true } : {}) },
      requireSupabase: true,
    });
  } catch (e) {
    await releaseBudget();
    if (fireClaimKey) await env.BWM_TELEGRAM_KV.delete(fireClaimKey).catch(() => undefined);
    return {
      ok: false,
      action: "failed_audit_closed",
      ref,
      error: String(e).slice(0, 200),
    };
  }

  let result: TelegramSendResult;
  try {
    result = await sendTelegramMessage(botToken, chatId, text, "HTML");
  } catch (e) {
    // Network-level throw (not a non-OK response) — release the reserved
    // budget slot before reporting the failure (codex r3).
    await updateOutboundLog(env, outboundId, {
      status: "failed", error: `telegram_send_threw: ${String(e).slice(0, 200)}`,
    });
    await releaseBudget();
    if (fireClaimKey) await env.BWM_TELEGRAM_KV.delete(fireClaimKey).catch(() => undefined);
    return { ok: false, action: "failed", ref, error: String(e).slice(0, 200) };
  }
  if (!result.ok) {
    await updateOutboundLog(env, outboundId, {
      status: "failed", error: result.error ?? "telegram_send_failed", telegramResponse: result.response,
    });
    await releaseBudget();
    if (fireClaimKey) await env.BWM_TELEGRAM_KV.delete(fireClaimKey).catch(() => undefined);
    return { ok: false, action: "failed", ref, error: result.error };
  }
  await updateOutboundLog(env, outboundId, {
    status: "sent", telegramMessageId: result.telegramMessageId, telegramResponse: result.response, error: null,
    metadata: { wire: { type: input.type, ref, key: input.key ?? null, ...(fingerprint ? { fingerprint } : {}), origin: input.origin ?? null, ...interactionMeta, ...(judgmentMeta ? { judgment: judgmentMeta } : {}) }, ...(isCaptureMode(env) ? { capture: true } : {}) },
  });
  await env.BWM_TELEGRAM_KV.put(KV_LAST_SEND_AT_KEY, new Date().toISOString());

  if (input.type === "fire" && fingerprint && result.telegramMessageId) {
    // NO expirationTtl (§2.5): the entry lives until the incident is RESOLVED. The
    // 15-min collapse sweep escalates anything still open past WIRE_FIRE_STALE_MS to
    // the team as a task — it never re-pings Robert.
    await env.BWM_TELEGRAM_KV.put(fireRegistryKey(fingerprint), JSON.stringify({
      message_id: result.telegramMessageId, ref, base: input, updates: [], count: 1, fingerprint,
      first_at: new Date().toISOString(), last_at: new Date().toISOString(), outbound_id: outboundId,
    }));
  }

  if (input.type === "call" || input.type === "signoff") {
    // Phase 2 (Close the Loop): a live decision on the wire IS a task Robert
    // owes — mirror it into command_tasks via task.queued fan-out. §12: only
    // wire call/signoff flows may create assignee=robert tasks. The answer
    // resolves it (task.resolved rides the wire-decision paths).
    const taskEventId = await emitWireTaskQueued(env, input, ref, false);
    await env.BWM_TELEGRAM_KV.put(`${KV_WIRE_OPEN_PREFIX}${ref}`, JSON.stringify({
      type: input.type, punchline: input.punchline, rec: input.rec ?? null,
      link: input.link ?? null, options: input.options ?? null,
      message_id: result.telegramMessageId ?? null, outbound_id: outboundId,
      task_event_id: taskEventId,
      input, ts: new Date().toISOString(),
    }), { expirationTtl: WIRE_OPEN_TTL_SECONDS });
    if (taskEventId) {
      // Stamp the mirror's EXACT event id on the outbound row: a late reply
      // (post-TTL) then resolves this task by identity, not by a
      // newest-by-ref guess — 5-char refs can collide over the service
      // lifetime (codex r6).
      await updateOutboundLog(env, outboundId, {
        status: "sent",
        metadata: { wire: { type: input.type, ref, key: input.key ?? null, origin: input.origin ?? null, task_event_id: taskEventId, ...(judgmentMeta ? { judgment: judgmentMeta } : {}) } },
      });
    }
    // Budget slot was reserved before the send (see releaseBudget for the
    // failure path); nothing further to count here.
  }

  return { ok: true, action: "sent", ref };
}

/** task.queued emit for a live call/signoff — command_tasks row lands via the
 *  operational_events fan-out trigger (source_event_id = returned ULID). A
 *  null return means the mirror failed; the wire item still works, it just
 *  won't appear on the task board (fail-soft, decision > bookkeeping). */
async function emitWireTaskQueued(
  env: Env,
  input: WireInput,
  ref: string,
  redelivered: boolean,
): Promise<string | null> {
  return emitOperationalEvent(env, "task.queued", {
    title: `Answer ${ref} — ${input.punchline}`.slice(0, 200),
    description: [
      input.stakes ? `Stakes: ${input.stakes}` : null,
      input.rec ? `Rec: ${input.rec}` : null,
      input.options && input.options.length > 0
        ? `Options: ${input.options.map((o, i) => `${i + 1}=${o}`).join(" · ")}`
        : null,
      input.link ?? null,
      "Human decision — Robert answers on the wire (reply or 👍 in Telegram); not agent-executable.",
    ].filter(Boolean).join("\n").slice(0, 1000) || `Wire ${input.type} awaiting Robert's answer.`,
    priority: "P1",
    assignee: "robert",
    created_by: "one-wire",
    source: "bwm-telegram-relay dispatchWire",
    wire_ref: ref,
    wire_type: input.type,
    // Tertiary human-gate signal for text classifiers (the bridge excludes
    // created_by=one-wire at fetch + classify; this phrase also matches its
    // HUMAN_GATE_RE if a future consumer only reads text).
    human_gate: "Robert answers on the wire — human decision, not agent-executable",
    ...(redelivered ? { redelivered: true } : {}),
    // Redundant durable judgment stamp: telegram_outbound's Supabase insert is
    // best-effort (KV-first), so a transient failure there would otherwise
    // lose the ONLY record the wire-sweep joins on and the answered call
    // would silently never capture (codex r2). The sweep falls back to this
    // task.queued payload when no outbound stamp is found.
    ...(input.judgment ? { judgment: wireJudgmentMeta(input) } : {}),
  }, input.session_id ?? "daemon:bwm-telegram-relay");
}

/** task.resolved emit when a wire-decision lands — closes the command_tasks
 *  row the live send queued. Hardenings from the Phase 2 codex review:
 *  (a) a missing task_event_id (registry expired past its 7-day TTL, or a
 *  pre-Phase-2 row) falls back to recovering the mirror's ULID by wire_ref
 *  from operational_events — the command_tasks row outlives KV state;
 *  (b) a transient emit failure parks a retry marker that the daily digest
 *  composers sweep, so the mirrored task can't stay queued forever behind a
 *  one-off Supabase blip;
 *  Returns true iff a resolution was actually emitted. Late paths (registry
 *  gone) must first classify the reply via findQueuedWireMirror — a
 *  follow-up to an answered ref must not resolve anything, and the decision
 *  narrative must persist BEFORE the task closes (codex r7+r8). */
const KV_WIRE_TASKRETRY_PREFIX = "wire:taskretry:";
const WIRE_TASKRETRY_TTL_SECONDS = 7 * 24 * 60 * 60;

/** Classify a late reply's target: {id} = the mirror is STILL QUEUED (this
 *  reply is a genuine late decision) · null = no queued mirror (ordinary
 *  follow-up to an answered ref, or never went live) · "unavailable" = a
 *  substrate read failed and no verdict is possible right now. */
async function findQueuedWireMirror(
  env: Env,
  taskEventId: string | null,
  ref: string,
): Promise<{ id: string } | "unavailable" | null> {
  let id = taskEventId;
  if (!id) {
    // 14-day bound: refs carry 25 bits of randomness and can repeat over the
    // service lifetime (codex r6).
    const refSince = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString();
    const rows = await fetchRows<{ id: string }>(
      env,
      `operational_events?select=id&event_type=eq.task.queued` +
      `&payload->>wire_ref=eq.${encodeURIComponent(ref)}` +
      `&occurred_at=gte.${encodeURIComponent(refSince)}&order=occurred_at.desc&limit=1`,
    );
    if (rows === null) return "unavailable";
    id = rows[0]?.id ?? null;
    if (!id) return null;
  }
  const mirror = await fetchRows<{ status: string }>(
    env,
    `command_tasks?select=status&source_event_id=eq.${encodeURIComponent(id)}&limit=1`,
  );
  if (mirror === null) return "unavailable";
  return mirror[0]?.status === "queued" ? { id } : null;
}

async function emitWireTaskResolved(
  env: Env,
  taskEventId: string | null | undefined,
  ref: string,
  choiceRaw: string,
  sessionId: string,
): Promise<boolean> {
  let id = taskEventId ?? null;
  if (!id) {
    // 14-day bound: refs carry 25 bits of randomness and can repeat over the
    // service lifetime — an unbounded newest-by-ref lookup could resolve a
    // NEWER unrelated task after a collision (codex r6). Within 14 days the
    // collision odds are negligible and the window comfortably covers the
    // 7-day registry TTL this fallback exists for.
    const refSince = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString();
    const rows = await fetchRows<{ id: string }>(
      env,
      `operational_events?select=id&event_type=eq.task.queued` +
      `&payload->>wire_ref=eq.${encodeURIComponent(ref)}` +
      `&occurred_at=gte.${encodeURIComponent(refSince)}&order=occurred_at.desc&limit=1`,
    );
    if (rows === null) {
      // Lookup UNAVAILABLE ≠ no mirror (codex r2): park a ref-only retry —
      // the digest sweep re-runs the lookup when the substrate is back.
      try {
        await env.BWM_TELEGRAM_KV.put(`${KV_WIRE_TASKRETRY_PREFIX}${ref}`, JSON.stringify({
          task_event_id: null, choice: choiceRaw.slice(0, 200), ts: new Date().toISOString(),
        }), { expirationTtl: WIRE_TASKRETRY_TTL_SECONDS });
      } catch (e) {
        console.error(JSON.stringify({ where: "emitWireTaskResolved.lookupRetryMarker", error: String(e) }));
      }
      return false;
    }
    id = rows[0]?.id ?? null;
    if (id) {
      // Fallback is for the TTL-expiry case ONLY. A follow-up reply to an
      // already-ANSWERED ref also lands here (registry gone) — without this
      // status check every follow-up would emit a duplicate task.resolved
      // and pollute completion analytics (codex r4). An UNAVAILABLE status
      // read is retryable, not a verdict — park the marker so a real
      // resolution can't be lost to a transient outage (codex r5); the sweep
      // re-checks status before replaying.
      const mirror = await fetchRows<{ status: string }>(
        env,
        `command_tasks?select=status&source_event_id=eq.${encodeURIComponent(id)}&limit=1`,
      );
      if (mirror === null) {
        try {
          await env.BWM_TELEGRAM_KV.put(`${KV_WIRE_TASKRETRY_PREFIX}${ref}`, JSON.stringify({
            task_event_id: id, choice: choiceRaw.slice(0, 200), ts: new Date().toISOString(),
          }), { expirationTtl: WIRE_TASKRETRY_TTL_SECONDS });
        } catch (e) {
          console.error(JSON.stringify({ where: "emitWireTaskResolved.statusRetryMarker", error: String(e) }));
        }
        return false;
      }
      if (mirror[0]?.status !== "queued") return false;
    }
  }
  if (!id) return false; // never went live — no mirror to close
  const ok = await emitOperationalEvent(env, "task.resolved", {
    task_id: id,
    outcome: "done",
    resolution: `wire-decision ${ref}: "${choiceRaw.slice(0, 200)}"`,
    source: "bwm-telegram-relay wire-decision",
  }, sessionId);
  if (!ok) {
    console.error(JSON.stringify({ where: "emitWireTaskResolved", warn: "task_resolved_emit_failed_parking_retry", ref, task_event_id: id }));
    try {
      await env.BWM_TELEGRAM_KV.put(`${KV_WIRE_TASKRETRY_PREFIX}${ref}`, JSON.stringify({
        task_event_id: id, choice: choiceRaw.slice(0, 200), ts: new Date().toISOString(),
      }), { expirationTtl: WIRE_TASKRETRY_TTL_SECONDS });
    } catch (e) {
      console.error(JSON.stringify({ where: "emitWireTaskResolved.retryMarker", error: String(e) }));
    }
  }
  return ok !== null;
}

/** Sweep parked task-resolve retries (see emitWireTaskResolved). Runs at the
 *  top of both daily digest composers — at most 50 markers/sweep, deleted only
 *  after the emit succeeds. */
async function retryPendingTaskResolves(env: Env): Promise<void> {
  try {
    const list = await env.BWM_TELEGRAM_KV.list({ prefix: KV_WIRE_TASKRETRY_PREFIX, limit: 50 });
    for (const k of list.keys) {
      const raw = await env.BWM_TELEGRAM_KV.get(k.name);
      if (!raw) continue;
      const ref = k.name.slice(KV_WIRE_TASKRETRY_PREFIX.length);
      let parsed: { task_event_id?: string | null; choice?: string } | null = null;
      try { parsed = JSON.parse(raw) as { task_event_id?: string | null; choice?: string }; } catch { /* malformed */ }
      if (!parsed) {
        await env.BWM_TELEGRAM_KV.delete(k.name);
        continue;
      }
      if (!parsed.task_event_id) {
        // Ref-only marker: the mirror lookup was unavailable at decision time
        // (codex r2) — re-run it now. Still unavailable → keep the marker for
        // the next sweep; genuinely no mirror → drop it. Same 14-day bound as
        // emitWireTaskResolved (codex r6, ref reuse).
        const refSince = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString();
        const rows = await fetchRows<{ id: string }>(
          env,
          `operational_events?select=id&event_type=eq.task.queued` +
          `&payload->>wire_ref=eq.${encodeURIComponent(ref)}` +
          `&occurred_at=gte.${encodeURIComponent(refSince)}&order=occurred_at.desc&limit=1`,
        );
        if (rows === null) continue;
        if (!rows[0]?.id) {
          await env.BWM_TELEGRAM_KV.delete(k.name);
          continue;
        }
        parsed.task_event_id = rows[0].id;
      }
      // Status guard before EVERY replay (codex r5): the mirror may have been
      // resolved by another path (late-reply fallback, expiry sweep) while
      // this marker sat parked — replaying then would double-resolve. An
      // unavailable read keeps the marker for the next sweep.
      const mirror = await fetchRows<{ status: string }>(
        env,
        `command_tasks?select=status&source_event_id=eq.${encodeURIComponent(parsed.task_event_id)}&limit=1`,
      );
      if (mirror === null) continue;
      if (mirror[0]?.status !== "queued") {
        await env.BWM_TELEGRAM_KV.delete(k.name);
        continue;
      }
      const ok = await emitOperationalEvent(env, "task.resolved", {
        task_id: parsed.task_event_id,
        outcome: "done",
        resolution: `wire-decision ${ref}: "${(parsed.choice ?? "").slice(0, 200)}" (retried)`,
        source: "bwm-telegram-relay wire-decision-retry",
      }, "daemon:bwm-telegram-relay");
      if (ok) await env.BWM_TELEGRAM_KV.delete(k.name);
    }
  } catch (e) {
    console.error(JSON.stringify({ where: "retryPendingTaskResolves", error: String(e) }));
  }
}

async function handleNotify(request: Request, env: Env): Promise<Response> {
  const key = request.headers.get("X-BWM-Internal-Key") ?? "";
  if (!env.BWM_INTERNAL_KEY || key !== env.BWM_INTERNAL_KEY) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }
  let raw: Record<string, unknown>;
  try {
    raw = (await request.json()) as Record<string, unknown>;
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }
  const parsed = parseWireInput(raw);
  if (!parsed.ok) return json({ ok: false, error: parsed.error }, 400);
  const result = await dispatchWire(env, parsed.input, "/notify");
  // Surface the capture opt-in outcome so senders see a silent drop (the send
  // itself is never blocked by judgment problems — contract: fails safe).
  const judgment = parsed.judgmentDropped
    ? `dropped: ${parsed.judgmentDropped}`
    : parsed.input.judgment
      ? "accepted"
      : undefined;
  // Report dropped fields for the same reason the judgment drop is reported: the
  // sender's copy did not fully reach Robert, and silence about that is how a sender
  // keeps emitting text nobody can render (adversarial review, P2).
  return json({
    ...result,
    ...(judgment ? { judgment } : {}),
    ...(parsed.droppedFields ? { dropped_fields: parsed.droppedFields } : {}),
  }, result.ok ? 200 : 502);
}

/** The reply footer, bound to a ref that is ACTUALLY ON SCREEN (plan v6 §2.4).
 *  Finding 12: the digest shipped `Reply to anything by ref ("C-003: go")` while the
 *  visible refs were S-S4C83 / C-MTPF2 / S-B37RS — a demonstration of an identifier
 *  that did not exist, in a shape the system does not even mint. Returns [] when
 *  nothing answerable rendered, because an instruction with nothing to act on is
 *  noise. Phase 6 replaces refs with 1./1A numbering; this is the interim honest form. */
export function replyFooter(renderedRefs: string[]): string[] {
  if (renderedRefs.length === 0) return [];
  return [`<i>Reply by ref — for example "${escapeHtml(renderedRefs[0])}: go" — or just type what you want.</i>`];
}

/** Join digest lines under the budget by dropping WHOLE trailing lines (§2.6).
 *  safeHtmlTruncate stays as the tripwire behind it, never as the strategy. */
function fitDigest(lines: string[]): string {
  const header = lines[0] ?? "";
  const { kept, dropped } = fitWholeUnits(lines.slice(1), DIGEST_RENDER_BUDGET - header.length - 1);
  if (dropped.length > 0) {
    console.warn(JSON.stringify({ where: "fitDigest", warn: "lines_dropped_whole", dropped: dropped.length }));
  }
  return safeHtmlTruncate([header, ...kept].join("\n"), DIGEST_RENDER_BUDGET);
}

/** Read + parse every wire:open registry entry, oldest first by enqueue time —
 *  refs carry random suffixes, so a ref sort could hide an old item behind the
 *  display cap forever (codex r3). Shared by Day Done, Day Ahead, redelivery. */
async function listOpenWireItems(env: Env): Promise<Array<Record<string, unknown> & { ref: string }>> {
  // Follow the KV cursor: past 100 open refs a single page is an ARBITRARY
  // subset, so its "oldest" is not the global oldest and omitted decisions
  // could expire without ever surfacing (codex r8). 10-page ceiling = 1000
  // keys, far past any real backlog.
  const keys: { name: string }[] = [];
  let cursor: string | undefined;
  for (let page = 0; page < 10; page++) {
    const res = await env.BWM_TELEGRAM_KV.list({ prefix: KV_WIRE_OPEN_PREFIX, limit: 100, cursor });
    keys.push(...res.keys);
    if (res.list_complete) break;
    cursor = (res as { cursor?: string }).cursor;
    if (!cursor) break;
  }
  return (await Promise.all(keys.map(async (k) => {
    const raw = await env.BWM_TELEGRAM_KV.get(k.name);
    if (!raw) return null;
    try {
      return { ref: k.name.slice(KV_WIRE_OPEN_PREFIX.length), ...(JSON.parse(raw) as Record<string, unknown>) };
    } catch { return null; }
  }))).filter((x): x is Record<string, unknown> & { ref: string } => !!x)
    .sort((a, b) => String(a["ts"] ?? "").localeCompare(String(b["ts"] ?? "")));
}

/** Render the "Waiting on you" bullet list (shared: Day Done + Day Ahead).
 *  10 oldest shown; a standing backlog past that is a budget/scorecard failure
 *  the Friday line exposes — not something to hide in rendering (codex r4). */
export function waitingOnYouLines(openItems: Array<Record<string, unknown> & { ref: string }>): { lines: string[]; renderedRefs: string[] } {
  const lines: string[] = [];
  const renderedRefs: string[] = [];
  if (openItems.length === 0) {
    lines.push("<b>Waiting on you:</b> nothing — all clear.");
    return { lines, renderedRefs };
  }
  // NO COUNT (plan v6 §2.3, finding 6). "Waiting on you (45)" told Robert he was 45
  // items behind while showing three, and 42 of those 45 were never his work. The
  // header now labels what is actually on screen, and nothing else.
  lines.push("<b>Waiting on you:</b>");
  const CHAR_BUDGET = 900;
  let used = 0;
  for (const o of openItems.slice(0, 3)) {
    // Every field is included WHOLE or omitted (§2.6). No .slice() anywhere: the
    // reviewed digest showed "rec: Say go — I'll merge + deploy PR #39 now to fix t"
    // and "2=Wait for Sol's cross-che", which is a decision Robert cannot act on.
    const punchline = String(o["punchline"] ?? "");
    if (!punchline) continue;
    const rec = String(o["rec"] ?? "");
    const optsArr = Array.isArray(o["options"]) ? (o["options"] as unknown[]).map(String) : [];
    const opts = optsArr.map((v, i) => `${i + 1}=${v}`).join(" · ");
    // Full href, short label — a truncated URL is worse than none (codex r6).
    const linkRaw = String(o["link"] ?? "");
    const link = linkRaw
      ? `<a href="${linkRaw.replace(/&/g, "&amp;").replace(/"/g, "&quot;")}">open</a>`
      : "";
    // The internal hold-state label ("(held: budget)") is a queue token, not business
    // meaning — banned from Robert-visible text (finding 9, rule 6).
    const parts = [`• ${escapeHtml(o.ref)} — ${escapeHtml(punchline)}`];
    if (rec && rec.length <= 240) parts.push(`<b>rec:</b> ${escapeHtml(rec)}`);
    if (opts && opts.length <= 240) parts.push(escapeHtml(opts));
    if (link) parts.push(link);
    const line = parts.join(" · ");
    if (used + line.length > CHAR_BUDGET) break;
    lines.push(line);
    used += line.length + 1;
    renderedRefs.push(o.ref);
  }
  // NO "+N more" (§2.3, finding 6): undisplayed backlog is not Robert's work and is
  // never appended to his list. Unanswered items resurface in a later digest on their
  // own; the standing backlog is a budget/scorecard problem, not a rendering one.
  if (renderedRefs.length === 0) {
    // Items exist but none could be rendered whole. A bare header with nothing under it
    // is worse than either outcome, and claiming "all clear" would be a lie.
    return { lines: ["<b>Waiting on you:</b> nothing that renders cleanly this run."], renderedRefs };
  }
  return { lines, renderedRefs };
}

/** Append the queued-notes section under an explicit LENGTH budget: only notes
 *  that actually fit the message get flushed; the rest roll to the next digest.
 *  Deleting a note the truncation swallowed would silently discard it (codex
 *  r6). Returns the KV keys of the notes actually rendered — the caller deletes
 *  them ONLY after the digest send succeeds. Shared: Day Done + Day Ahead. */
export function appendNotesSection(
  lines: string[],
  qItems: Array<Record<string, unknown> & { key: string }>,
  header: string,
): string[] {
  const renderedNoteKeys: string[] = [];
  if (qItems.length === 0) return renderedNoteKeys;
  const headerIdx = lines.length;
  lines.push("");
  let used = lines.reduce((n, l) => n + l.length + 1, 0);
  for (const it of qItems) {
    if (renderedNoteKeys.length >= 5) break;
    // Whole unit or drop it (§2.6). "oldest: \"Award records:" was a note cut at 72
    // characters, mid-quote, leaving a dangling colon.
    const punchline = String(it["punchline"] ?? "");
    if (!punchline || punchline.length > 240) continue;
    const noteLinkRaw = String(it["link"] ?? "");
    const noteLink = noteLinkRaw
      ? ` — <a href="${noteLinkRaw.replace(/&/g, "&amp;").replace(/"/g, "&quot;")}">open</a>`
      : "";
    const line = `• ${escapeHtml(punchline)}${noteLink}`;
    if (used + line.length > 1800) break; // keep the digest human-scannable
    lines.push(line);
    used += line.length + 1;
    renderedNoteKeys.push(it.key);
  }
  // NO COUNT and NO "+N more" (§2.3): "Notes (234)" plus "…+229 more (next digest)"
  // was 229 system records presented as Robert's personal backlog (finding 6).
  // Un-rendered notes simply stay queued.
  if (renderedNoteKeys.length === 0) { lines.splice(headerIdx, 1); return renderedNoteKeys; }
  lines[headerIdx] = `<b>${header}:</b>`;
  return renderedNoteKeys;
}

/** Read + parse the digest note queue, oldest first. Shared by both digests. */
async function listDigestNotes(env: Env): Promise<Array<Record<string, unknown> & { key: string }>> {
  const qList = await env.BWM_TELEGRAM_KV.list({ prefix: KV_WIRE_DIGESTQ_PREFIX, limit: 1000 });
  return (await Promise.all(qList.keys.map(async (k) => {
    const raw = await env.BWM_TELEGRAM_KV.get(k.name);
    if (!raw) return null;
    try { return { key: k.name, ...(JSON.parse(raw) as Record<string, unknown>) }; } catch { return null; }
  }))).filter((x): x is Record<string, unknown> & { key: string } => !!x)
    .sort((a, b) => String(a["ts"] ?? "").localeCompare(String(b["ts"] ?? "")));
}

/** Compose + send the Day Done digest: open fires, shipped (last 24h rolling —
 *  avoids DST math), waiting-on-you, queued notes. Always sends, even on a
 *  quiet day — the empty digest is the trust signal that silence = nothing. */
/** Shape of a wire:fire:<fingerprint> registry entry as the digest reads it. */
interface FireRegistryEntry {
  ref: string;
  base: WireInput;
  count: number;
  first_at?: string;
  last_at?: string;
  escalated?: boolean;
}

async function composeAndSendDayDone(env: Env, trigger: string): Promise<WireResult & { items?: number }> {
  const chatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!chatId) return { ok: false, action: "skipped_no_chat_id" };
  await retryPendingTaskResolves(env);
  const et = etNow();

  // Queued notes
  const qItems = await listDigestNotes(env);

  // Waiting on you
  const openItems = await listOpenWireItems(env);

  // Open fires
  const fireList = await env.BWM_TELEGRAM_KV.list({ prefix: KV_WIRE_FIRE_PREFIX, limit: 50 });
  const fireItems = (await Promise.all(fireList.keys.map(async (k) => {
    const raw = await env.BWM_TELEGRAM_KV.get(k.name);
    if (!raw) return null;
    try { return JSON.parse(raw) as FireRegistryEntry; } catch { return null; }
  }))).filter((x): x is FireRegistryEntry =>
    // Skip in-flight {pending:true} claims + malformed rows — rendering one
    // would throw on base.punchline and abort the whole digest (codex r5).
    !!x && !!(x as { base?: WireInput }).base && typeof (x as { base?: WireInput }).base?.punchline === "string");

  // Shipped — rolling 24h from operational_events
  let shippedCount = 0;
  let shippedOk = false;
  if (supabaseConfigured(env)) {
    try {
      const since = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
      const q = `operational_events?event_type=eq.build.shipped&occurred_at=gte.${encodeURIComponent(since)}` +
        `&select=payload,client_id&order=occurred_at.desc&limit=50`;
      const resp = await fetch(supabaseRestUrl(env, q), { headers: supabaseHeaders(env) });
      if (resp.ok) {
        shippedOk = true;
        const rows = (await resp.json()) as Array<{ payload: Record<string, unknown> | null; client_id: string | null }>;
        shippedCount = rows.length;
      }
    } catch (e) {
      console.error(JSON.stringify({ where: "composeDayDone.shipped", error: String(e) }));
    }
  }

  const lines: string[] = [];
  lines.push(`🌙 <b>DAY DONE — ${escapeHtml(et.label)}</b>`);
  if (fireItems.length > 0) {
    // NO COUNT in the header and NO "(×N)" repeat marker (plan v6 §2.3). The ×4 on the
    // Google Ads line told Robert an alarm had fired four times without telling him
    // whether anything had been done about it — pure stress, zero decision value
    // (finding 5). Punchlines render WHOLE or the line is dropped (§2.6).
    // Build the bullets FIRST, then emit the header only if at least one survived the
    // whole-unit check. Pushing the header up front shipped a header with nothing under
    // it whenever every candidate was too long to render whole (adversarial review, P1
    // — the same defect waitingOnYouLines was already fixed for).
    // Most-recent first: KV list order is lexicographic by fingerprint, which is
    // effectively random, so without this an old escalated fire can crowd out today's.
    const fireBullets = fireItems
      .slice()
      .sort((a, b) => String(b.last_at ?? b.first_at ?? "").localeCompare(String(a.last_at ?? a.first_at ?? "")))
      .filter((f) => f.base.punchline && f.base.punchline.length <= 240)
      .slice(0, 3)
      .map((f) => `• ${escapeHtml(f.ref)} — ${escapeHtml(f.base.punchline)}`);
    if (fireBullets.length > 0) {
      lines.push("<b>Still open:</b>");
      lines.push(...fireBullets);
    }
  }
  // The raw "Shipped (last 24h): 10" line is REMOVED (plan v6 §2.3): a bare count of
  // internal build events is not an outcome Robert can use, and §2.2 makes the recap
  // completed-outcomes-only. The count survives in metadata for telemetry. Phase 6
  // replaces this with path-bound outcome rendering; until then, honest silence.
  // (shippedCount / shippedOk are carried into the outbound metadata below.)
  // Friday scorecard (Phase 2): the weekly comms SLO rides the Day Done digest
  // + lands as narrative kind=comms-slo so the trend is queryable. Fail-soft:
  // an unavailable substrate renders as unavailable, never a false zero.
  // Placed BEFORE the variable-length waiting list AND the length-budgeted
  // notes: it is 3 fixed lines and must survive a busy Friday (codex r5+r6).
  if (etNow().weekday === "Fri") {
    const sc = await computeCommsScorecard(env);
    if (sc) {
      lines.push(...scorecardLines(sc));
      await emitCommsSlo(env, sc);
    } else {
      lines.push("<b>Comms scorecard:</b> (data unavailable)");
    }
  }
  const waiting = waitingOnYouLines(openItems);
  lines.push(...waiting.lines);
  const renderedNoteKeys = appendNotesSection(lines, qItems, "Notes");
  if (BOARD_URL) lines.push(`<i>Detail: ${escapeHtml(BOARD_URL)}</i>`);
  // Interim footer fix (plan v6 §2.4 / finding 12): the hard-coded "C-003" example was
  // never on screen and did not even match the live ref shape, so it could not be used.
  // The footer now names the FIRST ref that actually rendered, and is omitted entirely
  // when nothing answerable is on screen.
  lines.push(...replyFooter(waiting.renderedRefs));
  // Kill list, layer 3: FINAL RENDER (plan v6 §2.8). Anything carrying an
  // email-as-Robert signature is removed from the composed payload immediately before
  // send — so a signature arriving through a path nobody enumerated still cannot reach
  // the phone. A nonzero removal count is a defect signal, not routine hygiene.
  const scrubbed = scrubEmailAsRobert(fitDigest(lines));
  if (scrubbed.removed > 0) {
    console.error(JSON.stringify({
      where: "composeDayDone", error: "email_as_robert_reached_final_render",
      removed: scrubbed.removed, detail: "an ingress path is unaccounted for",
    }));
  }
  const text = scrubbed.text;

  let botToken: string;
  try {
    botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
  } catch (e) {
    return { ok: false, action: "failed", error: `token_mint_failed: ${String(e).slice(0, 200)}` };
  }
  const outboundId = await createOutboundLog(env, {
    sourceRoute: "/digest", chatId, parseMode: "HTML", text, status: "queued",
    // Counts live HERE, in metadata, where telemetry can use them — never on screen.
    metadata: {
      wire: { type: "digest", kind: "day-done", trigger, notes: qItems.length, waiting: openItems.length, fires: fireItems.length, shipped_24h: shippedOk ? shippedCount : null, killed_at_final_render: scrubbed.removed },
      ...(isCaptureMode(env) ? { capture: true } : {}),
    },
    requireSupabase: true,
  });
  const result = await sendTelegramMessage(botToken, chatId, text, "HTML");
  if (!result.ok) {
    await updateOutboundLog(env, outboundId, {
      status: "failed", error: result.error ?? "telegram_send_failed", telegramResponse: result.response,
    });
    return { ok: false, action: "failed", error: result.error };
  }
  await updateOutboundLog(env, outboundId, {
    status: "sent", telegramMessageId: result.telegramMessageId, telegramResponse: result.response, error: null,
  });
  await env.BWM_TELEGRAM_KV.put(KV_LAST_SEND_AT_KEY, new Date().toISOString());
  // Flush ONLY the notes that actually made it into the delivered message —
  // overflow (count OR length) rolls to the next digest (codex r6).
  await Promise.all(renderedNoteKeys.map((k) => env.BWM_TELEGRAM_KV.delete(k)));
  return { ok: true, action: "sent", items: qItems.length };
}

/** Fire collapse sweep (plan v6 §2.5), run on the 15-min cron.
 *
 *  The registry is resolution-bound and carries no TTL, so an incident nobody closes
 *  would otherwise sit in it forever. This sweep is the bounded end of that lifecycle:
 *  an entry still open past WIRE_FIRE_STALE_MS is handed to the TEAM as a task exactly
 *  once (`escalated` latches) and is then left alone. Robert is never pinged a second
 *  time for the same fingerprint — that is the whole point of the fingerprint.
 *
 *  Also collapses the debris a crashed dispatch can leave: a {pending:true} claim whose
 *  120s TTL has passed but whose key is still readable.
 *
 *  Emits telemetry (open / stale / escalated / collapsed) so a rising open count is
 *  visible before it becomes a backlog. Fail-soft throughout: a sweep problem must
 *  never take down the heartbeat cron it shares. */
async function sweepFireRegistry(env: Env): Promise<void> {
  const now = Date.now();
  let open = 0, stale = 0, escalated = 0, collapsed = 0;
  const list = await env.BWM_TELEGRAM_KV.list({ prefix: KV_WIRE_FIRE_PREFIX, limit: 1000 });
  for (const k of list.keys) {
    let raw: string | null;
    try { raw = await env.BWM_TELEGRAM_KV.get(k.name); } catch { continue; }
    if (!raw) continue;
    let reg: Record<string, unknown>;
    try { reg = JSON.parse(raw) as Record<string, unknown>; } catch {
      await env.BWM_TELEGRAM_KV.delete(k.name).catch(() => undefined);
      collapsed += 1;
      continue;
    }
    // Stranded pre-send claim: no message ever landed, so nothing can edit it.
    if (reg["pending"] === true) {
      const ts = Date.parse(String(reg["ts"] ?? ""));
      if (Number.isFinite(ts) && now - ts > 5 * 60 * 1000) {
        await env.BWM_TELEGRAM_KV.delete(k.name).catch(() => undefined);
        collapsed += 1;
      }
      continue;
    }
    open += 1;
    const first = Date.parse(String(reg["first_at"] ?? ""));
    if (!Number.isFinite(first) || now - first <= WIRE_FIRE_STALE_MS) continue;
    stale += 1;
    if (reg["escalated"] === true) {
      // TERMINAL CLEANUP (adversarial review, P2). Removing the TTL was deliberate, but
      // an escalated entry that never gets a reply would otherwise live forever and
      // eventually crowd newer fires out of the 3-item digest preview. Once the team has
      // owned it for a full stale window, the registry entry has done its job: the
      // incident lives in command_tasks now, which is its real system of record.
      const escalatedAt = Date.parse(String(reg["escalated_at"] ?? ""));
      if (Number.isFinite(escalatedAt) && now - escalatedAt > WIRE_FIRE_STALE_MS) {
        await env.BWM_TELEGRAM_KV.delete(k.name).catch(() => undefined);
        collapsed += 1;
      }
      continue;
    }
    const base = (reg["base"] ?? {}) as { punchline?: string; origin?: string };
    const taskId = await emitOperationalEvent(env, "task.queued", {
      assignee: "team",
      priority: "P2",
      title: `Unresolved incident open >7d: ${String(base.punchline ?? "(no punchline)").slice(0, 140)}`,
      description: "Opened on the wire and never resolved. Routed to the team by the fire collapse sweep — Robert is deliberately NOT re-pinged (plan v6 §2.5).",
      source: "telegram-relay:fire-collapse-sweep",
      fingerprint: String(reg["fingerprint"] ?? k.name.slice(KV_WIRE_FIRE_PREFIX.length)),
      wire_ref: String(reg["ref"] ?? ""),
      origin: base.origin ?? null,
    }, `fire-collapse-sweep-${new Date().toISOString().slice(0, 10)}`);
    if (taskId) {
      // Latch so the escalation happens exactly once per fingerprint.
      await env.BWM_TELEGRAM_KV.put(k.name, JSON.stringify({ ...reg, escalated: true, escalated_at: new Date().toISOString(), escalation_event_id: taskId }))
        .catch(() => undefined);
      escalated += 1;
    }
  }
  console.log(JSON.stringify({ where: "sweepFireRegistry", open, stale, escalated, collapsed, capture: isCaptureMode(env) }));
}

async function handleDigestFlush(request: Request, env: Env): Promise<Response> {
  const key = request.headers.get("X-BWM-Internal-Key") ?? "";
  if (!env.BWM_INTERNAL_KEY || key !== env.BWM_INTERNAL_KEY) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }
  const result = await composeAndSendDayDone(env, "manual_flush");
  return json(result, result.ok ? 200 : 502);
}

// ─────────────────────────────────────────────────────────────────────────────
// One Wire Phase 2 — Day Ahead digest, deferred redelivery, comms scorecard
// (Close the Loop; Attention-Routing-Spec v2.0.0 § 12, PROJ-ONE-WIRE-001)
// ─────────────────────────────────────────────────────────────────────────────

/** Read-only Supabase REST fetch. null = source UNAVAILABLE (render "(data
 *  unavailable)", never a false zero — verify-before-claiming); [] = healthy
 *  empty. */
async function fetchRows<T>(env: Env, query: string): Promise<T[] | null> {
  if (!supabaseConfigured(env)) return null;
  try {
    const resp = await fetch(supabaseRestUrl(env, query), { headers: supabaseHeaders(env) });
    if (!resp.ok) {
      console.error(JSON.stringify({ where: "fetchRows", status: resp.status, query: query.slice(0, 100) }));
      return null;
    }
    return (await resp.json()) as T[];
  } catch (e) {
    console.error(JSON.stringify({ where: "fetchRows", query: query.slice(0, 100), error: String(e) }));
    return null;
  }
}

/** ET offset ("-04:00" / "-05:00") at a given instant via Intl longOffset;
 *  -05:00 fallback on parse failure (digest-grade imprecision, not a
 *  correctness gate). */
function etOffsetAt(d: Date): string {
  const offPart = new Intl.DateTimeFormat("en-US", { timeZone: "America/New_York", timeZoneName: "longOffset" })
    .formatToParts(d).find((p) => p.type === "timeZoneName")?.value ?? "";
  const m = /GMT([+-])(\d{1,2})(?::(\d{2}))?/.exec(offPart);
  const sign = m?.[1] ?? "-";
  const hh = (m?.[2] ?? "5").padStart(2, "0");
  const mm = m?.[3] ?? "00";
  return `${sign}${hh}:${mm}`;
}

/** UTC instant of an ET date's local midnight. Two-pass: derive the offset at
 *  a guessed instant, then re-derive at the refined instant — converges across
 *  DST transitions. */
function etMidnightUtc(etDate: string): Date {
  let guess = new Date(`${etDate}T00:00:00-05:00`);
  for (let i = 0; i < 2; i++) {
    guess = new Date(`${etDate}T00:00:00${etOffsetAt(guess)}`);
  }
  return guess;
}

/** UTC instants of today's ET midnight → tomorrow's ET midnight. BOTH
 *  boundaries derive their own offset (codex Phase 2): on DST transition days
 *  a start-offset + 24h window would shift the New York calendar day by an
 *  hour. */
function etDayRangeUtc(): { startIso: string; endIso: string } {
  const et = etNow();
  const start = etMidnightUtc(et.date);
  // 36h forward is safely inside the next ET day regardless of a 23/25h day.
  const probe = new Date(start.getTime() + 36 * 60 * 60 * 1000);
  const nextDate = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit",
  }).format(probe);
  const end = etMidnightUtc(nextDate);
  return { startIso: start.toISOString(), endIso: end.toISOString() };
}

function etTimeShort(iso: string | null): string {
  if (!iso) return "";
  try {
    return new Intl.DateTimeFormat("en-US", {
      timeZone: "America/New_York", hour: "numeric", minute: "2-digit",
    }).format(new Date(iso));
  } catch { return String(iso); }
}

/** Redeliver decisions that were deferred at send time (quiet hours /
 *  Wednesday / budget) as LIVE interrupts — "redelivery at the first in-hours
 *  digest". 09:00 ET is in-hours by construction (quiet ends 08:00). Budget-
 *  aware: oldest first, stops at the hard cap; what doesn't fit stays in
 *  waiting-on-you. Wednesday still holds non-expiring items (digest+fire
 *  only). Returns the number redelivered. */
async function redeliverDeferredDecisions(env: Env, chatId: string): Promise<number> {
  const et = etNow();
  // The 13:00 UTC cron is in-hours by construction, but the manual
  // /digest/day-ahead endpoint can fire any time — redelivering at 02:00
  // would bypass the exact quiet-hours gate that deferred these items
  // (codex r2 P1). Quiet window → no redelivery; items stay held.
  if (et.hour >= WIRE_QUIET_START_HOUR || et.hour < WIRE_QUIET_END_HOUR) return 0;
  const openEntries = await listOpenWireItems(env);
  const deferredEntries = openEntries.filter((e) =>
    e["deferred"] && !e["message_id"] && e["input"] && typeof e["input"] === "object");
  if (deferredEntries.length === 0) return 0;

  const budgetKey = `${KV_WIRE_BUDGET_PREFIX}${et.date}`;
  let spent = parseInt((await env.BWM_TELEGRAM_KV.get(budgetKey)) ?? "0", 10) || 0;
  let botToken: string | null = null;
  let redelivered = 0;

  for (const entry of deferredEntries) {
    if (spent >= WIRE_INTERRUPT_HARD_CAP) break;
    const input = entry["input"] as WireInput;
    const expiresToday = input.expires_at ? etDateOf(input.expires_at) === et.date : false;
    if (et.weekday === "Wed" && !expiresToday) continue;
    if (!botToken) {
      try {
        botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
      } catch (e) {
        console.error(JSON.stringify({ where: "redeliverDeferred.mint", error: String(e).slice(0, 200) }));
        return redelivered;
      }
    }
    // Reserve the budget slot BEFORE the send (stamp-then-release, same
    // accepted-tradeoff as dispatchWire — KV has no CAS).
    spent += 1;
    await env.BWM_TELEGRAM_KV.put(budgetKey, String(spent), { expirationTtl: WIRE_BUDGET_TTL_SECONDS });
    // Guarded for the same reason as dispatchWire: a preservation violation must skip
    // ONE item, never abort the whole Day Ahead digest mid-loop.
    let text: string;
    try {
      text = renderWire(input, entry.ref);
    } catch (e) {
      // Release the slot we reserved above — nothing was sent, so consuming one of the
      // four daily live interrupts would be a silent loss (adversarial review, P2;
      // matches the mint-failure and send-failure branches below).
      spent -= 1;
      await env.BWM_TELEGRAM_KV.put(budgetKey, String(Math.max(0, spent)), { expirationTtl: WIRE_BUDGET_TTL_SECONDS });
      console.error(JSON.stringify({ where: "redeliverDeferred.render", error: "render_failed_closed", detail: String(e).slice(0, 200), ref: entry.ref }));
      continue;
    }
    const redeliveredJudgment = wireJudgmentMeta(input);
    const outboundId = await createOutboundLog(env, {
      sourceRoute: "/digest/day-ahead",
      originSessionId: input.session_id ?? null, chatId, parseMode: "HTML", text, status: "queued",
      metadata: { wire: { type: input.type, ref: entry.ref, action: "redelivered", origin: input.origin ?? null, ...(redeliveredJudgment ? { judgment: redeliveredJudgment } : {}) } },
      requireSupabase: true,
    });
    let result: TelegramSendResult;
    try {
      result = await sendTelegramMessage(botToken, chatId, text, "HTML");
    } catch (e) {
      spent = Math.max(0, spent - 1);
      await env.BWM_TELEGRAM_KV.put(budgetKey, String(spent), { expirationTtl: WIRE_BUDGET_TTL_SECONDS });
      await updateOutboundLog(env, outboundId, { status: "failed", error: `telegram_send_threw: ${String(e).slice(0, 200)}` });
      continue; // stays deferred; next digest retries
    }
    if (!result.ok) {
      spent = Math.max(0, spent - 1);
      await env.BWM_TELEGRAM_KV.put(budgetKey, String(spent), { expirationTtl: WIRE_BUDGET_TTL_SECONDS });
      await updateOutboundLog(env, outboundId, {
        status: "failed", error: result.error ?? "telegram_send_failed", telegramResponse: result.response,
      });
      continue;
    }
    await updateOutboundLog(env, outboundId, {
      status: "sent", telegramMessageId: result.telegramMessageId, telegramResponse: result.response, error: null,
    });
    // Now live → it's a task Robert owes (same mirror as a live dispatch).
    // Lookup-first: if a PRIOR redelivery emitted the mirror and then lost
    // its KV update, emitting again would orphan a duplicate task (codex r3).
    let taskEventId = (entry["task_event_id"] as string | null | undefined) ?? null;
    if (!taskEventId) {
      const refSince = new Date(Date.now() - 14 * 24 * 60 * 60 * 1000).toISOString();
      const prior = await fetchRows<{ id: string }>(
        env,
        `operational_events?select=id&event_type=eq.task.queued` +
        `&payload->>wire_ref=eq.${encodeURIComponent(entry.ref)}` +
        `&occurred_at=gte.${encodeURIComponent(refSince)}&order=occurred_at.desc&limit=1`,
      );
      taskEventId = prior?.[0]?.id ?? null;
    }
    if (!taskEventId) taskEventId = await emitWireTaskQueued(env, input, entry.ref, true);
    const { ref: _ref, deferred: _deferred, ...regRest } = entry;
    const updatedEntry = JSON.stringify({
      ...regRest,
      message_id: result.telegramMessageId ?? null,
      outbound_id: outboundId,
      task_event_id: taskEventId,
      redelivered_at: new Date().toISOString(),
    });
    // This write is what stops tomorrow's run from re-sending — retry once
    // on failure to shrink the duplicate-ping window (codex r3).
    try {
      await env.BWM_TELEGRAM_KV.put(`${KV_WIRE_OPEN_PREFIX}${entry.ref}`, updatedEntry, { expirationTtl: WIRE_OPEN_TTL_SECONDS });
    } catch (e) {
      console.error(JSON.stringify({ where: "redeliverDeferred.registryUpdate", error: String(e), ref: entry.ref }));
      try {
        await env.BWM_TELEGRAM_KV.put(`${KV_WIRE_OPEN_PREFIX}${entry.ref}`, updatedEntry, { expirationTtl: WIRE_OPEN_TTL_SECONDS });
      } catch (e2) {
        console.error(JSON.stringify({ where: "redeliverDeferred.registryUpdateRetry", error: String(e2), ref: entry.ref }));
      }
    }
    redelivered += 1;
  }
  return redelivered;
}

/** Close command_tasks mirrors whose wire:open registry entry expired
 *  unanswered (7-day TTL): the ref is no longer answerable from Telegram and
 *  the plan query excludes one-wire rows, so without this the obligation
 *  vanishes from every surface while staying queued forever (codex r3).
 *  Expiring mirrors match the registry lifecycle Phase 1 locked in. Runs
 *  daily from the Day Ahead composer. */
async function expireOrphanedWireMirrors(env: Env): Promise<void> {
  try {
    const rows = await fetchRows<{ source_event_id: string | null; title: string | null; created_at: string }>(
      env,
      `command_tasks?select=source_event_id,title,created_at&created_by=eq.one-wire&status=eq.queued&order=created_at.asc&limit=50`,
    );
    if (!rows || rows.length === 0) return;
    const openList = await env.BWM_TELEGRAM_KV.list({ prefix: KV_WIRE_OPEN_PREFIX, limit: 100 });
    const openRefs = new Set(openList.keys.map((k) => k.name.slice(KV_WIRE_OPEN_PREFIX.length)));
    const cutoffMs = Date.now() - WIRE_OPEN_TTL_SECONDS * 1000;
    for (const row of rows) {
      const m = /^Answer ([FCS]-[A-Z0-9]{3,6}) /.exec(row.title ?? "");
      if (!m || openRefs.has(m[1])) continue;
      // Younger-than-TTL rows with no registry entry were ANSWERED (decision
      // path deletes the entry; task.resolved rides it or the retry sweep) —
      // only a full TTL age proves expiry.
      const created = Date.parse(row.created_at);
      if (!Number.isFinite(created) || created > cutoffMs) continue;
      if (!row.source_event_id) continue;
      // outcome=cancelled, not done: the decision was never answered — done
      // would count it as finished work in outcome analytics (codex r4). The
      // fanout maps any non-blocked outcome to status=done, which still
      // closes the board row.
      await emitOperationalEvent(env, "task.resolved", {
        task_id: row.source_event_id,
        outcome: "cancelled",
        resolution: `expired unanswered — wire registry TTL (7d); ${m[1]} is no longer answerable by ref`,
        source: "bwm-telegram-relay wire-mirror-expiry",
      }, "daemon:bwm-telegram-relay");
    }
  } catch (e) {
    console.error(JSON.stringify({ where: "expireOrphanedWireMirrors", error: String(e) }));
  }
}

/** Compose + send the Day Ahead morning digest (09:00 ET): today's calendar,
 *  inbox needs-you, the command queue, open
 *  decisions, and Sarah's overnight log — absorbed from the same substrate
 *  tables her standalone 07:00 Telegram brief used to render (ea_threads /
 *  ea_escalations / ea_drafts / ea_calendar_events; the full brief text stays
 *  in ea_briefs + the Board). Every section renders "(data unavailable)" on a
 *  source failure, never a false zero. */
async function composeAndSendDayAhead(env: Env, trigger: string): Promise<WireResult & { redelivered?: number; items?: number }> {
  const chatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!chatId) return { ok: false, action: "skipped_no_chat_id" };
  await retryPendingTaskResolves(env);
  await expireOrphanedWireMirrors(env);
  const et = etNow();

  // Deferred decisions stay inside the scheduled digest. Sending them live
  // seconds before the digest creates a duplicate notification burst.
  const redelivered = WIRE_DIGEST_LIVE_REDELIVERY
    ? await redeliverDeferredDecisions(env, chatId)
    : 0;

  const { startIso, endIso } = etDayRangeUtc();
  const since24h = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString();
  const since7d = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();

  // Lower bound widened 24h: the calendar writer stores all-day events at
  // 00:00:00Z, which is BEFORE ET midnight in UTC — a [startIso, endIso)
  // query would drop every all-day event (codex r2). Timed rows re-check the
  // exact window in JS below; all-day rows match by ET date.
  const calWideStart = new Date(new Date(startIso).getTime() - 24 * 60 * 60 * 1000).toISOString();
  // order=desc: the window is widened a day backwards, so ascending order
  // could exhaust the row cap on yesterday's rows before today's (codex r8);
  // latest-first fills from today's side. Re-sorted ascending after filter.
  const calRaw = await fetchRows<{ summary: string | null; start_at: string | null; all_day: boolean | null; status: string | null }>(
    env,
    `ea_calendar_events?select=summary,start_at,all_day,status` +
    `&start_at=gte.${encodeURIComponent(calWideStart)}&start_at=lt.${encodeURIComponent(endIso)}&order=start_at.desc&limit=40`,
  );
  // Numeric compares: PostgREST may format offsets as +00:00 while startIso
  // uses .000Z — a string compare drops boundary events (codex r3 P3).
  const startMs = Date.parse(startIso);
  const endMs = Date.parse(endIso);
  const cal = calRaw === null ? null : calRaw.filter((r) => {
    if (!r.start_at) return false;
    if (r.all_day) return r.start_at.slice(0, 10) === et.date;
    const t = Date.parse(r.start_at);
    return Number.isFinite(t) && t >= startMs && t < endMs;
  }).sort((a, b) => String(a.start_at).localeCompare(String(b.start_at)));
  const needsThreads = await fetchRows<{ sender_email: string | null; subject: string | null; action_taken: string | null }>(
    env,
    `ea_threads?select=sender_email,subject,action_taken&action_taken=in.(escalate,halt)` +
    `&classified_at=gte.${encodeURIComponent(since24h)}&order=classified_at.desc&limit=8`,
  );
  const escalations = await fetchRows<{ sender_email: string | null; sarah_reason: string | null; scope: string | null }>(
    env,
    `ea_escalations?select=sender_email,sarah_reason,scope&status=eq.open&order=opened_at.desc&limit=8`,
  );
  // ea_drafts is NOT queried (plan v6 §2.8, layer 2). Send-as-Robert auto-drafting is
  // a capability Robert disabled: "I told you I don't want any more motherfucking
  // automatic replies." Counting the drafts it still produces and putting that number
  // on his phone is the prohibition failing at the one door that is supposed to
  // enforce it. Re-enabling is a reviewed PR that restores this query, not a flag.
  // Overdue waiting-on rows (Sarah's "you owe a reply" tracker) — part of her
  // retired morning brief's needs-you surface; without it an owed reply that
  // is neither a recent escalation nor a handoff would vanish from Telegram
  // (codex r4 P1). Same filter as ea/waiting_on.fetch_overdue().
  const overdue = await fetchRows<{ sender_email: string | null; subject: string | null; direction: string | null; due_at: string | null }>(
    env,
    `ea_waiting_on?select=sender_email,subject,direction,due_at&resolved_at=is.null` +
    `&due_at=lt.${encodeURIComponent(new Date().toISOString())}&order=due_at.asc&limit=8`,
  );
  // one-wire mirrors are excluded IN the query — a JS filter after limit=50
  // could starve the plan once enough mirrors accumulate (codex r2). The
  // or-clause keeps NULL created_by rows (neq alone drops SQL NULLs).
  const plan = await fetchRows<{ title: string | null; priority: string | null }>(
    env,
    `command_tasks?select=title,priority&assignee=eq.robert&status=eq.queued` +
    `&or=(created_by.is.null,created_by.neq.one-wire)` +
    `&order=priority.asc,created_at.asc&limit=50`,
  );
  const overnight = await fetchRows<{ action_taken: string | null }>(
    env,
    `ea_threads?select=action_taken&classified_at=gte.${encodeURIComponent(since24h)}&limit=1000`,
  );

  // ── Build each section into its own array, then assemble by PRIORITY with
  // a running length budget (codex r6 P1): decisions render first and always
  // in full; a lower-priority section that would blow the budget is replaced
  // by an explicit one-line omission — never silently truncated off the end.

  // Calendar section
  const calSection: string[] = [];
  if (cal === null) {
    calSection.push("<b>Calendar:</b> (data unavailable)");
  } else {
    const events = cal.filter((r) => (r.status ?? "").toLowerCase() !== "cancelled");
    if (events.length === 0) {
      calSection.push("<b>Calendar:</b> clear.");
    } else {
      // Bullets first, header only if any survived (adversarial review, P1).
      const calBullets: string[] = [];
      for (const ev of events.slice(0, 3)) {
        const when = ev.all_day ? "all day" : etTimeShort(ev.start_at);
        const summary = ev.summary ?? "(no title)";
        if (summary.length > 160) continue; // whole unit or nothing (§2.6)
        calBullets.push(`• ${escapeHtml(when)} — ${escapeHtml(summary)}`);
      }
      if (calBullets.length > 0) {
        calSection.push("<b>Calendar:</b>");
        calSection.push(...calBullets);
      } else {
        calSection.push("<b>Calendar:</b> nothing that renders cleanly this run.");
      }
    }
  }

  // Inbox needs-you section (Sarah's escalate/halt lanes + open Sarah→Bob
  // handoffs + overdue waiting-on rows). A HALT that went out live as a CALL
  // can also appear here via its ea_escalations row — accepted double-render:
  // halts are the rare highest-stakes lane and the two lines carry different
  // affordances (thread context here, answer-by-ref under Waiting on you).
  // "Clear" is claimed ONLY when ALL sources loaded and are empty (codex r2
  // P1; verify-before-claiming).
  const inboxSection: string[] = [];
  if (needsThreads === null && escalations === null && overdue === null) {
    inboxSection.push("<b>Inbox needs you:</b> (data unavailable)");
  } else {
    const inboxLines: string[] = [];
    // Whole unit or drop the line (§2.6) — no more 40/60-char slices that cut a
    // subject mid-word and leave Robert guessing what the message was about.
    const fits = (v: string, max: number) => v.length > 0 && v.length <= max;
    for (const t of needsThreads ?? []) {
      const verb = t.action_taken === "halt" ? "halted — needs you" : "escalated";
      const who = t.sender_email ?? "unknown"; const subj = t.subject ?? "(no subject)";
      if (!fits(who, 80) || !fits(subj, 160)) continue;
      inboxLines.push(`• ${escapeHtml(who)} — ${escapeHtml(subj)} <i>(${verb})</i>`);
    }
    for (const e of escalations ?? []) {
      const who = e.sender_email ?? "unknown"; const why = e.sarah_reason ?? e.scope ?? "handoff";
      if (!fits(who, 80) || !fits(why, 160)) continue;
      inboxLines.push(`• ${escapeHtml(who)} — ${escapeHtml(why)} <i>(with Bob)</i>`);
    }
    for (const w of overdue ?? []) {
      const label = w.direction === "owed_by_us" ? "you owe a reply" : "awaiting their reply";
      const since = String(w.due_at ?? "").slice(0, 10);
      const who = w.sender_email ?? "unknown"; const subj = w.subject ?? "(no subject)";
      if (!fits(who, 80) || !fits(subj, 160)) continue;
      inboxLines.push(`• ${escapeHtml(who)} — ${escapeHtml(subj)} <i>(${label}${since ? ` since ${escapeHtml(since)}` : ""})</i>`);
    }
    const failedSources = [needsThreads, escalations, overdue].filter((s) => s === null).length;
    const partial = failedSources > 0;
    if (inboxLines.length === 0) {
      inboxSection.push(partial
        ? `<b>Inbox needs you:</b> (${failedSources} source${failedSources === 1 ? "" : "s"} unavailable this run — nothing visible in the rest)`
        : "<b>Inbox needs you:</b> nothing — Sarah's lanes are clear.");
    } else {
      // NO COUNT, NO "+N more" (§2.3). A partial read is still reported honestly —
      // that is a data-availability fact, not a backlog tally.
      // inboxLines is non-empty here by the enclosing branch, so the header always has
      // a bullet under it.
      inboxSection.push("<b>Inbox needs you:</b>");
      inboxSection.push(...inboxLines.slice(0, 3));
      if (partial) inboxSection.push(`<i>(${failedSources} inbox source${failedSources === 1 ? "" : "s"} unavailable this run)</i>`);
    }
  }
  // No Drafts line: see the ea_drafts note above (§2.8 layer 2).

  // Plan section (command queue) — wire-mirror rows are excluded in the query
  // because the same asks render as refs under Waiting on you.
  const planSection: string[] = [];
  if (plan === null) {
    planSection.push("<b>Plan:</b> (data unavailable)");
  } else if (plan.length === 0) {
    planSection.push("<b>Plan:</b> queue is empty.");
  } else {
    // NO COUNT (§2.3): "Plan (50+ queued)" is the single line that most directly told
    // Robert he was fifty items behind. The priority TOKEN ("P2") is also gone —
    // internal severity labels are banned from Robert-visible text (rule 6).
    // Bullets first, header only if any survived (adversarial review, P1). Plan titles
    // are auto-generated as `Answer {ref} — {punchline}` and a punchline can run to the
    // 1000-char ingress cap, so "every candidate too long" is a realistic case here.
    const planBullets: string[] = [];
    for (const p of plan.slice(0, 3)) {
      const title = p.title ?? "(untitled)";
      if (title.length > 160) continue; // whole unit or nothing (§2.6)
      planBullets.push(`• ${escapeHtml(title)}`);
    }
    if (planBullets.length > 0) {
      planSection.push("<b>Plan:</b>");
      planSection.push(...planBullets);
    } else {
      planSection.push("<b>Plan:</b> nothing that renders cleanly this run.");
    }
  }

  // Overnight section (Sarah's log, data-derived from her real dispositions).
  // "Handled" counts only her AUTONOMOUS filings (archive/label/spam — the
  // set her retired brief called handled); escalate/halt are needs-you items
  // listed above, and counting them as handled would claim work Robert still
  // owes (codex r4).
  const overnightSection: string[] = [];
  if (overnight === null) {
    overnightSection.push("<b>Overnight:</b> (data unavailable)");
  } else {
    const HANDLED_ACTIONS = new Set(["archive", "label", "spam"]);
    const counts = new Map<string, number>();
    for (const r of overnight) {
      const a = r.action_taken ?? "";
      if (!HANDLED_ACTIONS.has(a)) continue;
      counts.set(a, (counts.get(a) ?? 0) + 1);
    }
    const total = [...counts.values()].reduce((a, b) => a + b, 0);
    // NO COUNT and no per-action breakdown (§2.3): "Sarah handled 12 (8 archive · 4
    // label)" is machine-room volume. What Robert needs is the binary — did anything
    // need him? — and the answer is already "no", or it would be listed above.
    overnightSection.push(total === 0
      ? "<b>Overnight:</b> quiet — nothing needed auto-handling."
      : "<b>Overnight:</b> handled without you — full brief on file.");
  }

  // ── Assemble: header → decisions (always full) → sections by priority ──
  const lines: string[] = [];
  lines.push(`🌅 <b>DAY AHEAD — ${escapeHtml(et.label)}</b>`);

  // Waiting on you — deferred items remain here instead of getting a separate
  // live redelivery immediately before the digest.
  // Highest priority: the decision list is the point of One Wire and is never
  // squeezed out by informational sections.
  const openItems = await listOpenWireItems(env);
  const waiting = waitingOnYouLines(openItems);
  lines.push(...waiting.lines);
  const SECTION_CEILING = 1800; // one human-scannable attention pass
  const pushSection = (section: string[], label: string) => {
    if (section.length === 0) return;
    const used = lines.reduce((n, l) => n + l.length + 1, 0);
    const add = section.reduce((n, l) => n + l.length + 1, 0);
    if (used + add > SECTION_CEILING) {
      lines.push(`<i>${label}: over length budget this run — detail on the Board.</i>`);
      return;
    }
    lines.push(...section);
  };
  pushSection(calSection, "Calendar");
  pushSection(inboxSection, "Inbox needs you");
  pushSection(planSection, "Plan");
  pushSection(overnightSection, "Overnight");

  // Overnight notes (fyis queued since Day Done) — flush-on-render, same
  // length-budget semantics as Day Done. Sarah-triage fyis STAY in the notes
  // flow even though escalate/halt threads also render under "Inbox needs
  // you": proxy-flushing them was tried and reverted (codex r2 P1) — the
  // origin also covers client-feedback fyis with no needs-you row, and a
  // dropped note is data loss while the overlap is one duplicate line in one
  // digest for the rare overnight escalation.
  const qItems = await listDigestNotes(env);
  const renderedNoteKeys = appendNotesSection(lines, qItems, "Overnight notes");

  if (BOARD_URL) lines.push(`<i>Detail: ${escapeHtml(BOARD_URL)}</i>`);
  // Same interim footer rule as Day Done (§2.4): bound to a ref on screen, or absent.
  lines.push(...replyFooter(waiting.renderedRefs));
  // Kill list, layer 3 (§2.8) — same guard as Day Done. This is the composer that
  // actually surfaced the inbound-email state, so the scrub matters most here.
  const scrubbed = scrubEmailAsRobert(fitDigest(lines));
  if (scrubbed.removed > 0) {
    console.error(JSON.stringify({
      where: "composeDayAhead", error: "email_as_robert_reached_final_render",
      removed: scrubbed.removed, detail: "an ingress path is unaccounted for",
    }));
  }
  const text = scrubbed.text;

  let botToken: string;
  try {
    botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
  } catch (e) {
    return { ok: false, action: "failed", error: `token_mint_failed: ${String(e).slice(0, 200)}`, redelivered };
  }
  const outboundId = await createOutboundLog(env, {
    sourceRoute: "/digest", chatId, parseMode: "HTML", text, status: "queued",
    metadata: {
      wire: {
        type: "digest", kind: "day-ahead", trigger,
        redelivered, notes: qItems.length, waiting: openItems.length,
        killed_at_final_render: scrubbed.removed,
      },
      ...(isCaptureMode(env) ? { capture: true } : {}),
    },
    requireSupabase: true,
  });
  const result = await sendTelegramMessage(botToken, chatId, text, "HTML");
  if (!result.ok) {
    await updateOutboundLog(env, outboundId, {
      status: "failed", error: result.error ?? "telegram_send_failed", telegramResponse: result.response,
    });
    return { ok: false, action: "failed", error: result.error, redelivered };
  }
  await updateOutboundLog(env, outboundId, {
    status: "sent", telegramMessageId: result.telegramMessageId, telegramResponse: result.response, error: null,
  });
  await env.BWM_TELEGRAM_KV.put(KV_LAST_SEND_AT_KEY, new Date().toISOString());
  // Flush ONLY the notes that made it into the delivered message (codex r6).
  await Promise.all(renderedNoteKeys.map((k) => env.BWM_TELEGRAM_KV.delete(k)));
  return { ok: true, action: "sent", redelivered, items: qItems.length };
}

async function handleDayAheadTrigger(request: Request, env: Env): Promise<Response> {
  const key = request.headers.get("X-BWM-Internal-Key") ?? "";
  if (!env.BWM_INTERNAL_KEY || key !== env.BWM_INTERNAL_KEY) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }
  const result = await composeAndSendDayAhead(env, "manual_flush");
  return json(result, result.ok ? 200 : 502);
}

// ── Comms scorecard (Friday Day Done + POST /scorecard/run) ─────────────────

interface CommsScorecard {
  window_days: number;
  sends_total: number;
  sends_per_day: number;
  live_interrupts_total: number;
  interrupts_per_day: number;
  digests_total: number;
  decisions_total: number;
  median_call_response_hours: number | null;
  unanswered_refs: number;
  computed_at: string;
}

/** 7-day comms telemetry over telegram_outbound.metadata.wire + wire-decision
 *  narratives. Returns null when EITHER substrate read fails — an honest
 *  "unavailable" beats a scorecard silently missing half its inputs. */
async function computeCommsScorecard(env: Env): Promise<CommsScorecard | null> {
  const windowDays = 7;
  const since = new Date(Date.now() - windowDays * 24 * 60 * 60 * 1000).toISOString();
  // 1000-row cap ≈ 4× the pre-One-Wire worst week; if volume ever exceeds it
  // the window silently shortens — revisit before that (log flags it).
  const outb = await fetchRows<{ queued_at: string; status: string; metadata: Record<string, unknown> | null }>(
    env,
    `telegram_outbound?select=queued_at,status,metadata&queued_at=gte.${encodeURIComponent(since)}&order=queued_at.desc&limit=1000`,
  );
  if (outb === null) return null;
  if (outb.length === 1000) console.warn(JSON.stringify({ where: "computeCommsScorecard", warn: "row_cap_hit_window_truncated" }));
  const decisions = await fetchRows<{ payload: Record<string, unknown> | null; occurred_at: string }>(
    env,
    `operational_events?select=payload,occurred_at&event_type=eq.narrative` +
    `&payload->>kind=eq.wire-decision&occurred_at=gte.${encodeURIComponent(since)}&limit=500`,
  );
  if (decisions === null) return null;

  const wireOf = (r: { metadata: Record<string, unknown> | null }) =>
    (r.metadata ?? {})["wire"] as { type?: string; ref?: string; action?: string } | undefined;
  // A FIRE edit updates the existing message in place — no new message, no
  // ping. Excluded from send totals or an incident storm inflates sends/day
  // (codex r9).
  const sent = outb.filter((r) => r.status === "sent" && wireOf(r)?.action !== "edited");
  // Interrupt = anything that pinged live: typed fire/call/signoff PLUS
  // untyped legacy rows (/send freeform + /event pings carry no
  // metadata.wire but absolutely interrupted Robert — codex r9). Digests and
  // queued fyis are the anti-interrupt.
  const liveInterrupts = sent.filter((r) => {
    const w = wireOf(r);
    if (!w) return true; // legacy untyped live send
    return ["fire", "call", "signoff"].includes(w.type ?? "");
  });
  const digests = sent.filter((r) => wireOf(r)?.type === "digest");

  // Latency anchors to the first LIVE delivery (status=sent), not the first
  // log row: a quiet-hours deferral writes a queued row at e.g. 22:00 but
  // Robert first SEES the call at the 09:00 redelivery — counting the held
  // period would misreport the ≤4-waking-hours SLO by a full night (codex
  // r8).
  const firstQueuedByRef = new Map<string, number>();
  for (const r of outb) {
    if (r.status !== "sent") continue;
    const w = wireOf(r);
    if (!w?.ref) continue;
    const t = Date.parse(r.queued_at);
    if (!Number.isFinite(t)) continue;
    const prev = firstQueuedByRef.get(w.ref);
    if (prev === undefined || t < prev) firstQueuedByRef.set(w.ref, t);
  }
  const latencies: number[] = [];
  for (const d of decisions) {
    const p = d.payload ?? {};
    const ref = String(p["wire_ref"] ?? "");
    const wt = String(p["wire_type"] ?? "");
    // CALL only — the metric is named median_call_response and §12's success
    // bar is about CALL response; mixing signoff samples would misreport it
    // (codex r3).
    if (!ref || wt !== "call") continue;
    const q = firstQueuedByRef.get(ref);
    if (q === undefined) continue;
    const dt = Date.parse(d.occurred_at) - q;
    if (Number.isFinite(dt) && dt >= 0) latencies.push(dt);
  }
  latencies.sort((a, b) => a - b);
  // True median: average the two middle samples on even counts (codex Phase 2
  // — floor(n/2) alone reports the upper-middle value).
  const mid = Math.floor(latencies.length / 2);
  const medianMs = latencies.length === 0
    ? null
    : latencies.length % 2 === 1
      ? latencies[mid]
      : (latencies[mid - 1] + latencies[mid]) / 2;

  let unanswered = 0;
  try {
    // Cursor-following count via the shared lister — a single KV page caps
    // the metric at exactly 100 during a backlog (codex r9).
    unanswered = (await listOpenWireItems(env)).length;
  } catch (e) {
    // Same honesty bar as the Supabase reads: an unavailable substrate makes
    // the WHOLE scorecard unavailable — never a false-zero metric (codex
    // Phase 2).
    console.error(JSON.stringify({ where: "computeCommsScorecard.unanswered", error: String(e) }));
    return null;
  }

  return {
    window_days: windowDays,
    sends_total: sent.length,
    sends_per_day: sent.length / windowDays,
    live_interrupts_total: liveInterrupts.length,
    interrupts_per_day: liveInterrupts.length / windowDays,
    digests_total: digests.length,
    decisions_total: latencies.length,
    median_call_response_hours: medianMs === null ? null : medianMs / 3_600_000,
    unanswered_refs: unanswered,
    computed_at: new Date().toISOString(),
  };
}

function scorecardLines(sc: CommsScorecard): string[] {
  const med = sc.median_call_response_hours;
  return [
    `📊 <b>Comms scorecard (${sc.window_days}d):</b>`,
    `• ${sc.sends_per_day.toFixed(1)} sends/day · ${sc.interrupts_per_day.toFixed(1)} live interrupts/day (target ≤3)`,
    `• CALL response: ${med !== null ? `median ${med.toFixed(1)}h over ${sc.decisions_total}` : "no answered calls this week"} · ${sc.unanswered_refs} unanswered ref${sc.unanswered_refs === 1 ? "" : "s"}`,
  ];
}

async function emitCommsSlo(env: Env, sc: CommsScorecard): Promise<string | null> {
  const id = await emitOperationalEvent(env, "narrative", {
    kind: "comms-slo",
    source: "bwm-telegram-relay scorecard",
    body:
      `Comms SLO ${sc.window_days}d: ${sc.sends_per_day.toFixed(1)} sends/day, ` +
      `${sc.interrupts_per_day.toFixed(1)} live interrupts/day, ` +
      `${sc.median_call_response_hours !== null ? `median CALL response ${sc.median_call_response_hours.toFixed(1)}h (n=${sc.decisions_total})` : "no answered calls"}, ` +
      `${sc.unanswered_refs} unanswered refs.`,
    metrics: sc as unknown as Record<string, unknown>,
  }, "daemon:bwm-telegram-relay");
  if (!id) console.error(JSON.stringify({ where: "emitCommsSlo", warn: "comms_slo_emit_failed" }));
  return id;
}

async function handleScorecardRun(request: Request, env: Env): Promise<Response> {
  const key = request.headers.get("X-BWM-Internal-Key") ?? "";
  if (!env.BWM_INTERNAL_KEY || key !== env.BWM_INTERNAL_KEY) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }
  const sc = await computeCommsScorecard(env);
  if (!sc) return json({ ok: false, error: "substrate_unavailable" }, 502);
  const eventId = await emitCommsSlo(env, sc);
  return json({ ok: eventId !== null, event_id: eventId, scorecard: sc }, eventId !== null ? 200 : 502);
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
      if (method === "POST" && path === "/notify") {
        return handleNotify(request, env);
      }
      if (method === "POST" && path === "/digest/flush") {
        return handleDigestFlush(request, env);
      }
      if (method === "POST" && path === "/digest/day-ahead") {
        return handleDayAheadTrigger(request, env);
      }
      if (method === "POST" && path === "/scorecard/run") {
        return handleScorecardRun(request, env);
      }
      if (method === "POST" && path === "/test") {
        return handleTest(request, env);
      }
      if (method === "POST" && path === "/capture-chat-id") {
        return handleCaptureChatId(request, env);
      }
      // Legacy route preserved as a compatibility input, but hard-wrapped to
      // the FYI digest. It can no longer send a live Telegram message.
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
    event: ScheduledController,
    env: Env,
    ctx: ExecutionContext,
  ): Promise<void> {
    // One Wire Day Done digest — 20:30 UTC (16:30 EDT).
    if (event.cron === "30 20 * * *") {
      ctx.waitUntil(
        composeAndSendDayDone(env, "cron").catch((e) =>
          console.error("scheduled digest failed:", (e as Error)?.message ?? e),
        ),
      );
      return;
    }
    // One Wire Day Ahead digest — 13:00 UTC (09:00 EDT). Redelivers deferred
    // decisions first (Phase 2, Close the Loop).
    if (event.cron === "0 13 * * *") {
      ctx.waitUntil(
        composeAndSendDayAhead(env, "cron").catch((e) =>
          console.error("scheduled day-ahead failed:", (e as Error)?.message ?? e),
        ),
      );
      return;
    }
    // Emit daemon.heartbeat — counts pulled from KV (best-effort; 0 on cold start)
    ctx.waitUntil(
      emitHeartbeat(env, 0, 0).catch((e) =>
        console.error("scheduled heartbeat failed:", (e as Error)?.message ?? e),
      ),
    );
    // Fire collapse sweep (plan v6 §2.5), on the 15-min cron.
    ctx.waitUntil(
      sweepFireRegistry(env).catch((e) =>
        console.error("fire collapse sweep failed:", (e as Error)?.message ?? e),
      ),
    );
  },
} satisfies ExportedHandler<Env>;
