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
}

// ─────────────────────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────────────────────

const VERSION = "2.0.0";
const BROKER_INTERNAL_URL = "https://internal/mint";
const TELEGRAM_API_BASE = "https://api.telegram.org";
const KV_CHAT_ID_KEY = "robert_chat_id";
const KV_BOOTSTRAP_CHAT_ID_KEY = "bootstrap_chat_id";
const KV_BOOTSTRAP_DONE_KEY = "bootstrap_done";
const KV_LAST_SEND_AT_KEY = "last_send_at";
const KV_DEDUP_PREFIX = "dedup:";
const DEDUP_TTL_SECONDS = 86_400; // 24 h

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

async function sendTelegramMessage(
  botToken: string,
  chatId: string | number,
  text: string,
  parseMode?: string,
): Promise<{ ok: boolean; error?: string }> {
  const body: Record<string, unknown> = { chat_id: chatId, text };
  if (parseMode) body.parse_mode = parseMode;

  const res = await fetch(`${TELEGRAM_API_BASE}/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const data = (await res.json()) as { ok: boolean; description?: string };
  if (!data.ok) {
    return { ok: false, error: data.description ?? `HTTP ${res.status}` };
  }
  return { ok: true };
}

// ─────────────────────────────────────────────────────────────────────────────
// Route: GET /health
// ─────────────────────────────────────────────────────────────────────────────

async function handleHealth(env: Env): Promise<Response> {
  const chatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  const lastSendAt = await env.BWM_TELEGRAM_KV.get(KV_LAST_SEND_AT_KEY);
  return json({
    status: "ok",
    version: VERSION,
    telegram_configured: !!chatId,
    last_send_at: lastSendAt ?? null,
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

  // Dedup check (24h TTL per event_id)
  if (eventId) {
    const dedupKey = `${KV_DEDUP_PREFIX}${eventId}`;
    const alreadySent = await env.BWM_TELEGRAM_KV.get(dedupKey);
    if (alreadySent) {
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
    return json({ ok: true, action: "skipped_no_chat_id", event_type: eventType });
  }

  // Mint token and send — fire-and-forget via waitUntil to not block response
  ctx.waitUntil(
    (async () => {
      let botToken: string;
      try {
        botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
      } catch (e) {
        console.error(JSON.stringify({ where: "handleEvent.mintToken", error: String(e) }));
        return;
      }

      const text = formatEventMessage(eventType, payload);
      const result = await sendTelegramMessage(botToken, chatId, text, "MarkdownV2");

      if (!result.ok) {
        // If MarkdownV2 formatting caused a parse error, retry as plain text
        console.warn(JSON.stringify({ where: "handleEvent.send", warn: "MarkdownV2 failed, retrying plain", error: result.error }));
        const plainResult = await sendTelegramMessage(botToken, chatId, `[${eventType}] ${JSON.stringify(payload).slice(0, 400)}`);
        if (!plainResult.ok) {
          console.error(JSON.stringify({ where: "handleEvent.send.plain", error: plainResult.error }));
          return;
        }
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

  let botToken: string;
  try {
    botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
  } catch (e) {
    return json({ ok: false, error: "token_mint_failed", detail: String(e).slice(0, 200) }, 502);
  }

  const result = await sendTelegramMessage(
    botToken,
    chatId,
    "✅ BWM Telegram Relay is live\n\nOps alerts will route here.",
  );

  if (!result.ok) {
    return json({ ok: false, error: "send_failed", detail: result.error }, 502);
  }

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
    return json({ ok: false, error: "chat_id not captured yet — send /start to the bot to register" }, 400);
  }

  let botToken: string;
  try {
    botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
  } catch (e) {
    console.error(JSON.stringify({ where: "handleSend.mintToken", error: String(e) }));
    return json({ ok: false, error: "token_mint_failed", detail: String(e).slice(0, 200) }, 502);
  }

  const result = await sendTelegramMessage(botToken, chatId, text, body.parse_mode);
  if (!result.ok) {
    console.error(JSON.stringify({ where: "handleSend.sendMessage", error: result.error }));
    return json({ ok: false, error: "telegram_send_failed", detail: result.error }, 502);
  }

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

  const message = update.message;
  if (!message) return json({ ok: true });

  const chatId = message.chat?.id;
  const fromId = message.from?.id;
  if (!chatId) return json({ ok: true });

  const inboundEventId = ulid();
  ctx.waitUntil(persistInboundMessage(env, inboundEventId, update));

  // Store chat_id on first contact
  const existingChatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!existingChatId) {
    await env.BWM_TELEGRAM_KV.put(KV_CHAT_ID_KEY, String(chatId));
    console.log(JSON.stringify({ where: "webhook", event: "chat_id_captured", chat_id: chatId, from_id: fromId }));
  }

  const text = message.text ?? "";

  // Forward non-command text to bwm-attention-router (non-blocking)
  if (text && !text.startsWith("/") && env.ATTENTION_ROUTER && env.ATTENTION_ROUTER_KEY) {
    ctx.waitUntil(
      env.ATTENTION_ROUTER.fetch("https://internal/classify", {
        method: "POST",
        headers: {
          "X-BWM-Internal-Key": env.ATTENTION_ROUTER_KEY,
          "Content-Type": "application/json",
          "User-Agent": `bwm-telegram-relay/${VERSION}`,
        },
        body: JSON.stringify({
          source: "telegram",
          raw_text: text,
          message_id: String(message.message_id),
          inbound_event_id: inboundEventId,
          user_id: String(fromId ?? chatId),
        }),
      })
        .then(async (r) => {
          if (!r.ok) {
            console.error(JSON.stringify({
              where: "webhook.attention_router",
              status: r.status,
              detail: (await r.text()).slice(0, 200),
            }));
          }
        })
        .catch((e) => console.error(JSON.stringify({ where: "webhook.attention_router", error: String(e) }))),
    );
  }

  // Forward non-command text to bwm-content-classifier (non-blocking, sibling of attention-router)
  // PROJ-TELEGRAM-MIGRATION-001 Phase 0 / Chip 7b. Tagged content routes to Brain inbox.
  if (text && !text.startsWith("/") && env.CONTENT_CLASSIFIER && env.CONTENT_CLASSIFIER_KEY) {
    ctx.waitUntil(
      env.CONTENT_CLASSIFIER.fetch("https://internal/classify", {
        method: "POST",
        headers: {
          "X-BWM-Internal-Key": env.CONTENT_CLASSIFIER_KEY,
          "Content-Type": "application/json",
          "User-Agent": `bwm-telegram-relay/${VERSION}`,
        },
        body: JSON.stringify({
          source: "telegram",
          raw_text: text,
          message_id: String(message.message_id),
          event_id: inboundEventId,
          user_id: String(fromId ?? chatId),
        }),
      })
        .then(async (r) => {
          if (!r.ok) {
            console.error(JSON.stringify({
              where: "webhook.content_classifier",
              status: r.status,
              detail: (await r.text()).slice(0, 200),
            }));
          }
        })
        .catch((e) => console.error(JSON.stringify({ where: "webhook.content_classifier", error: String(e) }))),
    );
  }

  // Respond to /start
  if (text.startsWith("/start")) {
    let botToken: string | null = null;
    try {
      botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
    } catch (e) {
      console.error(JSON.stringify({ where: "webhook./start.mintToken", error: String(e) }));
    }

    if (botToken) {
      ctx.waitUntil(
        sendTelegramMessage(botToken, chatId,
          `✅ Connected. BWM ops alerts will route to this chat.\n\nchat_id: ${chatId}\nReady to receive.`,
        ).catch((e) => console.error(JSON.stringify({ where: "webhook./start.reply", error: String(e) }))),
      );
    }
  }

  return json({ ok: true });
}

// ─────────────────────────────────────────────────────────────────────────────
// Inbound persistence
// ─────────────────────────────────────────────────────────────────────────────

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
}

interface TelegramMessage {
  message_id: number;
  from?: { id: number; first_name?: string; username?: string };
  chat: { id: number; type: string };
  text?: string;
  date: number;
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
