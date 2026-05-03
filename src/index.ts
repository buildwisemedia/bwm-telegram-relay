/**
 * bwm-telegram-relay — Send Telegram messages + receive webhook updates.
 *
 * Routes:
 *   GET  /health        → {ok:true, worker:'bwm-telegram-relay', version:'1.0.0'}
 *   POST /send          → X-BWM-Internal-Key auth; body {text, parse_mode?}
 *                         reads robert_chat_id from KV; mints TELEGRAM_BOT_TOKEN from broker
 *   POST /webhook       → validates X-Telegram-Bot-Api-Secret-Token == TELEGRAM_WEBHOOK_SECRET
 *                         parses Telegram update; stores chat_id on first contact; replies to /start
 *
 * Token flow: BROKER_BEARER → bwm-cred-broker /mint → TELEGRAM_BOT_TOKEN
 *
 * PROJ-COMMS-CHANNEL-MIGRATION-001
 */

export interface Env {
  /** KV namespace for persisting robert_chat_id */
  BWM_TELEGRAM_KV: KVNamespace;
  /** Service binding to bwm-cred-broker (avoids CF 1042 same-account subrequest block) */
  CRED_BROKER: Fetcher;
  /** Bearer token for authenticating to bwm-cred-broker */
  BROKER_BEARER: string;
  /** Shared key for /send route auth (X-BWM-Internal-Key header) */
  BWM_INTERNAL_KEY: string;
  /** Secret token Telegram sends in X-Telegram-Bot-Api-Secret-Token header */
  TELEGRAM_WEBHOOK_SECRET: string;
}

// Service binding uses a relative URL; the host is arbitrary when using Fetcher
const BROKER_INTERNAL_URL = "https://internal/mint";
const TELEGRAM_API_BASE = "https://api.telegram.org";
const KV_CHAT_ID_KEY = "robert_chat_id";

// --- Utility helpers -----------------------------------------------------------

const JSON_HEADERS = { "Content-Type": "application/json" };

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });
}

// --- Cred-broker mint ----------------------------------------------------------

interface MintResponse {
  secret: string;
  secret_name: string;
  ttl_seconds: number;
  expires_at: string;
  agent: string;
}

async function mintToken(env: Env, secretName: string): Promise<string> {
  const res = await env.CRED_BROKER.fetch(BROKER_INTERNAL_URL, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${env.BROKER_BEARER}`,
      "Content-Type": "application/json",
      "User-Agent": "bwm-telegram-relay/1.0.0",
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

// --- Telegram API helpers ------------------------------------------------------

async function sendTelegramMessage(
  botToken: string,
  chatId: string | number,
  text: string,
  parseMode?: string,
): Promise<{ ok: boolean; result?: unknown; error?: string }> {
  const body: Record<string, unknown> = { chat_id: chatId, text };
  if (parseMode) body.parse_mode = parseMode;

  const res = await fetch(`${TELEGRAM_API_BASE}/bot${botToken}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const data = (await res.json()) as { ok: boolean; result?: unknown; description?: string };

  if (!data.ok) {
    return { ok: false, error: data.description ?? `HTTP ${res.status}` };
  }
  return { ok: true, result: data.result };
}

// --- Route handlers ------------------------------------------------------------

async function handleHealth(): Promise<Response> {
  return json({
    ok: true,
    worker: "bwm-telegram-relay",
    version: "1.0.0",
  });
}

async function handleSend(request: Request, env: Env): Promise<Response> {
  // Auth check
  const key = request.headers.get("X-BWM-Internal-Key") ?? "";
  if (!env.BWM_INTERNAL_KEY || key !== env.BWM_INTERNAL_KEY) {
    return json({ ok: false, error: "unauthorized" }, 401);
  }

  // Parse body
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

  // Look up stored chat_id
  const chatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!chatId) {
    return json(
      {
        ok: false,
        error: "chat_id not captured yet — send /start to the bot to register",
      },
      400,
    );
  }

  // Mint bot token
  let botToken: string;
  try {
    botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
  } catch (e) {
    console.error(JSON.stringify({ where: "handleSend.mintToken", error: String(e) }));
    return json({ ok: false, error: "token_mint_failed", detail: String(e).slice(0, 200) }, 502);
  }

  // Send message
  const result = await sendTelegramMessage(botToken, chatId, text, body.parse_mode);

  if (!result.ok) {
    console.error(JSON.stringify({ where: "handleSend.sendMessage", error: result.error }));
    return json({ ok: false, error: "telegram_send_failed", detail: result.error }, 502);
  }

  return json({ ok: true, chat_id: chatId });
}

async function handleWebhook(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  // Validate webhook secret token
  const secretHeader = request.headers.get("X-Telegram-Bot-Api-Secret-Token") ?? "";
  if (!env.TELEGRAM_WEBHOOK_SECRET || secretHeader !== env.TELEGRAM_WEBHOOK_SECRET) {
    return json({ ok: false, error: "forbidden" }, 403);
  }

  // Parse update
  let update: TelegramUpdate;
  try {
    update = (await request.json()) as TelegramUpdate;
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }

  console.log(JSON.stringify({ where: "webhook", update_id: update.update_id }));

  const message = update.message;
  if (!message) {
    // Non-message update (callback query, etc.) — ack and move on
    return json({ ok: true });
  }

  const chatId = message.chat?.id;
  const fromId = message.from?.id;

  if (!chatId) {
    return json({ ok: true });
  }

  // Store chat_id on first contact (any message, not just /start)
  const existingChatId = await env.BWM_TELEGRAM_KV.get(KV_CHAT_ID_KEY);
  if (!existingChatId) {
    await env.BWM_TELEGRAM_KV.put(KV_CHAT_ID_KEY, String(chatId));
    console.log(
      JSON.stringify({
        where: "webhook",
        event: "chat_id_captured",
        chat_id: chatId,
        from_id: fromId,
      }),
    );
  }

  // Respond to /start command
  const text = message.text ?? "";
  if (text.startsWith("/start")) {
    // Mint token to reply
    let botToken: string | null = null;
    try {
      botToken = await mintToken(env, "TELEGRAM_BOT_TOKEN");
    } catch (e) {
      console.error(
        JSON.stringify({ where: "webhook./start.mintToken", error: String(e) }),
      );
    }

    if (botToken) {
      const replyText =
        "✅ Connected. BWM ops alerts will route to this chat.\n\n" +
        `chat_id: ${chatId}\n` +
        "Ready to receive.";

      ctx.waitUntil(
        sendTelegramMessage(botToken, chatId, replyText).catch((e) =>
          console.error(
            JSON.stringify({ where: "webhook./start.reply", error: String(e) }),
          ),
        ),
      );
    }
  }

  return json({ ok: true });
}

// --- Telegram update types (minimal) ------------------------------------------

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

// --- Worker export -------------------------------------------------------------

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname.replace(/\/+$/, "") || "/";

    if (request.method === "GET" && path === "/health") {
      return handleHealth();
    }

    if (request.method === "POST" && path === "/send") {
      return handleSend(request, env);
    }

    if (request.method === "POST" && path === "/webhook") {
      return handleWebhook(request, env, ctx);
    }

    return json({ ok: false, error: "not_found", path, method: request.method }, 404);
  },
} satisfies ExportedHandler<Env>;
