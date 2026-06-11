#!/usr/bin/env node
import { createHash } from "node:crypto";

const relayUrl = process.env.RELAY_URL ?? "https://bwm-telegram-relay.robert-ba0.workers.dev";
const internalKey = process.env.BWM_INTERNAL_KEY;
const webhookSecret = process.env.TELEGRAM_WEBHOOK_SECRET;
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_KEY;

for (const [name, value] of Object.entries({
  BWM_INTERNAL_KEY: internalKey,
  TELEGRAM_WEBHOOK_SECRET: webhookSecret,
  SUPABASE_URL: supabaseUrl,
  SUPABASE_SERVICE_KEY: supabaseKey,
})) {
  if (!value) {
    console.error(`Missing required env var: ${name}`);
    process.exit(2);
  }
}

const now = new Date().toISOString();
const sendText = `BWM Telegram audit smoke ${now}`;
const inboundText = `BWM Telegram inbound smoke ${now}`;
const inboundMessageId = Math.floor(Date.now() / 1000);

function sha256(text) {
  return createHash("sha256").update(text).digest("hex");
}

async function expectOk(label, response) {
  const body = await response.text();
  let parsed;
  try {
    parsed = JSON.parse(body);
  } catch {
    parsed = body;
  }
  if (!response.ok) {
    throw new Error(`${label} failed ${response.status}: ${body.slice(0, 500)}`);
  }
  return parsed;
}

async function supabaseGet(path) {
  const response = await fetch(`${supabaseUrl.replace(/\/+$/, "")}/rest/v1/${path}`, {
    headers: {
      apikey: supabaseKey,
      Authorization: `Bearer ${supabaseKey}`,
    },
  });
  return expectOk(`supabase ${path}`, response);
}

console.log(`Relay: ${relayUrl}`);

const sendResponse = await fetch(`${relayUrl}/send`, {
  method: "POST",
  headers: {
    "X-BWM-Internal-Key": internalKey,
    "Content-Type": "application/json",
  },
  body: JSON.stringify({ text: sendText }),
});
console.log("send:", await expectOk("/send", sendResponse));

const webhookResponse = await fetch(`${relayUrl}/webhook`, {
  method: "POST",
  headers: {
    "X-Telegram-Bot-Api-Secret-Token": webhookSecret,
    "Content-Type": "application/json",
  },
  body: JSON.stringify({
    update_id: inboundMessageId,
    message: {
      message_id: inboundMessageId,
      from: { id: 8536721859, first_name: "Smoke" },
      chat: { id: 8536721859, type: "private" },
      text: inboundText,
      date: Math.floor(Date.now() / 1000),
    },
  }),
});
console.log("webhook:", await expectOk("/webhook", webhookResponse));

await new Promise((resolve) => setTimeout(resolve, 3_000));

const outbound = await supabaseGet(
  `telegram_outbound?select=id,status,source_route,telegram_message_id,error,created_at&text_sha256=eq.${sha256(sendText)}&order=created_at.desc&limit=1`,
);
const inbound = await supabaseGet(
  `telegram_inbound?select=event_id,forward_status,forward_error,router_status,classifier_status,text,received_at&message_id=eq.${inboundMessageId}&order=received_at.desc&limit=1`,
);

console.log("outbound_readback:", outbound);
console.log("inbound_readback:", inbound);

if (outbound[0]?.status !== "sent") {
  throw new Error(`Expected outbound status sent, got ${outbound[0]?.status ?? "missing"}`);
}
if (inbound[0]?.forward_status !== "forwarded") {
  throw new Error(`Expected inbound status forwarded, got ${inbound[0]?.forward_status ?? "missing"}`);
}

console.log("ok");
