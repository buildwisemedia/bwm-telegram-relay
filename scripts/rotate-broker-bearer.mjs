#!/usr/bin/env node
import { createHash } from "node:crypto";
import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const args = new Set(process.argv.slice(2));
const getArg = (name, fallback = null) => {
  const prefix = `${name}=`;
  const found = process.argv.slice(2).find((arg) => arg.startsWith(prefix));
  return found ? found.slice(prefix.length) : fallback;
};

const agentName = getArg("--agent", "bwm-telegram-relay");
const shouldUpdateAgent = args.has("--update-agent-hash");
const shouldSetWorkerSecret = args.has("--set-worker-secret");
const shouldSmoke = args.has("--smoke");
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_KEY;

const rawToken = readFileSync(0, "utf8");
const token = rawToken.trim();

if (!token) {
  console.error("Read an empty token from stdin.");
  process.exit(2);
}

const tokenHash = createHash("sha256").update(token).digest("hex");
console.log(JSON.stringify({
  ok: true,
  agent: agentName,
  token_sha256_prefix: tokenHash.slice(0, 12),
  raw_length: rawToken.length,
  trimmed_length: token.length,
  trim_changed: rawToken !== token,
}));

async function supabase(path, init = {}) {
  if (!supabaseUrl || !supabaseKey) {
    throw new Error("SUPABASE_URL and SUPABASE_SERVICE_KEY are required for Supabase verification/update.");
  }
  const response = await fetch(`${supabaseUrl.replace(/\/+$/, "")}/rest/v1/${path}`, {
    ...init,
    headers: {
      apikey: supabaseKey,
      Authorization: `Bearer ${supabaseKey}`,
      "Content-Type": "application/json",
      ...(init.headers ?? {}),
    },
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`Supabase ${response.status}: ${text.slice(0, 500)}`);
  return text ? JSON.parse(text) : null;
}

if (supabaseUrl && supabaseKey) {
  const rows = await supabase(
    `cred_broker_agents?select=id,name,token_hash,scopes&name=eq.${encodeURIComponent(agentName)}&limit=1`,
  );
  const agent = rows[0];
  if (!agent) {
    throw new Error(`No cred_broker_agents row found for ${agentName}`);
  }

  console.log(JSON.stringify({
    ok: true,
    check: "broker_hash",
    agent_id: agent.id,
    db_hash_prefix: String(agent.token_hash ?? "").slice(0, 12),
    computed_hash_prefix: tokenHash.slice(0, 12),
    matches: agent.token_hash === tokenHash,
  }));

  if (agent.token_hash !== tokenHash && shouldUpdateAgent) {
    await supabase(`cred_broker_agents?id=eq.${encodeURIComponent(agent.id)}`, {
      method: "PATCH",
      headers: { Prefer: "return=minimal" },
      body: JSON.stringify({ token_hash: tokenHash, updated_at: new Date().toISOString() }),
    });
    console.log(JSON.stringify({ ok: true, action: "updated_agent_hash", agent_id: agent.id }));
  }
}

if (shouldSetWorkerSecret) {
  const result = spawnSync("npx", ["wrangler", "secret", "put", "BROKER_BEARER"], {
    input: `${token}\n`,
    stdio: ["pipe", "pipe", "pipe"],
    encoding: "utf8",
  });
  if (result.status !== 0) {
    console.error(result.stderr);
    process.exit(result.status ?? 1);
  }
  console.log(JSON.stringify({ ok: true, action: "updated_worker_secret", secret: "BROKER_BEARER" }));
}

if (shouldSmoke) {
  const here = dirname(fileURLToPath(import.meta.url));
  const smokePath = resolve(here, "telegram-smoke.mjs");
  const result = spawnSync("node", [smokePath], {
    stdio: "inherit",
    env: process.env,
  });
  if (result.status !== 0) process.exit(result.status ?? 1);
}
