#!/usr/bin/env node
// capture-jul27-fixtures.mjs — One Wire remediation Phase 1 fixture capture.
//
//   node scripts/capture-jul27-fixtures.mjs           # capture from live Supabase
//   node scripts/capture-jul27-fixtures.mjs --verify   # read-only integrity check
//
// The manifest (test/fixtures/jul27/manifest.json) is the SINGLE source of truth for
// which fixtures are POSTable to /gate/dry-run. Renderer-only fixtures (the six
// strict-prefix controls, per-field deletion negatives, correlation cases) live
// OUTSIDE the manifest, in test/fixtures/renderer/.
//
// Every fixture records `provenance`: "live" fixtures carry the real
// telegram_outbound / operational_events row id they were captured from; "synthetic"
// fixtures are constructed controls and say so explicitly. Nothing is silently made up.
//
// Steps emit STEP:<name>:OK|FAIL for test/phase/verifier-selftest.sh.
import { readFileSync, writeFileSync, existsSync, readdirSync, mkdirSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..");
const DIR = join(ROOT, "test/fixtures/jul27");
const MANIFEST = join(DIR, "manifest.json");
const BEFORE_ARTIFACT = join(DIR, "before-artifact.txt");
/** The byte-exact Jul 27 Day Done digest Robert reviewed (message 1899). */
const ARTIFACT_ID = "01KYJMAMTNPMBGH9HA3V6AY9MD";

const VERIFY = process.argv.includes("--verify");
let failed = false;
const ok = (s, d) => console.log(`STEP:${s}:OK${d ? ` ${d}` : ""}`);
const fail = (s, d) => { console.log(`STEP:${s}:FAIL${d ? ` ${d}` : ""}`); failed = true; };

// ── Supabase credentials, resolved exactly as the real log-event CLI does ─────
function loadEnv() {
  const p = join(process.env.HOME, ".bwm_secrets/log-event.env");
  if (existsSync(p)) {
    for (const line of readFileSync(p, "utf8").split("\n")) {
      const m = /^\s*(?:export\s+)?([A-Z_][A-Z0-9_]*)=(.*)$/.exec(line);
      if (m) process.env[m[1]] ??= m[2].replace(/^["']|["']$/g, "");
    }
  }
  const key = process.env.SUPABASE_SECRET_KEY || process.env.SUPABASE_SERVICE_ROLE_KEY || "";
  return { url: process.env.SUPABASE_URL || "", key };
}

async function sb(path) {
  const { url, key } = loadEnv();
  if (!url || !key) throw new Error("no supabase credentials");
  const r = await fetch(`${url.replace(/\/+$/, "")}/rest/v1/${path}`, {
    headers: { apikey: key, Authorization: `Bearer ${key}` },
  });
  if (!r.ok) throw new Error(`supabase ${r.status} on ${path}`);
  return r.json();
}

const manifest = JSON.parse(readFileSync(MANIFEST, "utf8"));

// ── VERIFY ───────────────────────────────────────────────────────────────────
if (VERIFY) {
  if (manifest.length !== 24) fail("manifest-length", `expected 24, got ${manifest.length}`);
  else ok("manifest-length", "24");

  if (new Set(manifest).size !== manifest.length) fail("manifest-unique", "duplicate ids");
  else ok("manifest-unique");

  const missing = manifest.filter((id) => !existsSync(join(DIR, `${id}.json`)));
  if (missing.length) fail("manifest-files", `missing fixture files: ${missing.join(",")}`);
  else ok("manifest-files", `${manifest.length} present`);

  // Boundary rule: no fixture file in the manifest dir that the manifest omits.
  const stray = readdirSync(DIR)
    .filter((f) => /^f.*\.json$/.test(f))
    .map((f) => f.replace(/\.json$/, ""))
    .filter((id) => !manifest.includes(id));
  if (stray.length) fail("manifest-boundary", `unlisted fixture files: ${stray.join(",")}`);
  else ok("manifest-boundary");

  // Every fixture declares honest provenance.
  const badProv = [];
  for (const id of manifest) {
    const p = join(DIR, `${id}.json`);
    if (!existsSync(p)) continue;
    let fx;
    try { fx = JSON.parse(readFileSync(p, "utf8")); } catch { badProv.push(`${id}:unparseable`); continue; }
    if (!fx.provenance || !["live", "synthetic"].includes(fx.provenance.kind)) badProv.push(`${id}:no-provenance`);
    else if (fx.provenance.kind === "live" && !fx.provenance.source_row_id) badProv.push(`${id}:live-without-row-id`);
  }
  if (badProv.length) fail("fixture-provenance", badProv.join(","));
  else ok("fixture-provenance");

  // Byte-fixture: freshly fetch the reviewed artifact and cmp against the committed copy.
  if (!existsSync(BEFORE_ARTIFACT)) {
    fail("before-artifact", `missing ${BEFORE_ARTIFACT}`);
  } else {
    try {
      const rows = await sb(`telegram_outbound?id=eq.${ARTIFACT_ID}&select=text_redacted`);
      if (!rows.length) {
        fail("before-artifact", `row ${ARTIFACT_ID} not found live`);
      } else {
        writeFileSync("/tmp/live-before.txt", rows[0].text_redacted ?? "");
        try {
          execFileSync("cmp", ["-s", BEFORE_ARTIFACT, "/tmp/live-before.txt"]);
          ok("before-artifact", "byte-identical to live row");
        } catch {
          fail("before-artifact", "committed fixture differs from the live row");
        }
      }
    } catch (e) {
      fail("before-artifact", String(e.message));
    }
  }
  process.exit(failed ? 1 : 0);
}

// ── CAPTURE ──────────────────────────────────────────────────────────────────
mkdirSync(DIR, { recursive: true });

const artifact = await sb(`telegram_outbound?id=eq.${ARTIFACT_ID}&select=text_redacted`);
if (!artifact.length) { fail("before-artifact", `row ${ARTIFACT_ID} not found`); process.exit(1); }
writeFileSync(BEFORE_ARTIFACT, artifact[0].text_redacted ?? "");
ok("before-artifact", `${(artifact[0].text_redacted ?? "").length} bytes`);

/** Pull a real telegram_outbound row and shape it into a wire-input fixture. */
async function liveWire(id, note) {
  const rows = await sb(
    `telegram_outbound?id=eq.${id}&select=id,created_at,status,source_route,metadata,text_redacted`,
  );
  if (!rows.length) throw new Error(`row ${id} not found`);
  const r = rows[0];
  return {
    provenance: { kind: "live", source_table: "telegram_outbound", source_row_id: r.id, captured_at: new Date().toISOString(), note },
    observed: { created_at: r.created_at, status: r.status, source_route: r.source_route, metadata: r.metadata, text_redacted: r.text_redacted },
  };
}

/** Pull a real operational_events row (the recap negative controls). */
async function liveEvent(id, note) {
  const rows = await sb(`operational_events?id=eq.${id}&select=id,event_type,payload,client_id,occurred_at`);
  if (!rows.length) throw new Error(`row ${id} not found`);
  const r = rows[0];
  return {
    provenance: { kind: "live", source_table: "operational_events", source_row_id: r.id, captured_at: new Date().toISOString(), note },
    observed: { event_type: r.event_type, payload: r.payload, client_id: r.client_id, occurred_at: r.occurred_at },
  };
}

const synth = (note, body) => ({
  provenance: { kind: "synthetic", captured_at: new Date().toISOString(), note },
  ...body,
});

const write = (id, obj) => writeFileSync(join(DIR, `${id}.json`), `${JSON.stringify(obj, null, 2)}\n`);

// Real Jul 27 / reviewed-surface rows, resolved by id.
const LIVE = {
  f01: ["01KYHYED33PH59XP4VK2XP4357", "LaunchAgent health flap — the fire Robert saw twice (random key suffix)"],
  f02: ["01KYHT0925Y9GP0EJ7S2A79BEV", "Google Ads fire, coalesced x4, no automation-state field"],
  f03: ["01KY29J7YT0A4A40ZJQ4JX514G", "Social publisher signoff — team-owned nudge routed to Robert"],
  f04: ["01KY2AVM8H19NZ5JABEMNBMECH", "D2S approval-buttons call — reversible internal merge routed to Robert"],
  f05: ["01KY324PHZECQ354C89B62FKDY", "Email-as-Robert signoff — a capability Robert disabled"],
  f14: ["01KYHTJRMXCE09EVRPHBNGHVC7", "Day Ahead of Jul 27 — inbound-email state surface"],
};
const LIVE_EVENTS = {
  f16a: ["01KYHFJWAVCX74PDFAM9JMB5K3", "Shape A build.shipped, delivery_state=pr_created, merged=false → renders NOTHING"],
  f16b: ["01KYHVVXX82W3J52W4QP28NGWF", "Shape B build.shipped, 'Review preview only - NOT live' in prose → renders NOTHING"],
  f16c: ["01KYJ6FYX3DK59ECD1CKRQCGQV", "Shape B build.shipped, 'REVIEW PREVIEW ONLY' in prose → renders NOTHING"],
};

for (const [id, [rowId, note]] of Object.entries(LIVE)) {
  try { write(id, await liveWire(rowId, note)); ok(`fixture-${id}`, rowId); }
  catch (e) { fail(`fixture-${id}`, String(e.message)); }
}
for (const [id, [rowId, note]] of Object.entries(LIVE_EVENTS)) {
  try { write(id, await liveEvent(rowId, note)); ok(`fixture-${id}`, rowId); }
  catch (e) { fail(`fixture-${id}`, String(e.message)); }
}

// Digest-queue notes Robert saw quoted on the Jul 27 Day Done. Pinned by EXPLICIT
// row id, not by a LIKE search: the digest itself quotes every note, so a search
// resolves to the Day Done row (the symptom) instead of the note that produced it.
// The digest-queue TTL is 3 days, which is why some of these were authored Jul 23–24
// and still surfaced on the Jul 27 digest.
const LIVE_NOTES = {
  f06: ["01KYB2APRA3MX16AYJT2V564M3", "ASAP threshold fyi — a team-owned SLA note rendered as Robert's work"],
  f07: ["01KYEA1F0A6ZGYXX50DE0RHM2W", "P1 bob-orchestrator bob-dry-tank — raw internal jargon"],
  f08: ["01KYDVQ506S96V5FPJ17PT3V67", "Bob incident.opened done-check — raw event name leaked"],
  f09: ["01KYGAGKGTYN6J83T2FPVKQDRR", "clipped note — the source unit the digest cut mid-word"],
};
for (const [id, [rowId, note]] of Object.entries(LIVE_NOTES)) {
  try { write(id, await liveWire(rowId, note)); ok(`fixture-${id}`, rowId); }
  catch (e) { fail(`fixture-${id}`, String(e.message)); }
}

// KV snapshots — captured from the live wire registries via the internal audit route
// when reachable; otherwise recorded as an explicit empty snapshot rather than invented.
for (const [id, prefix, note] of [
  ["f10", "wire:open:", "wire:open registry snapshot — the 'Waiting on you' backlog"],
  ["f11", "wire:digestq:", "wire:digestq registry snapshot — the queued Notes backlog"],
]) {
  let entries = [];
  let source = "unavailable";
  try {
    const out = execFileSync("npx", ["--no-install", "wrangler", "kv", "key", "list",
      "--namespace-id", "ab6c7f536e92480d877341b28e551d15", "--prefix", prefix, "--remote"],
      { cwd: ROOT, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"], timeout: 120000 });
    entries = JSON.parse(out).map((k) => k.name);
    source = "wrangler kv key list --remote";
  } catch { /* recorded as unavailable below — never fabricated */ }
  write(id, synth(note, { kv_prefix: prefix, snapshot_source: source, keys: entries }));
  ok(`fixture-${id}`, `${entries.length} keys (${source})`);
}

// Synthetic controls — constructed, and labelled as such.
write("f12", synth("eligible batched call — the POSITIVE control: a real Robert-only decision", {
  wire_input: {
    type: "call", punchline: "Cathryn wants 10 client slots at the Pilot price — approve the block booking or hold the line at list.",
    stakes: "Ten Pilot installs at once against the Phase-1 cap of 30 clients through May 2027.",
    rec: "Hold list price; offer the block as sequential starts instead of a discount.",
    options: ["Hold list price", "Approve the block at Pilot price"],
    origin: "bwm-revenue-desk", authority: "pricing_or_offer", expires_at: "2026-07-27",
  },
  expected: { verdict: "allow", lane: "batched" },
}));
write("f13", synth("spoof / replay / double-use triplet against the conversational lane", {
  cases: [
    { name: "spoof", wire_input: { type: "call", punchline: "spoofed reply", origin: "not-telegram-responder", reply_to_inbound: "01SPOOF00000000000000000000" }, expected: { verdict: "reject", reason: "unregistered_sender" } },
    { name: "replay", wire_input: { type: "call", punchline: "replayed reply", origin: "telegram-responder", reply_to_inbound: "01REPLAY0000000000000000000" }, expected: { verdict: "reject", reason: "consume_returned_zero_rows" } },
    { name: "double_use", wire_input: { type: "call", punchline: "double-used reply", origin: "telegram-responder", reply_to_inbound: "01DOUBLE0000000000000000000" }, expected: { verdict: "exactly_one_winner" } },
  ],
}));
write("f15", synth("synthetic eligible LIVE fire — the send/edit control for flap-replay", {
  wire_input: { type: "fire", punchline: "Design2Sell booking form is rejecting every submission — no lead can reach the team.", stakes: "Every inbound lead is being lost while this is open.", key: "fixture:f15:d2s-booking-form-down", origin: "fixture-harness" },
  expected: { first_dispatch: "sent", second_dispatch: "edited", distinct_telegram_message_ids: 1 },
}));
write("f17", synth("recap-status negative set — merely-queued/threshold/assignment work renders NOTHING", {
  cases: [
    { event_type: "task.queued", payload: { assignee: "team", title: "Rotate the broker bearer" }, expected: { renders: false } },
    { event_type: "narrative", payload: { kind: "threshold-crossed", body: "ASAP hit the 2-hour follow-up mark" }, expected: { renders: false } },
    { event_type: "build.shipped", payload: { delivery_state: "pr_created", summary: "PR opened" }, expected: { renders: false } },
    { event_type: "client_state.transition", payload: { to_stage: "closing", reason: "contract sent" }, expected: { renders: false, why: "source REMOVED from the closed recap set (plan v6 §2.2)" } },
  ],
}));
for (const [id, authority, punchline] of [
  ["f18a", "client_copy_approval", "Design2Sell's new homepage copy needs your approval before it goes live Friday."],
  ["f18b", "contract_terms", "Hope Sky wants the 90-day initial period cut to 30 — that is a contract-terms change."],
  ["f18c", "refund", "Townsend is asking for a partial refund on last month's install."],
  ["f18d", "pricing_or_offer", "Cathryn's referral wants Ascend at the Pilot price — a standing-price decision."],
  ["f18e", "money_commit", "The ad account will overspend the matched commitment by Friday unless you approve the raise."],
]) {
  write(id, synth(`Tier-C control (${authority}) — expiring, MUST batch, MUST never ring the phone`, {
    wire_input: { type: "call", punchline, authority, origin: "fixture-harness", expires_at: "2026-07-27" },
    expected: { live_sends: 0, in_decision_digest: true, approval_task_status: "queued" },
  }));
}
["f12", "f13", "f15", "f17", "f18a", "f18b", "f18c", "f18d", "f18e"].forEach((id) => ok(`fixture-${id}`, "synthetic"));

const present = manifest.filter((id) => existsSync(join(DIR, `${id}.json`)));
console.log(`capture-jul27-fixtures: ${present.length}/${manifest.length} fixtures on disk`);
process.exit(failed ? 1 : 0);
