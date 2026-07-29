#!/usr/bin/env node
// reconcile-fire-registry.mjs — one-time migration of pre-fingerprint wire:fire:* entries.
//
//   node scripts/reconcile-fire-registry.mjs --dry-run            # report only
//   node scripts/reconcile-fire-registry.mjs --apply              # rewrite
//   node scripts/reconcile-fire-registry.mjs --verify             # assert the end state
//   ... --namespace <id>                                          # default: production KV
//
// WHY (Sol QA r1, P0-1). Entries written before the fingerprint landed are stored under
// shortHash(key ?? punchline) and carry a 24h TTL. Two consequences, both re-pings:
//   · the dispatch lookup computes shortHash("ntf|<origin>|<key>") and never finds them,
//     so the next recurrence of a still-open incident SENDS A NEW MESSAGE;
//   · when the TTL expires the entry vanishes entirely, with the same result.
//
// This moves each entry to the identity the current code computes and strips the TTL, so
// the "no re-pings" promise holds across the deploy boundary rather than starting from it.
//
// It never invents identity: the new slot is derived from the entry's OWN stored
// base.key + base.origin, through the same fireFingerprint the worker uses. An entry with
// no stored key cannot be reconciled and is reported, not guessed.
import { execFileSync } from "node:child_process";
import { fireFingerprint } from "../src/index.ts";

const args = process.argv.slice(2);
const MODE = args.includes("--apply") ? "apply" : args.includes("--verify") ? "verify" : "dry-run";
const NS = args.includes("--namespace") ? args[args.indexOf("--namespace") + 1] : "ab6c7f536e92480d877341b28e551d15";
const PREFIX = "wire:fire:";

const ok = (s, d) => console.log(`STEP:${s}:OK${d ? ` ${d}` : ""}`);
const fail = (s, d) => { console.log(`STEP:${s}:FAIL${d ? ` ${d}` : ""}`); process.exit(1); };

const kv = (...a) => execFileSync("npx", ["--no-install", "wrangler", "kv", ...a, "--namespace-id", NS, "--remote"],
  { cwd: new URL("..", import.meta.url).pathname, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 120000 });

let keys;
try {
  keys = JSON.parse(kv("key", "list", "--prefix", PREFIX)).map((k) => k.name);
} catch (e) {
  fail("reconcile-list", `cannot list ${PREFIX}: ${String(e.message).slice(0, 200)}`);
}
ok("reconcile-list", `${keys.length} registry entries`);

const rows = [];
for (const name of keys) {
  let raw;
  try { raw = kv("key", "get", name); } catch { rows.push({ name, error: "unreadable" }); continue; }
  let reg;
  try { reg = JSON.parse(raw); } catch { rows.push({ name, error: "unparseable" }); continue; }
  rows.push({ name, reg, raw });
}

const plan = [];
for (const r of rows) {
  if (r.error) continue;
  const reg = r.reg;
  if (reg.pending === true) continue;                       // transient claim, not an incident
  const base = reg.base ?? {};
  const current = r.name.slice(PREFIX.length);
  // Derive the target slot from the entry's OWN stored key + origin, through the same
  // function the worker uses. No inference, no prose.
  const target = base.key ? fireFingerprint({ key: base.key, origin: base.origin }) : null;
  plan.push({
    name: r.name,
    ref: reg.ref,
    current,
    target,
    hasFingerprint: typeof reg.fingerprint === "string" && reg.fingerprint.length > 0,
    needsMove: !!target && target !== current,
    needsStamp: !(typeof reg.fingerprint === "string" && reg.fingerprint.length > 0),
    reg,
    key: base.key ?? null,
    origin: base.origin ?? null,
  });
}

// ── VERIFY ───────────────────────────────────────────────────────────────────
if (MODE === "verify") {
  const problems = [];
  for (const p of plan) {
    if (!p.key) { problems.push(`${p.name}: no stored base.key — cannot carry identity`); continue; }
    if (p.needsMove) problems.push(`${p.name}: still at the legacy slot (target ${p.target})`);
    if (p.needsStamp) problems.push(`${p.name}: no fingerprint stamped — the deletion paths cannot clear it`);
  }
  // Every surviving entry must be TTL-free. `kv key list` reports `expiration` only for
  // keys that have one, so its ABSENCE is the assertion.
  let listed = [];
  try { listed = JSON.parse(kv("key", "list", "--prefix", PREFIX)); } catch { /* reported below */ }
  const withTtl = listed.filter((k) => k.expiration !== undefined && k.expiration !== null);
  for (const k of withTtl) problems.push(`${k.name}: still carries an expiration (${new Date(k.expiration * 1000).toISOString()}) — an unresolved incident must never age out`);

  if (problems.length) {
    for (const p of problems) console.log(`STEP:reconcile-verify:FAIL ${p}`);
    process.exit(1);
  }
  ok("reconcile-verify", `${plan.length} entries: all at deterministic slots, all stamped, none with a TTL`);
  process.exit(0);
}

// ── REPORT / APPLY ───────────────────────────────────────────────────────────
console.log(`\nmode: ${MODE}   namespace: ${NS}\n`);
for (const p of plan) {
  const action = !p.key ? "SKIP (no stored key)" : p.needsMove ? `MOVE -> ${p.target}` : p.needsStamp ? "STAMP in place" : "already reconciled";
  console.log(`  ${p.name}  ref=${p.ref}  origin=${p.origin}`);
  console.log(`     key=${JSON.stringify(p.key)}`);
  console.log(`     ${action}`);
}

if (MODE === "dry-run") {
  console.log(`\n${plan.filter((p) => p.needsMove || p.needsStamp).length} entr(ies) need reconciliation. Re-run with --apply.`);
  ok("reconcile-dry-run");
  process.exit(0);
}

// COLLISION = a genuine merge. Two legacy entries whose keys differed only by the
// volatile suffix resolve to ONE target — they were always the same incident, split by
// randomness. Last-write-wins would pick arbitrarily, so pick deliberately: the entry
// with the EARLIEST first_at is the original post, and that is the message future
// recurrences should edit. The later duplicates are removed.
const byTarget = new Map();
for (const p of plan) {
  if (!p.key || (!p.needsMove && !p.needsStamp)) continue;
  const cur = byTarget.get(p.target);
  if (!cur || String(p.reg.first_at ?? "") < String(cur.reg.first_at ?? "")) byTarget.set(p.target, p);
}
for (const [target, winner] of byTarget) {
  const losers = plan.filter((p) => p.target === target && p !== winner && (p.needsMove || p.needsStamp));
  if (losers.length) {
    console.log(`  MERGE ${target}: keeping ${winner.ref} (first_at ${winner.reg.first_at}), ` +
                `superseding ${losers.map((l) => `${l.ref}@${l.reg.first_at}`).join(", ")}`);
  }
}

let moved = 0, stamped = 0, skipped = 0, merged = 0;
for (const p of plan) {
  if (!p.key) { skipped += 1; continue; }
  if (!p.needsMove && !p.needsStamp) continue;
  const winner = byTarget.get(p.target);
  if (winner !== p) {
    // A superseded duplicate: drop its legacy slot, do not write the target.
    try { kv("key", "delete", p.name); merged += 1; } catch (e) { fail("reconcile-apply", `${p.name}: ${String(e.message).slice(0, 200)}`); }
    continue;
  }
  const next = { ...p.reg, fingerprint: p.target, reconciled_at: new Date().toISOString() };
  const targetName = `${PREFIX}${p.target}`;
  try {
    // Write the new slot FIRST. If this run dies between the two calls the incident is
    // discoverable at both slots — duplicated bookkeeping, never a lost identity.
    // No --expiration-ttl: unresolved entries are resolution-bound (plan v6 §2.5).
    execFileSync("npx", ["--no-install", "wrangler", "kv", "key", "put", targetName,
      JSON.stringify(next), "--namespace-id", NS, "--remote"],
      { cwd: new URL("..", import.meta.url).pathname, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"], timeout: 120000 });
    if (p.needsMove) {
      kv("key", "delete", p.name);
      moved += 1;
    } else {
      stamped += 1;
    }
  } catch (e) {
    fail("reconcile-apply", `${p.name}: ${String(e.message).slice(0, 200)}`);
  }
}
ok("reconcile-apply", `moved=${moved} stamped=${stamped} merged=${merged} skipped=${skipped}`);
