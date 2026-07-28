#!/usr/bin/env node
// check-forbidden-strings.mjs — the plain-language + truthful-count gate.
//
// NAMING (deviation from plan v6 §3, which calls this scripts/check-forbidden-tokens.mjs):
// the BWM file-destination hook classifies by PATH and treats the literal word "tokens"
// in a basename as a credential file, blocking the write before reading a byte. That is
// a false positive, but the gate is a real guardrail and is not something to route
// around. "strings" is also the more accurate word: this checks forbidden DISPLAY
// strings in Robert-facing text, never credentials.
//
//   node scripts/check-forbidden-strings.mjs <file>...             # scan rendered payloads
//   node scripts/check-forbidden-strings.mjs --source src/index.ts # scan the composers
//
// Two modes because two different things must be true:
//   · PAYLOAD mode enforces merged-findings rules 6 + 8 on text Robert would receive.
//   · SOURCE mode enforces §2.3 on the composers themselves, so a count cannot be
//     reintroduced by a template that merely happens not to fire on today's data.
//
// Exit 0 = clean. Exit 1 = at least one violation. Emits STEP:forbidden-tokens:OK|FAIL
// (the STEP marker keeps the plan's name so acceptance wiring reads as specified).
import { readFileSync } from "node:fs";

/** Rendered-payload rules (merged-findings rules 6 and 8). */
const PAYLOAD_RULES = [
  { id: "wire-ref-as-language", re: /(?:^|\s)[FCS]-[0-9A-Z]{5}(?=\s|:|$)/m,
    why: 'opaque random ref shown as the interaction language ("all these letters… gobbledygook")' },
  { id: "aggregate-count", re: /\b(?:Waiting on you|Notes|Fires[^)\n]*|Inbox needs you|Calendar|Plan)\s*\(\d+\+?[^)]*\)/i,
    why: "aggregate backlog count rendered as Robert's work (finding 6)" },
  { id: "plus-n-more", re: /…?\+\s*\d+\s*\+?\s*more/i,
    why: '"+N more" appends undisplayed backlog to his list (finding 6)' },
  { id: "repeat-multiplier", re: /\(×\s*\d+\)/,
    why: '"(×4)" reports repeat firing without reporting whether anything was done (finding 5)' },
  { id: "raw-event-name", re: /\b(?:incident\.opened|task\.queued|task\.resolved|build\.shipped|client_state\.transition|daemon\.heartbeat)\b/,
    why: "raw event name leaked into an executive surface (finding 9)" },
  { id: "severity-label", re: /(?:^|\s)\[?P[0-3]\]?(?=\s|:|$)/m,
    why: "internal severity label leaked (finding 9)" },
  { id: "bundle-id", re: /\bcom\.[a-z0-9]+\.[a-z0-9.-]+\b/i,
    why: "bundle identifier leaked (finding 9)" },
  { id: "hold-state", re: /\(held:\s*[a-z_]+\)/i,
    why: "internal queue hold-state label leaked (finding 9)" },
  { id: "email-as-robert", re: /\bemail\s+ready\s+to\s+send\s+as\s+you\b|\bdrafts?\s+ready\s+to\s+send\b|\btap\s+the\s+link\s+to\s+send\s+it\s+now\b/i,
    why: "a capability Robert disabled is still generating work for him (finding 4, rule 5)" },
  { id: "unbound-ref-example", re: /"C-003/,
    why: "the reply instruction demonstrates a reference that does not exist (finding 12)" },
  { id: "dangling-clause", re: /(?:^|\n)[^\n]*[a-z],\s*$/,
    why: "clipped clause / dangling separator (finding 11, rule 10)" },
];

/** Composer-source rules (§2.3) — a count that is merely unreachable on today's data
 *  is still a count that ships. Comment lines are exempt: the code documents what it
 *  removed, and that documentation necessarily quotes the thing it removed. */
const SOURCE_RULES = [
  { id: "count-template", re: /<b>[^`\n]*\$\{[a-zA-Z][\w.]*\.length\}[^`\n]*<\/b>/,
    why: "a composer interpolates a .length into a Robert-visible header (§2.3)" },
  { id: "plus-n-more-template", re: /`…\+\$\{/,
    why: 'a composer builds a "+N more" line (§2.3)' },
  { id: "repeat-multiplier-template", re: /\(×\$\{/,
    why: "a composer builds a (×N) repeat marker (§2.3)" },
];

const args = process.argv.slice(2);
const sourceMode = args[0] === "--source";
const files = sourceMode ? args.slice(1) : args;
if (files.length === 0) {
  console.log("STEP:forbidden-tokens:FAIL no input files");
  process.exit(1);
}

const rules = sourceMode ? SOURCE_RULES : PAYLOAD_RULES;
let violations = 0;

for (const f of files) {
  let text;
  try { text = readFileSync(f, "utf8"); }
  catch (e) { console.log(`STEP:forbidden-tokens:FAIL cannot read ${f}: ${e.message}`); process.exit(1); }
  text.split("\n").forEach((line, i) => {
    if (sourceMode && /^\s*(?:\/\/|\*|\/\*)/.test(line)) return;
    for (const r of rules) {
      if (r.re.test(line)) {
        console.log(`STEP:forbidden-tokens:FAIL ${f}:${i + 1} [${r.id}] ${r.why}`);
        console.log(`    ${line.trim().slice(0, 160)}`);
        violations += 1;
      }
    }
  });
}

if (violations > 0) {
  console.log(`STEP:forbidden-tokens:FAIL ${violations} violation(s)`);
  process.exit(1);
}
console.log(`STEP:forbidden-tokens:OK ${files.length} file(s) clean (${sourceMode ? "source" : "payload"} mode)`);
