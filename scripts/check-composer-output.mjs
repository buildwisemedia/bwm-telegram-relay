#!/usr/bin/env node
// check-composer-output.mjs — run the REAL composer branches and scan what they render.
//
//   node scripts/check-composer-output.mjs
//
// WHY THIS EXISTS (Sol QA r1, P1-3). The source-mode rule in check-forbidden-strings.mjs
// matched one narrow shape — a `.length` interpolated inside a bold header — and reported
// OK while two live branches still rendered aggregate counts: the Friday comms scorecard
// and Day Ahead's partial-source lines. Neither is a bold-header `.length`, and neither
// fires on ordinary data (Friday-only; all-sources-healthy), so nothing caught them.
//
// A grep over source can only find the count shapes someone anticipated. This EXECUTES
// each branch — including the ones that need a Friday, a failed data source, or an
// escalated registry to reach — and scans the strings that actually come out.
//
// One count is permitted, by name: the comms scorecard (@count-exemption:comms-scorecard),
// a deliberate carve-out ruled 2026-07-28. It measures the wire itself rather than
// Robert's work, and interrupts/day falling is how the remediation proves itself. Every
// other count is a violation. The exemption is matched by NAME so it passes deliberately.
import {
  waitingOnYouLines, appendNotesSection, replyFooter, renderWire, scorecardLines,
} from "../src/index.ts";

const ok = (s, d) => console.log(`STEP:${s}:OK${d ? ` ${d}` : ""}`);
let violations = 0;
let quiet = false;          // set while probing that a rule CAN fire
let quietHits = 0;
const fail = (branch, line, why) => {
  if (quiet) { quietHits += 1; return; }
  console.log(`STEP:composer-output:FAIL [${branch}] ${why}`);
  console.log(`    ${line}`);
  violations += 1;
};

/** The count shapes §2.3 bans, applied to RENDERED output. */
const COUNT_RULES = [
  { id: "parenthesised-count", re: /\(\s*\d+\+?\s*(?:queued|more|items?)?\s*\)/ },
  { id: "header-count", re: /<b>[^<]*\(\d+\+?[^)]*\)[^<]*<\/b>/ },
  { id: "plus-n-more", re: /…?\+\s*\d+\s*\+?\s*more/i },
  { id: "repeat-multiplier", re: /\(×\s*\d+\)/ },
  { id: "bare-source-count", re: /\b\d+\s+(?:source|item|note|draft|decision|record)s?\b/i },
  { id: "colon-count", re: /<b>[^<]+:<\/b>\s*\d+\s*$/ },
];

/** Lines the scorecard exemption covers — matched against the REAL rendered output of
 *  scorecardLines(), so the carve-out can never be wider than the function it names. */
function scorecardExemptLines() {
  const sample = {
    window_days: 7, sends_per_day: 4.2, interrupts_per_day: 1.7,
    median_call_response_hours: 3.5, decisions_total: 6, unanswered_refs: 2,
  };
  const withMedian = scorecardLines(sample);
  const withoutMedian = scorecardLines({ ...sample, median_call_response_hours: null, unanswered_refs: 1 });
  return new Set([...withMedian, ...withoutMedian]);
}
const EXEMPT = scorecardExemptLines();

function scan(branch, lines) {
  for (const line of lines) {
    if (!line) continue;
    if (EXEMPT.has(line)) continue;   // @count-exemption:comms-scorecard — by name, deliberately
    for (const r of COUNT_RULES) {
      if (r.re.test(line)) fail(branch, line, `renders a §2.3 aggregate count [${r.id}]`);
    }
  }
}

// ── Branch: Waiting on you, at real backlog size ─────────────────────────────
{
  const items = Array.from({ length: 45 }, (_, i) => ({
    ref: `C-T${String(i).padStart(4, "0")}`, punchline: `decision ${i}`,
    ts: `2026-07-27T00:00:${String(i % 60).padStart(2, "0")}Z`,
  }));
  scan("waiting-on-you", waitingOnYouLines(items).lines);
  scan("waiting-on-you-empty", waitingOnYouLines([]).lines);
  scan("waiting-on-you-unrenderable", waitingOnYouLines([{ ref: "C-X", punchline: "", ts: "2026-07-27T00:00:00Z" }]).lines);
}

// ── Branch: Notes, at real backlog size ──────────────────────────────────────
{
  const lines = [];
  appendNotesSection(lines, Array.from({ length: 234 }, (_, i) => ({
    key: `k${i}`, punchline: `note ${i}`, ts: "2026-07-26T00:00:00Z",
  })), "Notes");
  scan("notes", lines);
  const empty = [];
  appendNotesSection(empty, [], "Notes");
  scan("notes-empty", empty);
}

// ── Branch: the Friday comms scorecard (the branch the grep could not see) ───
{
  scan("scorecard-with-median", scorecardLines({
    window_days: 7, sends_per_day: 4.2, interrupts_per_day: 1.7,
    median_call_response_hours: 3.5, decisions_total: 6, unanswered_refs: 2,
  }));
  scan("scorecard-no-median", scorecardLines({
    window_days: 7, sends_per_day: 0.4, interrupts_per_day: 0, median_call_response_hours: null,
    decisions_total: 0, unanswered_refs: 1,
  }));
  // The exemption must be NARROW: a scorecard-shaped line that scorecardLines() does not
  // actually produce must still be caught.
  quiet = true; quietHits = 0;
  scan("scorecard-exemption-is-narrow", ["<b>Waiting on you (45):</b>"]);
  quiet = false;
  if (quietHits === 0) {
    console.log("STEP:composer-output:FAIL [scorecard-exemption-is-narrow] the exemption is too wide — it swallowed a real backlog count");
    violations += 1;
  } else {
    ok("composer-exemption-narrow", "a real backlog count is still caught alongside the exemption");
  }
}

// ── Branch: fire repeat rendering + the footer ───────────────────────────────
{
  scan("fire-render", renderWire(
    { type: "fire", punchline: "Booking form is rejecting every submission", key: "k", origin: "o" },
    "F-T1", ["↻ 09:15 ET — still open", "↻ 10:15 ET — still open"],
  ).split("\n"));
  scan("footer", replyFooter(["C-8KQ2M"]));
  scan("footer-empty", replyFooter([]));
}

// ── Branch: Day Ahead partial-source lines (Friday-independent, but data-dependent) ──
// These strings are rendered by composeAndSendDayAhead's inbox branch. They cannot be
// reached without a live env, so the exact literals are asserted here against the source
// to keep this check honest about what it did and did not execute.
{
  const { readFileSync } = await import("node:fs");
  const src = readFileSync(new URL("../src/index.ts", import.meta.url), "utf8");
  const expected = [
    '"<b>Inbox needs you:</b> some sources were unavailable this run — nothing visible in the rest."',
    '"<i>(some inbox sources were unavailable this run)</i>"',
  ];
  for (const lit of expected) {
    if (!src.includes(lit)) {
      console.log(`STEP:composer-output:FAIL [day-ahead-partial] expected count-free literal missing: ${lit}`);
      violations += 1;
    }
  }
  // And the counted forms must be gone.
  if (/\$\{failedSources\}/.test(src)) {
    console.log("STEP:composer-output:FAIL [day-ahead-partial] a failed-source COUNT is still interpolated into rendered output");
    violations += 1;
  }
}

if (violations > 0) {
  console.log(`STEP:composer-output:FAIL ${violations} violation(s) in executed composer output`);
  process.exit(1);
}
ok("composer-output", "all executed composer branches count-free (scorecard exempt by name)");
