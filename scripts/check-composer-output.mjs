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
  buildInboxSection,
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
  // Allows up to two intervening words: Sol's bypass rendered "2 inbox sources
  // unavailable", which a rigid `\d+\s+sources` rule read as clean.
  { id: "bare-source-count", re: /\b\d+\s+(?:\w+\s+){0,2}(?:source|item|note|draft|decision|record|thread|message|reply|task|ref)s?\b/i },
  { id: "colon-count", re: /<b>[^<]+:<\/b>\s*\d+\s*$/ },
  // Chrome that OPENS with a number is a tally by construction. Scoped to parenthetical
  // and italic status lines so a punchline like "spent $239.31 for 17 clicks" — real
  // business content, not chrome — is untouched.
  { id: "parenthetical-leading-count", re: /^(?:<i>)?\(\s*\d+\b/ },
];

/** @count-exemption:comms-scorecard — the THREE RULED LINES, pinned by SHAPE.
 *
 *  The first version built this set from whatever scorecardLines() returned, which meant
 *  a backlog line ADDED to that function exempted itself automatically. Sol slipped
 *  `<b>Waiting on you (N):</b>` through exactly that way. The carve-out is now a fixed
 *  list of the three shapes the ruling covers — window days, sends/day, interrupts/day
 *  and target, decision total, unanswered — and nothing else that ever appears there.
 *
 *  A wording change to a ruled line breaks its pattern, which surfaces as an uncovered
 *  line rather than a silent pass: re-ruling is a decision, not a side effect. */
const SCORECARD_RULED_PATTERNS = [
  /^📊 <b>Comms scorecard \(\d+d\):<\/b>$/,
  /^• [\d.]+ sends\/day · [\d.]+ live interrupts\/day \(target ≤3\)$/,
  /^• Decisions answered: (?:median [\d.]+h over \d+|none answered this week) · \d+ still unanswered$/,
];
const isRuledScorecardLine = (line) => SCORECARD_RULED_PATTERNS.some((re) => re.test(line));

function scan(branch, lines, { scorecard = false } = {}) {
  for (const line of lines) {
    if (!line) continue;
    // The exemption applies ONLY inside the scorecard branch, and ONLY to a line whose
    // shape the ruling actually covers. Anything else there is scanned like any output.
    if (scorecard && isRuledScorecardLine(line)) continue;
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
  const scWith = scorecardLines({
    window_days: 7, sends_per_day: 4.2, interrupts_per_day: 1.7,
    median_call_response_hours: 3.5, decisions_total: 6, unanswered_refs: 2,
  });
  const scWithout = scorecardLines({
    window_days: 7, sends_per_day: 0.4, interrupts_per_day: 0, median_call_response_hours: null,
    decisions_total: 0, unanswered_refs: 1,
  });
  scan("scorecard-with-median", scWith, { scorecard: true });
  scan("scorecard-no-median", scWithout, { scorecard: true });

  // COVERAGE: every line the function actually returns must be one the ruling covers.
  // Without this, a line that carries no count but was never ruled on would pass silently,
  // and the carve-out would drift wider than the decision that authorised it.
  for (const line of [...scWith, ...scWithout]) {
    if (!isRuledScorecardLine(line)) {
      console.log(`STEP:composer-output:FAIL [scorecard-coverage] scorecardLines() returns a line outside the ruling: ${line}`);
      violations += 1;
    }
  }
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

// ── Branch: Day Ahead "Inbox needs you", EXECUTED with failed sources ────────
// This branch is why the checker exists in its current form. It used to be verified by
// searching src/index.ts for two approved literals, and Sol slipped an inline-computed
// failed-source count past it by leaving an unused copy of a literal elsewhere in the
// file. Literal presence is not branch proof. buildInboxSection() is now a pure function,
// so every partial-read combination is actually RUN and its real output scanned.
{
  const thread = [{ sender_email: "a@example.com", subject: "a real subject", action_taken: "escalate" }];
  const esc = [{ sender_email: "b@example.com", sarah_reason: "needs a human", scope: null }];
  const due = [{ sender_email: "c@example.com", subject: "still waiting", direction: "owed_by_us", due_at: "2026-07-20T00:00:00Z" }];

  // All 8 combinations of loaded/failed across the three sources — including every
  // partial-read shape, with and without visible items.
  for (const nt of [thread, null]) {
    for (const es of [esc, null]) {
      for (const ov of [due, null]) {
        const failed = [nt, es, ov].filter((x) => x === null).length;
        scan(`day-ahead-inbox(failed=${failed},items=${[nt, es, ov].filter(Boolean).length})`,
             buildInboxSection(nt, es, ov));
      }
    }
  }
  // And the empty-but-partial shapes, which take the other rendering branch.
  scan("day-ahead-inbox(empty,partial)", buildInboxSection([], null, null));
  scan("day-ahead-inbox(empty,healthy)", buildInboxSection([], [], []));

  // The branch must genuinely have been reachable — a silently-empty section would make
  // every scan above vacuous.
  const partialOut = buildInboxSection([], null, null).join("\n");
  if (!/unavailable/.test(partialOut)) {
    console.log("STEP:composer-output:FAIL [day-ahead-inbox] the partial-read branch produced no partial-read line — the scans above proved nothing");
    violations += 1;
  }
}

if (violations > 0) {
  console.log(`STEP:composer-output:FAIL ${violations} violation(s) in executed composer output`);
  process.exit(1);
}
ok("composer-output", "all executed composer branches count-free (scorecard exempt by name)");
