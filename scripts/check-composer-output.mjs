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

/** Strip markup before classifying. Sol slipped `<i>(<b>2</b> inbox sources …)</i>` past
 *  every rule by putting a tag between the parenthesis and the number: the text a human
 *  reads and the string a regex sees were different documents. Classify what Robert sees. */
function normalize(html) {
  return html
    .replace(/<[^>]*>/g, "")
    .replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">")
    .replace(/\s+/g, " ")
    .trim();
}

/** Mask the numeric forms that are NEVER tallies, before classification.
 *
 *  A date, a clock time, a dollar figure, a percentage, a duration and a list ordinal are
 *  facts about the world, not counts of Robert's queue. Masking them is what lets the
 *  tally rules stay strict without rejecting "you owe a reply since 2026-07-20" or
 *  "09:15 ET — source recovered". Enumerating the non-tally numeric shapes is decidable;
 *  enumerating every tally shape is not — three rounds of open-ended digit rules proved
 *  that in both directions. */
function maskNonTallyNumbers(text) {
  return text
    .replace(/\d{4}-\d{2}-\d{2}(?:T[\d:.]+Z?)?/g, "«date»")   // ISO dates/timestamps
    .replace(/\b\d{1,2}:\d{2}(?::\d{2})?\b/g, "«time»")        // clock times
    .replace(/\$\s?[\d,]+(?:\.\d+)?/g, "«money»")              // dollar figures
    .replace(/\b\d+(?:\.\d+)?\s?%/g, "«pct»")                  // percentages
    .replace(/\b\d+(?:\.\d+)?\s?(?:d|h|hrs?|hours?|min(?:utes?)?|s|sec(?:onds?)?|ms|days?|weeks?|months?)\b/gi, "«dur»")
    .replace(/(^|[\s(])\d+[.)](?=\s)/g, "$1«ord»");             // list ordinals: "1. " / "2) "
}

/** Numbers, digits AND words — "two inbox sources were unavailable" is a tally. */
const NUM = "(?:\\d+|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|dozen|hundreds?|thousands?)";

/** AGGREGATE/STATUS vocabulary — the nouns that make a number a TALLY OF ROBERT'S QUEUE
 *  rather than a fact about the world.
 *
 *  Requiring vocabulary is what lets legitimate content through. Three rounds of
 *  open-ended digit rules kept failing in both directions: they missed suffix and written
 *  forms, and they rejected "$239.31 spent for 17 clicks", "09:15 ET" and "1. Review the
 *  brief". "clicks" and "dollars" are deliberately ABSENT — those are business metrics,
 *  which are exactly what Robert is supposed to see. */
const AGG = "(?:source|item|note|draft|decision|record|thread|read|entry|ref|alert|incident|backlog|queue|task|reply|message)s?"
          + "|(?:pending|unanswered|unread|waiting|remaining|queued|outstanding|more)";

/** The count shapes §2.3 bans, applied to NORMALISED rendered output.
 *  Both directions — a tally reads the same whether the number leads or trails. */
const COUNT_RULES = [
  // "45 items", "two inbox sources", "3 more"
  { id: "tally-prefix", re: new RegExp(`\\b${NUM}\\s+(?:\\w+\\s+){0,2}(?:${AGG})\\b`, "i") },
  // "failed reads: 2", "unanswered — 3", "notes 12"
  { id: "tally-suffix", re: new RegExp(`\\b(?:${AGG})\\b[^.\\n]{0,24}?[:\u2014-]?\\s*\\b${NUM}\\b`, "i") },
  // "(45)" / "(50+ queued)" immediately after a section label
  { id: "parenthesised-count", re: /\(\s*\d+\+?\s*(?:queued|more|items?)?\s*\)/ },
  // "…+42 more" / "+229 more"
  { id: "plus-n-more", re: /\+\s*\d+\s*\+?\s*more/i },
  // "(×4)"
  { id: "repeat-multiplier", re: /\(\s*×\s*\d+\s*\)/ },
];

/** LEGITIMATE content that must NEVER trip a rule. Asserted on every run, so a future
 *  broadening of the rules cannot quietly start rejecting real digests — a digest that
 *  refuses to render because it contains a clock time is a worse failure than the count
 *  regression being guarded against. */
const MUST_PASS_CONTROLS = [
  "<i>($239.31 spent for 17 clicks)</i>",
  "<i>(09:15 ET — source recovered)</i>",
  "<i>(1. Review the approved brief)</i>",
  "🔴 <b>Urgent: Google Ads fire: BWM Core spent $239.31 in 7d for 17 clicks</b>",
  "• 10:30 — Robert + Emiliya (lead follow-up)",
  "<b>Overnight:</b> handled without you — full brief on file.",
];

/** Tallies that must ALWAYS be caught, in every form Sol has found. */
const MUST_FAIL_CONTROLS = [
  ["suffix tally",           "<i>(some inbox sources were unavailable this run — failed reads: 2)</i>"],
  ["written-number tally",   "<i>(two inbox sources were unavailable this run)</i>"],
  ["markup-wrapped tally",   "<i>(<b>2</b> inbox sources were unavailable this run)</i>"],
  ["classic header count",   "<b>Waiting on you (45):</b>"],
  ["plus-n-more",            "…+42 more — answer by ref anytime"],
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

/** Scan COMPOSER CHROME for aggregate tallies.
 *
 *  §2.3 (no counts) governs what the COMPOSER writes — headers, status lines, footers.
 *  §2.6 (source-unit preservation) governs what the SENDER wrote, and requires it to pass
 *  through verbatim. Those two rules point opposite ways on the same string, so the scan
 *  has to know which is which: a real punchline reads "6 client items can't be approved",
 *  and that is business content Robert needs, not a tally of his backlog. Censoring it
 *  would violate §2.6 to satisfy §2.3.
 *
 *  So source units are REMOVED before classification, and what remains — the composer's
 *  own words — is what gets scanned. Callers pass the units they fed in. */
function scan(branch, lines, { scorecard = false, sourceUnits = [] } = {}) {
  for (const line of lines) {
    if (!line) continue;
    // The exemption applies ONLY inside the scorecard branch, and ONLY to a line whose
    // shape the ruling actually covers. Anything else there is scanned like any output.
    if (scorecard && isRuledScorecardLine(line)) continue;
    let text = normalize(line);
    for (const u of sourceUnits) {
      const n = normalize(u);
      if (n) text = text.split(n).join(" ");
    }
    text = maskNonTallyNumbers(text).replace(/\s+/g, " ").trim();
    if (!text) continue;
    for (const r of COUNT_RULES) {
      if (r.re.test(text)) fail(branch, line, `renders a §2.3 aggregate count [${r.id}] in composer chrome: "${text}"`);
    }
  }
}

// ── Branch: Waiting on you, at real backlog size ─────────────────────────────
{
  // Realistic punchlines, including ones that legitimately CONTAIN counts — a sender's
  // "6 client items can't be approved" is business content and must survive.
  const REAL = [
    "Design2Sell approval buttons broke with today's security update — 6 client items can't be approved.",
    "Google Ads fire: BWM Core spent $239.31 in 7d for 17 clicks and no conversions.",
    "Cathryn wants 10 client slots at the Pilot price — approve the block or hold list price.",
  ];
  const items = Array.from({ length: 45 }, (_, i) => ({
    ref: `C-T${String(i).padStart(4, "0")}`, punchline: REAL[i % REAL.length],
    ts: `2026-07-27T00:00:${String(i % 60).padStart(2, "0")}Z`,
  }));
  scan("waiting-on-you", waitingOnYouLines(items).lines, { sourceUnits: REAL });
  scan("waiting-on-you-empty", waitingOnYouLines([]).lines);
  scan("waiting-on-you-unrenderable", waitingOnYouLines([{ ref: "C-X", punchline: "", ts: "2026-07-27T00:00:00Z" }]).lines);
}

// ── Branch: Notes, at real backlog size ──────────────────────────────────────
{
  const NOTE_TEXT = [
    "Asap Pest Wildlife: a client message reached the 2-hour team follow-up mark.",
    "Approvals tracker: 9 items pending over 48h.",
    "Townsend Realty Group: a client message reached the 2-hour team follow-up mark.",
  ];
  const lines = [];
  appendNotesSection(lines, Array.from({ length: 234 }, (_, i) => ({
    key: `k${i}`, punchline: NOTE_TEXT[i % NOTE_TEXT.length], ts: "2026-07-26T00:00:00Z",
  })), "Notes");
  scan("notes", lines, { sourceUnits: NOTE_TEXT });
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
  const firePunch = "Design2Sell booking form is rejecting every submission — 12 leads lost today.";
  const fireStakes = "Every inbound lead is being lost while this is open.";
  scan("fire-render", renderWire(
    { type: "fire", punchline: firePunch, stakes: fireStakes, key: "k", origin: "o" },
    "F-T1", ["↻ 09:15 ET — still open", "↻ 10:15 ET — still open"],
  ).split("\n"), { sourceUnits: [firePunch, fireStakes] });
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
  const thread = [{ sender_email: "a@example.com", subject: "Quote for 3 rooms — can you confirm?", action_taken: "escalate" }];
  const esc = [{ sender_email: "b@example.com", sarah_reason: "needs a human", scope: null }];
  const due = [{ sender_email: "c@example.com", subject: "still waiting", direction: "owed_by_us", due_at: "2026-07-20T00:00:00Z" }];
  const INBOX_UNITS = ["Quote for 3 rooms — can you confirm?", "needs a human", "still waiting",
                       "a@example.com", "b@example.com", "c@example.com"];

  // All 8 combinations of loaded/failed across the three sources — including every
  // partial-read shape, with and without visible items.
  for (const nt of [thread, null]) {
    for (const es of [esc, null]) {
      for (const ov of [due, null]) {
        const failed = [nt, es, ov].filter((x) => x === null).length;
        scan(`day-ahead-inbox(failed=${failed},items=${[nt, es, ov].filter(Boolean).length})`,
             buildInboxSection(nt, es, ov), { sourceUnits: INBOX_UNITS });
      }
    }
  }
  // And the empty-but-partial shapes, which take the other rendering branch.
  scan("day-ahead-inbox(empty,partial)", buildInboxSection([], null, null));
  scan("day-ahead-inbox(empty,healthy)", buildInboxSection([], [], []));

  // ── APPROVED-SHAPE PINNING for the partial-status lines ──────────────────
  // The decisive control, and the one that closes suffix / written-number /
  // markup-wrapped tallies at once. Rather than asking "does this contain a banned
  // shape?" — a question three rounds of regexes could not answer in either direction —
  // it asks "is this EXACTLY one of the outputs that was approved?". Enumerating the
  // allowed set is decidable; enumerating the disallowed set is not. This is the same
  // move that made source-unit preservation work.
  const APPROVED_STATUS = {
    allNull:        "<b>Inbox needs you:</b> (data unavailable)",
    emptyPartial:   "<b>Inbox needs you:</b> some sources were unavailable this run — nothing visible in the rest.",
    emptyHealthy:   "<b>Inbox needs you:</b> nothing — Sarah's lanes are clear.",
    visibleHeader:  "<b>Inbox needs you:</b>",
    visiblePartial: "<i>(some inbox sources were unavailable this run)</i>",
  };
  const pin = (name, actual, expected) => {
    if (actual !== expected) {
      console.log(`STEP:composer-output:FAIL [day-ahead-pin:${name}] output is not the approved shape`);
      console.log(`    expected: ${expected}`);
      console.log(`    actual:   ${actual}`);
      violations += 1;
    }
  };

  const allNull = buildInboxSection(null, null, null);
  pin("all-null-length", String(allNull.length), "1");
  pin("all-null", allNull[0], APPROVED_STATUS.allNull);

  const emptyPartial = buildInboxSection([], null, null);
  pin("empty-partial-length", String(emptyPartial.length), "1");
  pin("empty-partial", emptyPartial[0], APPROVED_STATUS.emptyPartial);

  const emptyHealthy = buildInboxSection([], [], []);
  pin("empty-healthy-length", String(emptyHealthy.length), "1");
  pin("empty-healthy", emptyHealthy[0], APPROVED_STATUS.emptyHealthy);

  const visiblePartial = buildInboxSection(thread, null, null);
  pin("visible-partial-header", visiblePartial[0], APPROVED_STATUS.visibleHeader);
  pin("visible-partial-status", visiblePartial[visiblePartial.length - 1], APPROVED_STATUS.visiblePartial);
  for (const l of visiblePartial.slice(1, -1)) {
    if (!l.startsWith("•")) {
      console.log(`STEP:composer-output:FAIL [day-ahead-pin:visible-partial-body] a non-bullet line appeared between the header and the status line: ${l}`);
      violations += 1;
    }
  }

  const visibleHealthy = buildInboxSection(thread, esc, due);
  pin("visible-healthy-header", visibleHealthy[0], APPROVED_STATUS.visibleHeader);
  if (visibleHealthy.some((l) => l === APPROVED_STATUS.visiblePartial)) {
    console.log("STEP:composer-output:FAIL [day-ahead-pin:visible-healthy] a partial-read status line rendered when every source loaded");
    violations += 1;
  }
  for (const l of visibleHealthy.slice(1)) {
    if (!l.startsWith("•")) {
      console.log(`STEP:composer-output:FAIL [day-ahead-pin:visible-healthy-body] unexpected non-bullet line: ${l}`);
      violations += 1;
    }
  }
}

// ── Controls: the rules must catch every known tally and reject no real content ───
{
  for (const [name, sample] of MUST_FAIL_CONTROLS) {
    quiet = true; quietHits = 0;
    scan(`control-must-fail:${name}`, [sample]);
    quiet = false;
    if (quietHits === 0) {
      console.log(`STEP:composer-output:FAIL [control] a known tally slipped through — ${name}: ${sample}`);
      violations += 1;
    }
  }
  ok("composer-must-fail-controls", `${MUST_FAIL_CONTROLS.length} known tally forms all caught`);

  const before = violations;
  for (const sample of MUST_PASS_CONTROLS) scan("control-must-pass", [sample]);
  if (violations > before) {
    console.log("STEP:composer-output:FAIL [control] the rules rejected legitimate content — a digest that refuses to render over a clock time is worse than the regression being guarded");
  } else {
    ok("composer-must-pass-controls", `${MUST_PASS_CONTROLS.length} legitimate lines (money, times, ordinals) all accepted`);
  }
}

if (violations > 0) {
  console.log(`STEP:composer-output:FAIL ${violations} violation(s) in executed composer output`);
  process.exit(1);
}
ok("composer-output", "all executed composer branches count-free (scorecard exempt by name)");
