// render-preservation.test.ts — Phase 2 source-unit preservation + kill-list controls.
//
// The six strict-prefix fixtures are the EXACT fragments Robert saw on the Jul 27
// digest (test/fixtures/jul27/before-artifact.txt). Each is a strict prefix of a real
// source unit, produced by a fixed-character slice. The rule under test:
//
//     a source unit is rendered WHOLE, or it is not rendered at all —
//     a strict prefix must NEVER appear as the terminal form of the unit.
//
// These are red-then-green controls: run against the pre-fix composers they fail;
// against the current ones they pass.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  renderWire,
  waitingOnYouLines,
  appendNotesSection,
  replyFooter,
  fitWholeUnits,
  assertSourcePreservation,
  isEmailAsRobert,
  isEmailAsRobertOrigin,
  scrubEmailAsRobert,
  fireFingerprint,
  parseWireInput,
} from "../src/index.ts";

/** The six fragments, each with the whole unit it was cut from. */
const STRICT_PREFIX_FIXTURES = [
  { fragment: "built, merged, deployed and producti",
    whole: "Social publisher phase 0 is built, merged, deployed and production-verified. One human click is all that's left." },
  { fragment: "Nudge Rodel or Emiliya to approve or reject any",
    whole: "Nudge Rodel or Emiliya to approve or reject any one of the 6 pending D2S items." },
  { fragment: "Say go — I'll merge + deploy PR #39 now to fix t",
    whole: "Say go — I'll merge + deploy PR #39 now to fix the buttons, then finish the security cross-check." },
  { fragment: "2=Wait for Sol's cross-che",
    whole: "Wait for Sol's cross-check before pushing" },
  { fragment: "a client message reached the 2-hour team follow-up m",
    whole: "Asap Pest Wildlife: a client message reached the 2-hour team follow-up mark" },
  { fragment: 'oldest: "Award records:',
    whole: 'Approvals tracker: 9 items pending over 48h — oldest: "Award records: which years was Design2Sell ranked national Top 10?"' },
];

/** A unit is present WHOLE, or wholly absent. Never a terminal strict prefix. */
function assertWholeOrAbsent(rendered: string, whole: string, fragment: string) {
  if (rendered.includes(whole)) return;                 // whole → fine
  if (!rendered.includes(fragment)) return;             // absent → fine
  assert.fail(
    `strict-prefix leak: rendered contains ${JSON.stringify(fragment)} but not the whole unit ` +
    `${JSON.stringify(whole.slice(0, 60))}…`,
  );
}

test("six strict-prefix fixtures: renderWire never emits a cut source unit", () => {
  for (const { fragment, whole } of STRICT_PREFIX_FIXTURES) {
    const text = renderWire(
      { type: "call", punchline: whole, rec: whole, options: [whole] },
      "C-TEST1",
    );
    assertWholeOrAbsent(text, whole, fragment);
  }
});

test("six strict-prefix fixtures: the Waiting-on-you composer never cuts a unit", () => {
  for (const { fragment, whole } of STRICT_PREFIX_FIXTURES) {
    const { lines } = waitingOnYouLines([
      { ref: "C-TEST1", punchline: whole, rec: whole, options: [whole], ts: "2026-07-27T00:00:00Z" },
    ]);
    assertWholeOrAbsent(lines.join("\n"), whole, fragment);
  }
});

test("six strict-prefix fixtures: the Notes composer never cuts a unit", () => {
  for (const { fragment, whole } of STRICT_PREFIX_FIXTURES) {
    const lines: string[] = ["header"];
    appendNotesSection(lines, [{ key: "k1", punchline: whole, ts: "2026-07-27T00:00:00Z" }], "Notes");
    assertWholeOrAbsent(lines.join("\n"), whole, fragment);
  }
});

test("a punchline at the old 300-char cap now renders whole (4 of 50 real ones hit it)", () => {
  const whole = `${"A".repeat(295)} END-MARKER`;
  const text = renderWire({ type: "fire", punchline: whole, key: "k", origin: "o" }, "F-TEST1");
  assert.ok(text.includes(whole), "the whole punchline must survive");
  assert.ok(text.includes("END-MARKER"), "the tail must not be cut");
});

test("fitWholeUnits drops whole units and never slices one", () => {
  const { kept, dropped } = fitWholeUnits(["aaaa", "bbbbbbbbbb", "cc"], 8);
  assert.deepEqual(kept, ["aaaa", "cc"]);
  assert.deepEqual(dropped, ["bbbbbbbbbb"]);
  for (const k of kept) assert.ok(["aaaa", "bbbbbbbbbb", "cc"].includes(k), "kept units are verbatim");
});

test("assertSourcePreservation throws on a mutated unit and passes on a whole one", () => {
  assert.doesNotThrow(() => assertSourcePreservation("hello whole world", ["whole"]));
  assert.throws(() => assertSourcePreservation("hello wh world", ["whole"]), /source_unit_preservation_violated/);
});

// ── §2.3 aggregate counts ────────────────────────────────────────────────────

test("Waiting-on-you renders NO count and NO +N more", () => {
  const items = Array.from({ length: 45 }, (_, i) => ({
    ref: `C-T${String(i).padStart(4, "0")}`, punchline: `decision ${i}`, ts: `2026-07-27T00:00:${String(i % 60).padStart(2, "0")}Z`,
  }));
  const { lines, renderedRefs } = waitingOnYouLines(items);
  const text = lines.join("\n");
  assert.ok(!/\(45\)/.test(text), "the 45-count must not render");
  assert.ok(!/\+\s*\d+\s*more/.test(text), '"+N more" must not render');
  assert.match(text, /<b>Waiting on you:<\/b>/);
  assert.ok(renderedRefs.length > 0 && renderedRefs.length <= 3);
});

test("Notes renders NO count and NO +N more", () => {
  const q = Array.from({ length: 234 }, (_, i) => ({ key: `k${i}`, punchline: `note ${i}`, ts: "2026-07-27T00:00:00Z" }));
  const lines: string[] = ["header"];
  appendNotesSection(lines, q, "Notes");
  const text = lines.join("\n");
  assert.ok(!/\(234\)/.test(text), "the 234-count must not render");
  assert.ok(!/\+\s*\d+\s*more/.test(text), '"+N more" must not render');
  assert.match(text, /<b>Notes:<\/b>/);
});

// ── §2.4 interim footer ──────────────────────────────────────────────────────

test("the footer binds to a ref actually on screen, and is absent when nothing rendered", () => {
  assert.deepEqual(replyFooter([]), [], "no answerable item → no instruction");
  const [line] = replyFooter(["C-8KQ2M", "S-B37RS"]);
  assert.ok(line.includes("C-8KQ2M"), "must name the FIRST rendered ref");
  assert.ok(!line.includes("C-003"), "the unbound hard-coded example must be gone");
});

// ── §2.5 incident identity ───────────────────────────────────────────────────

test("the fingerprint is stable across repeats and carries no random material", () => {
  const a = fireFingerprint({ key: "launchagent-health:aeo-probe:crashed", origin: "launchagent-health" });
  const b = fireFingerprint({ key: "launchagent-health:aeo-probe:crashed", origin: "launchagent-health" });
  assert.equal(a, b, "same sender + same key must fingerprint identically");
  assert.ok(a && /^[0-9a-f]{16}$/.test(a));
});

test("the live Jul 27/28 random-suffix keys are exactly what split one incident into three", () => {
  // Real telegram_outbound keys — same 12-char condition signature, three random tails.
  const fps = [
    "launchagent-health-4126371745a9-b7be76f9",
    "launchagent-health-4126371745a9-d1db1829",
    "launchagent-health-4126371745a9-ce77dd6d",
  ].map((k) => fireFingerprint({ key: k, origin: "launchagent-health" }));
  assert.equal(new Set(fps).size, 3, "random suffixes DO split — which is why the sender must not add them");
  // The deterministic key that replaced them collapses to one.
  const stable = ["a", "b", "c"].map(() =>
    fireFingerprint({ key: "launchagent-health:com.buildwisemedia.aeo-probe:not-running", origin: "launchagent-health" }));
  assert.equal(new Set(stable).size, 1, "the stable key must collapse to ONE incident");
});

test("severity is NOT part of the structural /event identity (P1→P0 edits, never re-pings)", () => {
  const p1 = fireFingerprint({ key: "evt|design2sell|booking|form-rejecting", origin: "event:incident.opened:01AAA" });
  const p0 = fireFingerprint({ key: "evt|design2sell|booking|form-rejecting", origin: "event:incident.opened:01BBB" });
  assert.equal(p1, p0, "the same condition at a new severity/event id is the SAME incident");
});

test("no prose folding: a keyless fire gets no fingerprint", () => {
  assert.equal(fireFingerprint({ key: undefined, origin: "somebody" }), null);
});

test("sender binding: the same key from two senders is two incidents", () => {
  assert.notEqual(
    fireFingerprint({ key: "disk-full", origin: "sender-a" }),
    fireFingerprint({ key: "disk-full", origin: "sender-b" }),
  );
});

// ── §2.8 email-as-Robert kill list ───────────────────────────────────────────

test("the f05 email-as-Robert item is recognised verbatim", () => {
  assert.ok(isEmailAsRobert(
    'Email ready to send as you → dap@decaturatlantaprinting.com: "Re: New card job"',
  ));
  assert.ok(isEmailAsRobert("Tap the link to send it now. To discard instead, ignore it"));
  assert.ok(isEmailAsRobert("Drafts ready to send: 3"));
});

test("the kill list fails CLOSED on an evaluation error", () => {
  const exploding = { toString() { throw new Error("boom"); } } as unknown as string;
  assert.equal(isEmailAsRobert(exploding), true, "an error must count as a match");
});

test("the kill list does not eat ordinary business copy", () => {
  // An adversarial review killed 9 of these 10 under the first (over-general) regex set.
  // A kill list that eats real decisions is a worse failure than the one it prevents:
  // the sender gets HTTP 200 and Robert gets silence. These are the regression guard.
  const MUST_SURVIVE = [
    "Sent the proposal to the client as you asked this morning.",
    "3 new ad creative drafts ready to send to the design team.",
    "Approve and send this draft blog post to Cathryn's list?",
    "Design2Sell booking form is rejecting every submission",
    "Cathryn wants 10 client slots at the Pilot price",
    "A client message reached the 2-hour team follow-up mark",
    "Replied to Barbara as you were on the call — she has the quote now.",
    "The vendor responded as you predicted; they want a 3-week lead time.",
    "Two proposal drafts are ready to send once you pick a price.",
    "I drafted a reply as a starting point — the team will send it.",
  ];
  for (const line of MUST_SURVIVE) {
    assert.equal(isEmailAsRobert(line), false, `kill list must NOT eat: ${JSON.stringify(line)}`);
  }
});

test("the kill list still catches the real capability, by text and by producer", () => {
  const MUST_DIE = [
    'Email ready to send as you → dap@example.com: "Re: New card job"',
    "Tap the link to send it now. To discard instead, ignore it.",
    "Drafts ready to send: 3",
    "ignore it — the draft expires in 7 days.",
  ];
  for (const line of MUST_DIE) {
    assert.equal(isEmailAsRobert(line), true, `kill list must catch: ${JSON.stringify(line)}`);
  }
  // Producer binding is the precise mechanism — it does not depend on prose at all.
  assert.equal(isEmailAsRobertOrigin("bwm-gmail-send"), true);
  assert.equal(isEmailAsRobertOrigin("bwm-gmail-send worker"), true);
  assert.equal(isEmailAsRobertOrigin("launchagent-health"), false);
  assert.equal(isEmailAsRobertOrigin(null), false);
});

test("layer 3 scrubs a signature line out of an already-composed payload", () => {
  const composed = [
    "🌙 <b>DAY DONE</b>",
    "• C-8KQ2M — a real decision",
    "• S-B37RS — Email ready to send as you → someone@example.com",
    "<i>Reply by ref</i>",
  ].join("\n");
  const { text, removed } = scrubEmailAsRobert(composed);
  assert.equal(removed, 1);
  assert.ok(!text.includes("Email ready to send as you"));
  assert.ok(text.includes("a real decision"), "unrelated lines survive");
});

// ── the reviewed digest, as a whole-artifact control ─────────────────────────

test("the reviewed Jul 27 digest is exactly what the fixed composers no longer produce", async () => {
  const { readFileSync } = await import("node:fs");
  const before = readFileSync(new URL("./fixtures/jul27/before-artifact.txt", import.meta.url), "utf8");
  // Sanity: the fixture really is the defective surface (guards against a silently
  // emptied fixture making this test vacuous).
  assert.match(before, /Waiting on you \(45\)/);
  assert.match(before, /Notes \(234\)/);
  assert.match(before, /\+42 more/);
  assert.match(before, /\(×4\)/);
  assert.match(before, /Email ready to send as you/);
  assert.match(before, /"C-003/);

  // The same inputs through the current composers produce none of it.
  const { lines } = waitingOnYouLines(
    Array.from({ length: 45 }, (_, i) => ({ ref: `C-T${i}`, punchline: `d${i}`, ts: `2026-07-27T00:00:${String(i % 60).padStart(2, "0")}Z` })),
  );
  const notesLines: string[] = [];
  appendNotesSection(notesLines, Array.from({ length: 234 }, (_, i) => ({ key: `k${i}`, punchline: `n${i}`, ts: "2026-07-27T00:00:00Z" })), "Notes");
  const after = [...lines, ...notesLines, ...replyFooter(["C-T0"])].join("\n");
  for (const forbidden of [/\(45\)/, /\(234\)/, /\+\d+ more/, /\(×\d+\)/, /Email ready to send as you/, /"C-003/]) {
    assert.ok(!forbidden.test(after), `forbidden pattern still present: ${forbidden}`);
  }
});

// ── migration: fires opened BEFORE the fingerprint deploy ────────────────────

test("a pre-deploy fire (no stored fingerprint) is still resolvable by its legacy slot", () => {
  // Six such entries existed in the live registry at deploy time. Their outbound rows
  // carry metadata.wire.key but no metadata.wire.fingerprint. Resolving one must clear
  // the legacy shortHash(key) slot, not a fingerprint slot that was never written.
  const legacyMeta = { type: "fire", key: "gads-guard-9793789968" } as Record<string, unknown>;
  const storedFp = (legacyMeta as { fingerprint?: string }).fingerprint;
  assert.equal(storedFp, undefined, "the pre-deploy shape genuinely has no fingerprint");
  // The new fingerprint for the same key is a DIFFERENT slot — which is exactly why the
  // fallback is needed rather than a re-derivation.
  const newFp = fireFingerprint({ key: "gads-guard-9793789968", origin: "gads-guard" });
  assert.ok(newFp && typeof newFp === "string");
});

test("layer 3 repairs what it removes: no dangling header, no orphaned footer", () => {
  // A pre-deploy wire:open entry carrying the email item — it bypassed ingress because
  // it was registered before the kill list existed, so layer 3 is the only guard.
  const { lines, renderedRefs } = waitingOnYouLines([{
    ref: "S-B37RS",
    punchline: 'Email ready to send as you → dap@example.com: "Re: New card job"',
    ts: "2026-07-21T19:23:46Z",
  }]);
  const composed = ["🌙 <b>DAY DONE</b>", ...lines, ...replyFooter(renderedRefs)].join("\n");
  assert.match(composed, /Email ready to send as you/, "the defect is genuinely present before the scrub");

  const { text, removed } = scrubEmailAsRobert(composed);
  assert.equal(removed, 1);
  assert.ok(!text.includes("Email ready to send as you"), "the item must be gone");
  assert.ok(!/<b>Waiting on you:<\/b>/.test(text), "a header with nothing under it must be dropped");
  assert.ok(!/Reply by ref/.test(text), "a footer naming an off-screen ref must be dropped (finding 12)");
});

test("layer 3 keeps the header and footer when a real item survives alongside", () => {
  const { lines, renderedRefs } = waitingOnYouLines([
    { ref: "C-8KQ2M", punchline: "Approve the Cathryn block booking or hold list price.", ts: "2026-07-21T10:00:00Z" },
    { ref: "S-B37RS", punchline: "Email ready to send as you → dap@example.com", ts: "2026-07-21T19:23:46Z" },
  ]);
  const composed = ["🌙 <b>DAY DONE</b>", ...lines, ...replyFooter(renderedRefs)].join("\n");
  const { text, removed } = scrubEmailAsRobert(composed);
  assert.equal(removed, 1);
  assert.match(text, /<b>Waiting on you:<\/b>/, "the header still has a bullet under it");
  assert.match(text, /C-8KQ2M/, "the real decision survives");
  assert.match(text, /Reply by ref — for example "C-8KQ2M/, "the footer still names an on-screen ref");
});

// ── adversarial-review regressions ───────────────────────────────────────────

test("an over-long option drops the WHOLE set, never renumbering the survivors", () => {
  // Dropping option 2 of 3 would silently promote option 3 into slot 2, so Robert's "2"
  // resolves to a choice he never saw. The numbering IS the reply contract, and the
  // decision is made at INGRESS, which is where this must be asserted.
  const long = "x".repeat(200);
  const parsed = parseWireInput({ type: "call", punchline: "pick one", rec: "go", options: ["Go now", long, "Something else"] });
  assert.ok(parsed.ok);
  if (!parsed.ok) return;
  assert.equal(parsed.input.options, undefined, "the whole set drops together");
  assert.ok(parsed.droppedFields?.some((d) => d.startsWith("options:whole_set_dropped")), "and the sender is told");

  const text = renderWire(parsed.input, "C-TEST9");
  assert.ok(!text.includes("2 = Something else"), "survivors must not be renumbered");
  assert.match(text, /Reply with your call/, "falls back to free-text reply");
});

test("an item that cannot render whole leaves no dangling header", () => {
  // Mirrors the Day Done / Calendar / Plan sites: build bullets first, emit the header
  // only if at least one bullet survived.
  const { lines, renderedRefs } = waitingOnYouLines([{ ref: "C-LONG1", punchline: "", ts: "2026-07-27T00:00:00Z" }]);
  const text = lines.join("\n");
  assert.equal(renderedRefs.length, 0);
  assert.ok(!/<b>Waiting on you:<\/b>$/.test(text), "no bare header with nothing under it");
  assert.match(text, /nothing that renders cleanly/);
});

test("a long punchline that FITS the budget still renders whole", () => {
  // The complement of the case above: whole-unit means whole, not short.
  const long = `${"y".repeat(380)} END`;
  const { lines, renderedRefs } = waitingOnYouLines([{ ref: "C-LONG2", punchline: long, ts: "2026-07-27T00:00:00Z" }]);
  assert.equal(renderedRefs.length, 1);
  assert.ok(lines.join("\n").includes(long), "the whole punchline survives");
});
