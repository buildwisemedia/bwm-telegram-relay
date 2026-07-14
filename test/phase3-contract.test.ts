import assert from "node:assert/strict";
import test from "node:test";

import {
  enqueueDigestItem,
  legacyDigestPunchline,
  legacySuppressionReason,
  renderWire,
} from "../src/index.ts";

test("the canonical wrap-check is acknowledged without entering the digest", () => {
  assert.equal(legacySuppressionReason("wrap-check"), "phase3_wrap_check");
});

test("test and explicit no-action messages are suppressed", () => {
  assert.equal(legacySuppressionReason("[close-sweep gate 1 smoke — ignore]"), "test_or_smoke");
  assert.equal(legacySuppressionReason("Lane capacity changed. No action needed."), "explicit_no_action");
  assert.equal(legacySuppressionReason("A real client update"), null);
});

test("legacy dumps become short plain-English digest notes", () => {
  assert.equal(
    legacyDigestPunchline("<b>BOB NEEDS A HUMAN REPLY</b>\nSlack ID C123 and command syntax"),
    "A client reply is waiting for human review in the work queue.",
  );
  assert.equal(
    legacyDigestPunchline("HUMAN TRIAGE — top 10 of 128 records"),
    "Bob has items waiting for team triage in the work queue.",
  );
});

test("keyed FYIs merge to one latest digest note", async () => {
  const rows = new Map<string, string>();
  const env = {
    BWM_TELEGRAM_KV: {
      put: async (key: string, value: string) => { rows.set(key, value); },
    },
  };
  await enqueueDigestItem(env as never, {
    wire_type: "fyi",
    punchline: "Client message reached the 30-minute mark",
    reason: "fyi",
    key: "feedback-sla:design2sell:item-1",
  });
  await enqueueDigestItem(env as never, {
    wire_type: "fyi",
    punchline: "Client message reached the 2-hour mark",
    reason: "fyi",
    key: "feedback-sla:design2sell:item-1",
  });
  assert.equal(rows.size, 1);
  assert.match([...rows.values()][0], /2-hour mark/);
});

test("live messages use plain-English labels and hide internal wire refs", () => {
  const text = renderWire({
    type: "fire",
    punchline: "Design2Sell has waited more than 48 hours for a reply",
    stakes: "The team response SLA has failed.",
    ask: "Choose who will send the client reply now.",
  }, "F-9X2QZ");
  assert.match(text, /<b>Urgent: Design2Sell has waited more than 48 hours for a reply<\/b>/);
  assert.doesNotMatch(text, /F-9X2QZ|\bFIRE\b/);
  assert.match(text, /Choose who will send the client reply now/);
});
