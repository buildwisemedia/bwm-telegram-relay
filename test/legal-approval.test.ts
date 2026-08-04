import { test } from "node:test";
import assert from "node:assert/strict";

import {
  buildRelayDecisionProof,
  legalApprovalChoice,
  parseLegalApprovalRequest,
  parseWireInput,
  renderWire,
} from "../src/index.ts";

const packetId = "synthetic-acme-mnda-2026-08-03-deadbeef";
const packetDigest = "a".repeat(64);
const requestDigest = "b".repeat(64);
const expectedToken = `APPROVE ${packetId} ${packetDigest.slice(0, 12)}`;

function approvalFields() {
  return {
    approval_packet_id: packetId,
    approval_packet_digest: packetDigest,
    approval_request_digest: requestDigest,
    approval_expected_token: expectedToken,
  };
}

test("legal approval metadata requires all fields and signoff type", () => {
  assert.deepEqual(parseLegalApprovalRequest({}, "signoff"), { ok: true });
  assert.equal(parseLegalApprovalRequest({ approval_packet_id: packetId }, "signoff").ok, false);
  assert.equal(parseLegalApprovalRequest(approvalFields(), "call").ok, false);
  const parsed = parseLegalApprovalRequest(approvalFields(), "signoff");
  assert.ok(parsed.ok && parsed.approval);
  assert.equal(parsed.approval?.expected_token, expectedToken);
});

test("approval token must bind full packet identity with 12 hex characters", () => {
  const short = { ...approvalFields(), approval_expected_token: `APPROVE ${packetId} ${packetDigest.slice(0, 8)}` };
  const wrongPacket = { ...approvalFields(), approval_expected_token: `APPROVE other ${packetDigest.slice(0, 12)}` };
  assert.equal(parseLegalApprovalRequest(short, "signoff").ok, false);
  assert.equal(parseLegalApprovalRequest(wrongPacket, "signoff").ok, false);
});

test("wire parser retains approval identity and renderer demands exact reply", () => {
  const parsed = parseWireInput({
    type: "signoff",
    punchline: "Approve synthetic packet?",
    ask: `Reply exactly: ${expectedToken}`,
    options: [expectedToken, `REJECT ${packetId}`],
    ...approvalFields(),
  });
  assert.ok(parsed.ok);
  if (!parsed.ok) return;
  assert.equal(parsed.input.legal_approval?.request_digest, requestDigest);
  const rendered = renderWire(parsed.input, "S-ABCDE");
  assert.match(rendered, new RegExp(expectedToken));
  assert.match(rendered, new RegExp(`REJECT ${packetId}`));
  assert.ok(!rendered.includes("1 ="), "numbered replies cannot authorize legal paper");
});

test("relay decision proof carries only relay-bound request and inbound evidence", () => {
  const parsed = parseLegalApprovalRequest(approvalFields(), "signoff");
  assert.ok(parsed.ok && parsed.approval);
  if (!parsed.ok || !parsed.approval) return;
  const proof = buildRelayDecisionProof(
    "S-ABCDE", parsed.approval, expectedToken,
    "telegram-inbound-synthetic", "telegram-reply", "2026-08-03T12:00:00Z",
  );
  assert.equal(proof.verified, true);
  assert.equal(proof.packet_digest, packetDigest);
  assert.equal(proof.request_digest, requestDigest);
  assert.equal(proof.choice_raw, expectedToken);
  assert.equal(proof.inbound_event_id, "telegram-inbound-synthetic");
});

test("only the exact legal approval or rejection token can seal a decision", () => {
  const parsed = parseLegalApprovalRequest(approvalFields(), "signoff");
  assert.ok(parsed.ok && parsed.approval);
  if (!parsed.ok || !parsed.approval) return;
  assert.equal(legalApprovalChoice(parsed.approval, expectedToken), "approve");
  assert.equal(legalApprovalChoice(parsed.approval, `REJECT ${packetId}`), "reject");
  for (const invalid of (
    ["1", "yes", "approve", `${expectedToken.slice(0, -1)}0`, `APPROVE ${packetId} ${packetDigest.slice(0, 8)}`]
  )) {
    assert.equal(legalApprovalChoice(parsed.approval, invalid), null);
  }
});
