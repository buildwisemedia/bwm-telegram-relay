#!/usr/bin/env bash
# phase1-accept.sh — One Wire remediation Phase 1 acceptance (facts + fixtures).
# Behaviour-change-free phase: nothing here deploys, sends, or mutates production.
#
# Every step emits STEP:<name>:OK|FAIL. test/phase/verifier-selftest.sh feeds this
# script crafted failures and asserts each one lands at its INTENDED marker — a
# verifier that cannot fail is not a verifier.
#
# DEVIATION FROM plan v6 §3 (orchestrator-authorized, 2026-07-28): step ⑤
# `verify-spec-version.sh` and the `spec_expected_version` pin are OUT OF SCOPE.
# Phase 2 changes no contract semantics; the Brain spec amendment gates the Phase 5
# ENFORCE flip, not this phase. No Brain write is performed by this phase.
set -euo pipefail

ROOT="${BWM_ACCEPT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$ROOT"

step_ok()   { echo "STEP:$1:OK${2:+ $2}"; }
step_fail() { echo "STEP:$1:FAIL${2:+ $2}"; exit 1; }

# ① Facts still hold (git graph · projector probe · notifier identity · live schema).
bash scripts/refresh-facts.sh --assert || step_fail "facts-assert" "refresh-facts --assert returned nonzero"

# ② Fixture manifest + byte-exact before-artifact.
node scripts/capture-jul27-fixtures.mjs --verify || step_fail "fixtures-verify" "capture --verify returned nonzero"
step_ok "fixtures-verify"

# ③ Delivery-state map: at least one state, and NO non-final state carries a phrase.
if jq -e '(.states|length) > 0 and ([.states[]|select(.final==false and .phrase != null)]|length) == 0' \
     scripts/delivery-state-map.json >/dev/null; then
  step_ok "state-map" "$(jq -r '[.states[]|select(.final)|.state]|join(",")' scripts/delivery-state-map.json) are final"
else
  step_fail "state-map" "a non-final delivery_state carries a phrase, or the map is empty"
fi

# ④ The "no schema changes anywhere in this project" claim, grounded for the FULL
#    claim lifecycle: BOTH responder columns must already exist live.
if jq -e '(.telegram_inbound_columns | index("responder_status") != null)
       and (.telegram_inbound_columns | index("responder_claimed_at") != null)' \
     scripts/pinned-facts.json >/dev/null; then
  step_ok "no-migration-claim" "responder_status + responder_claimed_at both pre-exist"
else
  step_fail "no-migration-claim" "pinned facts do not carry both responder columns"
fi

# ⑤ (skipped — see DEVIATION note above: spec-version pin belongs to the Phase 5 gate)

# ⑥ Deploy receipts genuinely never dirty the tree.
if git check-ignore -q scripts/.runtime/deploy-receipt.json; then
  step_ok "runtime-ignored"
else
  step_fail "runtime-ignored" "scripts/.runtime/deploy-receipt.json is NOT gitignored"
fi

# ⑦ Immutability: pinned-facts.json must carry ONLY the Phase 1 immutable set —
#    a per-deploy value leaking in here is what re-creates the circular dependency.
if jq -e 'has("last_deploy_ts") or has("last_deploy_sha") or has("approval_pending_cmd") or has("approval_id_jq")' \
     scripts/pinned-facts.json >/dev/null 2>&1; then
  step_fail "facts-immutable" "a per-deploy or deleted pin leaked into scripts/pinned-facts.json"
fi
step_ok "facts-immutable"

# ⑧ The verifier must itself be verified. Guarded against recursion: the self-test
#    RUNS this script inside sandboxes, so it sets BWM_SELFTEST=1 to stop the nesting.
if [ -z "${BWM_SELFTEST:-}" ]; then
  BWM_SELFTEST=1 bash test/phase/verifier-selftest.sh phase1 \
    || step_fail "verifier-selftest" "crafted failures did not land at their intended markers"
else
  step_ok "verifier-selftest" "skipped (already inside a self-test sandbox)"
fi

echo "PHASE1: ALL STEPS GREEN"
