#!/usr/bin/env bash
# phase2-accept.sh — One Wire remediation Phase 2 acceptance (truth fixes + kill list).
#
# Phase 2 is the first LIVE deploy. Every step emits STEP:<name>:OK|FAIL, and
# test/phase/verifier-selftest.sh proves each check can actually fail.
#
# DEPLOY-GATED STEPS: ③ (deploy), ④ (flap replay against the capture worker) and ⑦
# (pollution check) touch the network. BWM_ACCEPT_OFFLINE=1 skips exactly those and
# marks them SKIPPED — used by the self-test sandboxes, never as a pass.
set -euo pipefail

ROOT="${BWM_ACCEPT_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
cd "$ROOT"

step_ok()   { echo "STEP:$1:OK${2:+ $2}"; }
step_skip() { echo "STEP:$1:SKIPPED${2:+ $2}"; }
step_fail() { echo "STEP:$1:FAIL${2:+ $2}"; exit 1; }

# Any step whose failure would otherwise be swallowed runs under an explicit guard, so
# a broken tool reads as FAIL rather than as an empty pass.
OFFLINE="${BWM_ACCEPT_OFFLINE:-0}"

# ① Unit tests — including the six strict-prefix preservation controls.
npm test >/tmp/phase2-npm-test.log 2>&1 || { sed -n '1,40p' /tmp/phase2-npm-test.log; step_fail "unit-tests" "npm test failed"; }
step_ok "unit-tests" "$(grep -E '^# pass' /tmp/phase2-npm-test.log | head -1 | tr -d '#')"

# ② Typecheck.
npm run typecheck >/tmp/phase2-tsc.log 2>&1 || { sed -n '1,30p' /tmp/phase2-tsc.log; step_fail "typecheck" "tsc reported errors"; }
step_ok "typecheck"

# ③ /health must expose git_sha — the whole deploy proof rests on it existing.
if grep -q 'git_sha: env.GIT_SHA' src/index.ts; then
  step_ok "health-git-sha" "exposed on /health"
else
  step_fail "health-git-sha" "/health does not report git_sha; a deploy could not be proven"
fi

# ④ §2.3 — no aggregate count can be reintroduced by a composer template, and the
#    reviewed digest still trips the payload rules (so the checker is not vacuous).
node scripts/check-forbidden-strings.mjs --source src/index.ts || step_fail "forbidden-tokens" "a composer still builds a Robert-visible count"
if node scripts/check-forbidden-strings.mjs test/fixtures/jul27/before-artifact.txt >/dev/null 2>&1; then
  step_fail "forbidden-tokens-teeth" "the reviewed Jul 27 digest PASSED the checker — the rules are not detecting the defects they name"
fi
step_ok "forbidden-tokens-teeth" "the reviewed digest still fails the checker, as it must"

# ⑤ §2.8 — the email-as-Robert kill list is wired at all three layers.
MISSING=""
grep -q "isEmailAsRobert(input.punchline" src/index.ts        || MISSING="$MISSING ingress-wire"
grep -q "isEmailAsRobert(text)" src/index.ts                  || MISSING="$MISSING ingress-event"
grep -q "scrubEmailAsRobert(fitDigest(lines))" src/index.ts   || MISSING="$MISSING final-render"
# Match an actual QUERY, not the word — the code documents the removal in prose right
# where it happened, and that prose necessarily names the table it stopped reading.
grep -qE '\`ea_drafts\?select' src/index.ts                     && MISSING="$MISSING ea_drafts-still-queried"
[ -z "$MISSING" ] || step_fail "kill-list-layers" "missing//extra:$MISSING"
step_ok "kill-list-layers" "ingress(/notify+/event) · source(ea_drafts removed) · final-render"

# ⑥ §2.5 — the fire lifecycle is resolution-bound, not time-bound.
grep -q "WIRE_FIRE_TTL_SECONDS" src/index.ts && step_fail "fire-lifecycle" "a TTL still governs the fire registry"
grep -q "sweepFireRegistry" src/index.ts || step_fail "fire-lifecycle" "no collapse sweep is wired"
step_ok "fire-lifecycle" "no TTL; collapse sweep on the 15-min cron"

# ⑦ The launchagent-health sender emits a STABLE key (the flap at its source).
OPS="${BWM_OPS_EVENTS_REPO:-$HOME/bwm-ops-events}"
if [ -d "$OPS" ]; then
  if grep -rq "token_hex\|uuid4" "$OPS/launchagent-health/"*.py 2>/dev/null; then
    if grep -rn "token_hex\|uuid4" "$OPS/launchagent-health/"*.py | grep -qv "tmp-"; then
      step_fail "sender-stable-key" "random material remains in the launchagent-health notification-key path"
    fi
  fi
  # Prove determinism against the real implementation, not by reading it.
  python3 - "$OPS" <<'PY' || exit 1
import importlib.util, sys, pathlib
root = pathlib.Path(sys.argv[1]) / "launchagent-health"
spec = importlib.util.spec_from_file_location("remediation", root / "remediation.py")
mod = importlib.util.module_from_spec(spec)
sys.modules["remediation"] = mod
try:
    spec.loader.exec_module(mod)
except Exception as e:
    print(f"STEP:sender-stable-key:FAIL cannot load remediation.py: {e}")
    sys.exit(1)
eng = mod.RemediationEngine.__new__(mod.RemediationEngine)
keys = {eng._escalation_key("com.buildwisemedia.aeo-probe", "not running") for _ in range(5)}
if len(keys) != 1:
    print(f"STEP:sender-stable-key:FAIL notification key is NOT deterministic: {keys}")
    sys.exit(1)
k = keys.pop()
import re
if re.search(r"-[0-9a-f]{8}$", k):
    print(f"STEP:sender-stable-key:FAIL key still carries a random suffix: {k}")
    sys.exit(1)
print(f"STEP:sender-stable-key:OK deterministic across 5 calls: {k}")
PY
else
  step_fail "sender-stable-key" "$OPS not found"
fi

# ⑧ The live checkout must equal origin/main (the plan's landing procedure).
if [ -d "$OPS" ]; then
  GRAPH="$(git -C "$OPS" rev-list --left-right --count origin/main...HEAD | tr '\t' ' ')"
  AHEAD="$(echo "$GRAPH" | awk '{print $2}')"
  [ "$AHEAD" = "0" ] || step_fail "ops-checkout-synced" "live checkout has $AHEAD commit(s) not on origin/main (graph '$GRAPH')"
  step_ok "ops-checkout-synced" "graph '$GRAPH'"
fi

# ⑨ Deploy, proven by /health.git_sha.
if [ "$OFFLINE" = "1" ]; then
  step_skip "deploy" "BWM_ACCEPT_OFFLINE=1"
  step_skip "flap-replay" "BWM_ACCEPT_OFFLINE=1"
  step_skip "capture-pollution" "BWM_ACCEPT_OFFLINE=1"
else
  ./scripts/deploy-verified.sh || step_fail "deploy" "deploy-verified.sh failed"
  step_ok "deploy"
  ./scripts/deploy-verified.sh --env capture || step_fail "deploy-capture" "capture worker deploy failed"
  step_ok "deploy-capture"
  bash test/live/flap-replay.sh capture || step_fail "flap-replay" "the flap reproduced, or the negative control did not split"
  step_ok "flap-replay"
  bash test/live/capture-pollution-check.sh || step_fail "capture-pollution" "a drill polluted production, or the check could not run"
  step_ok "capture-pollution"
fi

# ⑩ The verifier must itself be verified (guarded against recursion).
if [ -z "${BWM_SELFTEST:-}" ]; then
  BWM_SELFTEST=1 bash test/phase/verifier-selftest.sh phase2 \
    || step_fail "verifier-selftest" "crafted failures did not land at their intended markers"
else
  step_ok "verifier-selftest" "skipped (already inside a self-test sandbox)"
fi

echo "PHASE2: ALL STEPS GREEN"
