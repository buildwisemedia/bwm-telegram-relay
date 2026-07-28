#!/usr/bin/env bash
# deploy-verified.sh — deploy the relay and PROVE the deployed code is the intended code.
#
# "Deployed" is a claim wrangler makes. The proof is that GET /health reports the exact
# SHA we just built from. This script refuses to deploy anything it cannot prove:
#   · branch must be main            (a peer's branch must never reach the prod worker)
#   · working tree must be clean     (the worker ships the TREE, not the commit)
#   · HEAD must equal origin/main    (no unreviewed local commit goes live)
#   · GIT_SHA is injected at deploy  (--var GIT_SHA:<sha>)
#   · live /health.git_sha must match
#
# The receipt lands in scripts/.runtime/ (gitignored), so a deploy never dirties the
# tree and the clean-tree gate holds on the NEXT invocation too.
#
# Usage: ./scripts/deploy-verified.sh [--env capture]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WRANGLER_ENV=""
RELAY_DEFAULT="https://bwm-telegram-relay.robert-ba0.workers.dev"
if [ "${1:-}" = "--env" ] && [ -n "${2:-}" ]; then
  WRANGLER_ENV="$2"
  RELAY_DEFAULT="https://bwm-telegram-relay-${2}.robert-ba0.workers.dev"
fi
RELAY="${BWM_TELEGRAM_RELAY_URL:-$RELAY_DEFAULT}"

step_ok()   { echo "STEP:$1:OK${2:+ $2}"; }
step_fail() { echo "STEP:$1:FAIL${2:+ $2}"; exit 1; }

BRANCH="$(git branch --show-current)"
[ "$BRANCH" = "main" ] || step_fail "deploy-branch" "on '$BRANCH', not main — wrangler ships the WORKING TREE"
step_ok "deploy-branch" "main"

[ -z "$(git status --porcelain)" ] || step_fail "deploy-clean-tree" "working tree is dirty; the deployed bytes would not match any commit"
step_ok "deploy-clean-tree"

git fetch -q origin
SHA="$(git rev-parse HEAD)"
ORIGIN_SHA="$(git rev-parse origin/main)"
[ "$SHA" = "$ORIGIN_SHA" ] || step_fail "deploy-head-matches-origin" "HEAD $SHA != origin/main $ORIGIN_SHA"
step_ok "deploy-head-matches-origin" "$SHA"

if [ -n "$WRANGLER_ENV" ]; then
  npx wrangler deploy --env "$WRANGLER_ENV" --var "GIT_SHA:$SHA" || step_fail "deploy-run" "wrangler deploy failed"
else
  npx wrangler deploy --var "GIT_SHA:$SHA" || step_fail "deploy-run" "wrangler deploy failed"
fi
step_ok "deploy-run" "$RELAY"

# Cloudflare propagation is fast but not instantaneous; poll rather than sleep-and-hope.
LIVE=""
for _ in 1 2 3 4 5 6 7 8 9 10; do
  LIVE="$(curl -fsS "$RELAY/health" 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin).get("git_sha") or "")' 2>/dev/null || true)"
  [ "$LIVE" = "$SHA" ] && break
  sleep 3
done
[ "$LIVE" = "$SHA" ] || step_fail "deploy-sha-verified" "live /health.git_sha='$LIVE' != deployed '$SHA'"
step_ok "deploy-sha-verified" "$SHA"

mkdir -p "$ROOT/scripts/.runtime"
python3 - "$ROOT/scripts/.runtime/deploy-receipt.json" "$SHA" "$RELAY" "${WRANGLER_ENV:-production}" <<'PY'
import json, sys, datetime
json.dump({
    "last_deploy_sha": sys.argv[2],
    "last_deploy_ts": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "relay": sys.argv[3],
    "env": sys.argv[4],
}, open(sys.argv[1], "w"), indent=2)
PY
step_ok "deploy-receipt" "scripts/.runtime/deploy-receipt.json"

# The gate that makes this script honest: the receipt must not have dirtied the tree.
[ -z "$(git status --porcelain)" ] || step_fail "deploy-tree-still-clean" "the deploy dirtied the working tree"
step_ok "deploy-tree-still-clean"
