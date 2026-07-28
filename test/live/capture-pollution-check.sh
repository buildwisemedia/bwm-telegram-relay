#!/usr/bin/env bash
# capture-pollution-check.sh — the standing proof that drills never touch production.
#
# Fails on ANY production hit AND on ANY query failure. A dead query must never read as
# clean: "we could not check" and "there is nothing there" are different facts, and only
# one of them is a pass.
#
# Checks, over a window that covers every drill this phase runs:
#   1. NO operational_events row whose session_id looks like a capture/drill session.
#   2. NO telegram_outbound row carrying metadata.capture = true.
#   3. NO telegram_outbound row produced by a fixture origin.
set -euo pipefail

WINDOW_HOURS="${CAPTURE_CHECK_WINDOW_HOURS:-24}"

step_ok()   { echo "STEP:$1:OK${2:+ $2}"; }
step_fail() { echo "STEP:$1:FAIL${2:+ $2}"; exit 1; }

if [ -f "$HOME/.bwm_secrets/log-event.env" ]; then
  set -a; . "$HOME/.bwm_secrets/log-event.env"; set +a
fi
SB_KEY="${SUPABASE_SECRET_KEY:-${SUPABASE_SERVICE_ROLE_KEY:-}}"
# Resolve credentials locally and fail CLOSED: every acceptance script runs with
# `set -u`, so an unset variable would abort before the query ran and could be
# mistaken for a clean result.
if [ -z "${SUPABASE_URL:-}" ] || [ -z "$SB_KEY" ]; then
  step_fail "capture-pollution" "no supabase credentials — cannot prove non-pollution"
fi

SINCE="$(python3 -c "import datetime,sys; print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(hours=int(sys.argv[1]))).isoformat().replace('+00:00','Z'))" "$WINDOW_HOURS")"

# q <label> <path> — curl -f makes any HTTP error a step failure, so a dead query
# cannot pass. jq is not assumed; python parses and counts.
q() {
  local label="$1" path="$2" out
  out="$(curl -fsS "$SUPABASE_URL/rest/v1/$path" -H "apikey: $SB_KEY" -H "Authorization: Bearer $SB_KEY" 2>/dev/null)" \
    || step_fail "capture-pollution" "$label query FAILED — cannot prove non-pollution"
  local n
  n="$(printf '%s' "$out" | python3 -c 'import json,sys
try:
    print(len(json.load(sys.stdin)))
except Exception:
    print(-1)')" || step_fail "capture-pollution" "$label response unparseable"
  [ "$n" = "-1" ] && step_fail "capture-pollution" "$label response unparseable"
  if [ "$n" != "0" ]; then
    echo "    offending rows: $(printf '%s' "$out" | head -c 400)"
    step_fail "capture-pollution" "$label found $n production row(s) — a drill polluted production"
  fi
  echo "    $label: 0"
}

q "capture-session events" \
  "operational_events?occurred_at=gte.$SINCE&session_id=like.*capture*&select=id,session_id&limit=20"
q "fixture-harness events" \
  "operational_events?occurred_at=gte.$SINCE&session_id=like.*fixture*&select=id,session_id&limit=20"
q "capture-stamped outbound" \
  "telegram_outbound?created_at=gte.$SINCE&metadata->>capture=eq.true&select=id,source_route&limit=20"
q "fixture-origin outbound" \
  "telegram_outbound?created_at=gte.$SINCE&metadata->wire->>origin=eq.fixture-harness&select=id,source_route&limit=20"

step_ok "capture-pollution" "no production rows from any drill in the last ${WINDOW_HOURS}h"
