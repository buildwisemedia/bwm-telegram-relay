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

# ── 0. CANARY: prove the query mechanism actually sees data ─────────────────
# Without this, every "0 rows" below could mean "the credentials point at nothing" and
# the whole check would pass by being blind. The canary asks for rows that MUST exist.
CANARY="$(curl -fsS "$SUPABASE_URL/rest/v1/telegram_outbound?created_at=gte.$SINCE&select=id&limit=5" \
  -H "apikey: $SB_KEY" -H "Authorization: Bearer $SB_KEY" 2>/dev/null)" \
  || step_fail "capture-pollution" "CANARY query failed — this check is blind, not clean"
N_CANARY="$(printf '%s' "$CANARY" | python3 -c 'import json,sys
try:
    print(len(json.load(sys.stdin)))
except Exception:
    print(-1)')"
if [ "$N_CANARY" -lt 1 ] 2>/dev/null || [ "$N_CANARY" = "-1" ]; then
  step_fail "capture-pollution" "CANARY saw $N_CANARY rows in the last ${WINDOW_HOURS}h — the queries are not reaching live data, so a clean result proves nothing"
fi
echo "    canary (must be >0): $N_CANARY"

# ── 1. NO operational_events row from a drill ────────────────────────────────
# This is the real pollution risk: operational_events drives the fan-out trigger into
# command_tasks / comms_log / command_alerts / deliverables. A single leaked row there
# creates downstream work. emitOperationalEvent diverts to KV in capture mode, and the
# three legacy direct writers are sealed by canWriteOperationalEventsDirectly().
q "capture-session events" \
  "operational_events?occurred_at=gte.$SINCE&session_id=like.*capture*&select=id,session_id&limit=20"
q "fixture-harness events" \
  "operational_events?occurred_at=gte.$SINCE&session_id=like.*fixture*&select=id,session_id&limit=20"
q "flap-drill events" \
  "operational_events?occurred_at=gte.$SINCE&session_id=like.flap-*&select=id,session_id&limit=20"
q "capture heartbeats" \
  "operational_events?occurred_at=gte.$SINCE&event_type=eq.daemon.heartbeat&payload->>run_kind=eq.capture&select=id&limit=20"

# ── 2. Every drill outbound row MUST be stamped capture=true ─────────────────
# telegram_outbound rows from the capture worker are EXPECTED — they are the audit
# evidence flap-replay reads, and plan v6 §2.10 excludes metadata.capture=true rows from
# every denominator. What must never happen is an UNSTAMPED drill row: that one would
# silently enter the denominators it is supposed to be excluded from.
UNSTAMPED="$(curl -fsS "$SUPABASE_URL/rest/v1/telegram_outbound?created_at=gte.$SINCE&metadata->wire->>origin=eq.fixture-harness&metadata->>capture=not.eq.true&select=id,source_route,metadata&limit=20" \
  -H "apikey: $SB_KEY" -H "Authorization: Bearer $SB_KEY" 2>/dev/null)" \
  || step_fail "capture-pollution" "unstamped-drill-row query FAILED — cannot prove non-pollution"
N_UNSTAMPED="$(printf '%s' "$UNSTAMPED" | python3 -c 'import json,sys
try:
    print(len(json.load(sys.stdin)))
except Exception:
    print(-1)')"
[ "$N_UNSTAMPED" = "-1" ] && step_fail "capture-pollution" "unstamped-drill-row response unparseable"
if [ "$N_UNSTAMPED" != "0" ]; then
  echo "    $(printf '%s' "$UNSTAMPED" | head -c 400)"
  step_fail "capture-pollution" "$N_UNSTAMPED drill outbound row(s) are NOT stamped capture=true"
fi
echo "    unstamped drill outbound rows: 0"

# ── 3. No drill row was ever actually delivered to Telegram ─────────────────
# The capture worker short-circuits Telegram I/O, so every drill row carries a synthetic
# response. A row with a real Telegram response would mean a drill reached Robert.
REAL="$(curl -fsS "$SUPABASE_URL/rest/v1/telegram_outbound?created_at=gte.$SINCE&metadata->>capture=eq.true&telegram_response->>capture=is.null&status=eq.sent&select=id&limit=20" \
  -H "apikey: $SB_KEY" -H "Authorization: Bearer $SB_KEY" 2>/dev/null)" \
  || step_fail "capture-pollution" "delivered-drill query FAILED — cannot prove non-delivery"
N_REAL="$(printf '%s' "$REAL" | python3 -c 'import json,sys
try:
    print(len(json.load(sys.stdin)))
except Exception:
    print(-1)')"
[ "$N_REAL" = "-1" ] && step_fail "capture-pollution" "delivered-drill response unparseable"
[ "$N_REAL" = "0" ] || step_fail "capture-pollution" "$N_REAL drill row(s) reached the real Telegram API"
echo "    drill rows delivered to Telegram: 0"

step_ok "capture-pollution" "no drill event rows, no unstamped drill rows, nothing delivered (last ${WINDOW_HOURS}h)"
