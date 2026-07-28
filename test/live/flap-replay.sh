#!/usr/bin/env bash
# flap-replay.sh <env> — replay the LaunchAgent flap against the live relay.
#
#   bash test/live/flap-replay.sh capture
#
# This is the end-to-end proof for finding 10 and plan v6 §2.5. The reviewed surface
# showed the SAME unresolved alert posted twice — at 20:36 and again at 05:05 — under a
# message that literally reads "Updates will edit this message — no re-pings."
#
# The drill dispatches the same fire TWICE with the stable key the fixed sender now
# emits, and asserts the relay produced exactly:
#     one action="sent" + one action="edited", against ONE telegram message id.
# Two "sent" rows, or two distinct message ids, is the flap reproducing.
#
# It also runs the NEGATIVE control: the same condition under the OLD random-suffix key
# shape must split into two messages — which is what proves the assertion has teeth and
# is not passing for some unrelated reason.
set -euo pipefail

ENV_NAME="${1:-capture}"
[ "$ENV_NAME" = "capture" ] || { echo "STEP:flap-replay:FAIL refusing to run against '$ENV_NAME' — capture only"; exit 1; }

RELAY="${BWM_CAPTURE_RELAY_URL:-https://bwm-telegram-relay-capture.robert-ba0.workers.dev}"
step_ok()   { echo "STEP:$1:OK${2:+ $2}"; }
step_fail() { echo "STEP:$1:FAIL${2:+ $2}"; exit 1; }

KEY_ARG="${BWM_INTERNAL_KEY:-}"
if [ -z "$KEY_ARG" ]; then
  KEY_ARG="$(python3 -c '
import json, os
try:
    print(json.load(open(os.path.expanduser("~/.claude/settings.json"))).get("env", {}).get("BWM_INTERNAL_KEY", ""))
except Exception:
    print("")')"
fi
[ -n "$KEY_ARG" ] || step_fail "flap-replay" "BWM_INTERNAL_KEY unavailable"

RUN="flap-$(date -u +%Y%m%dT%H%M%SZ)"

# post <key> <punchline> → the relay's JSON result
post() {
  curl -fsS -X POST "$RELAY/notify" \
    -H "X-BWM-Internal-Key: $KEY_ARG" -H "Content-Type: application/json" \
    --data "$(python3 -c '
import json, sys
print(json.dumps({
    "type": "fire",
    "punchline": sys.argv[2],
    "stakes": "Drill traffic on the capture worker. Never reaches Telegram.",
    "key": sys.argv[1],
    "origin": "fixture-harness",
    "session_id": sys.argv[3],
}))' "$1" "$2" "$RUN")"
}

action_of() { printf '%s' "$1" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("action",""))'; }

# ── The FIXED sender: one stable key, dispatched twice ───────────────────────
STABLE_KEY="flap-replay:$RUN:com.buildwisemedia.aeo-probe:not-running"
R1="$(post "$STABLE_KEY" "Scheduled job needs intervention: com.buildwisemedia.aeo-probe")" \
  || step_fail "flap-first-dispatch" "relay call failed"
A1="$(action_of "$R1")"
[ "$A1" = "sent" ] || step_fail "flap-first-dispatch" "expected action=sent, got '$A1' ($R1)"
step_ok "flap-first-dispatch" "sent"

# The pre-send claim carries a 120s TTL; a second dispatch inside it is correctly
# SKIPPED, not edited. Wait past it so the drill exercises the edit path it is about.
sleep 125

R2="$(post "$STABLE_KEY" "Scheduled job needs intervention: com.buildwisemedia.aeo-probe")" \
  || step_fail "flap-second-dispatch" "relay call failed"
A2="$(action_of "$R2")"
[ "$A2" = "edited" ] || step_fail "flap-second-dispatch" "expected action=edited (no re-ping), got '$A2' ($R2)"
step_ok "flap-second-dispatch" "edited"

# ── Assert against the durable audit, not just the HTTP replies ──────────────
if [ -f "$HOME/.bwm_secrets/log-event.env" ]; then set -a; . "$HOME/.bwm_secrets/log-event.env"; set +a; fi
SB_KEY="${SUPABASE_SECRET_KEY:-${SUPABASE_SERVICE_ROLE_KEY:-}}"
[ -n "${SUPABASE_URL:-}" ] && [ -n "$SB_KEY" ] || step_fail "flap-outbound-set" "no supabase credentials to read the audit"

ROWS="$(curl -fsS "$SUPABASE_URL/rest/v1/telegram_outbound?origin_session_id=eq.$RUN&select=id,status,metadata,telegram_message_id&order=created_at.asc" \
  -H "apikey: $SB_KEY" -H "Authorization: Bearer $SB_KEY")" || step_fail "flap-outbound-set" "audit query failed"

# The rows go to a FILE, not stdin: a heredoc-fed python already owns stdin, so
# `printf ... | python3 - <<PY` makes json.load read the SCRIPT and blow up.
ROWS_FILE="$(mktemp)"
printf '%s' "$ROWS" > "$ROWS_FILE"
python3 - "$ROWS_FILE" <<'PY' || { rm -f "$ROWS_FILE"; exit 1; }
import json, sys
rows = json.load(open(sys.argv[1]))
sent = [r for r in rows if ((r.get("metadata") or {}).get("wire") or {}).get("action") != "edited" and r.get("status") == "sent"]
edited = [r for r in rows if ((r.get("metadata") or {}).get("wire") or {}).get("action") == "edited"]
ids = {r.get("telegram_message_id") for r in rows if r.get("telegram_message_id")}
problems = []
if len(rows) != 2: problems.append(f"expected 2 outbound rows, got {len(rows)}")
if len(sent) != 1: problems.append(f"expected exactly 1 sent, got {len(sent)}")
if len(edited) != 1: problems.append(f"expected exactly 1 edited, got {len(edited)}")
if len(ids) != 1: problems.append(f"expected ONE telegram message id, got {sorted(ids)} — the flap reproduced")
fps = {((r.get("metadata") or {}).get("wire") or {}).get("fingerprint") for r in rows}
if len(fps) != 1 or None in fps: problems.append(f"expected ONE fingerprint on both rows, got {fps}")
if problems:
    for p in problems: print("STEP:flap-outbound-set:FAIL " + p)
    sys.exit(1)
print(f"STEP:flap-outbound-set:OK 1 sent + 1 edited, single message id {ids.pop()}, fingerprint {fps.pop()}")
PY
rm -f "$ROWS_FILE"

# ── NEGATIVE control: the OLD random-suffix key shape must still split ───────
# Without this, a passing assertion above could mean "the relay coalesces everything",
# which would be a different bug wearing the same green.
N1="$(post "flap-replay:$RUN:sig-$(python3 -c 'import secrets;print(secrets.token_hex(4))')" "negative control")" || true
N2="$(post "flap-replay:$RUN:sig-$(python3 -c 'import secrets;print(secrets.token_hex(4))')" "negative control")" || true
if [ "$(action_of "$N1")" = "sent" ] && [ "$(action_of "$N2")" = "sent" ]; then
  step_ok "flap-negative-control" "two random-suffix keys DO split into two sends (assertion has teeth)"
else
  step_fail "flap-negative-control" "random-suffix keys did not split — the coalescing assertion proves nothing (got '$(action_of "$N1")' / '$(action_of "$N2")')"
fi

step_ok "flap-replay" "no re-ping under a stable key; split preserved under a random one"
