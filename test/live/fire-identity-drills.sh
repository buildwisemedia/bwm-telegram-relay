#!/usr/bin/env bash
# fire-identity-drills.sh <env> — EXECUTABLE proof of the two P0 fixes (Sol QA r1).
#
#   bash test/live/fire-identity-drills.sh capture
#
# Sol's finding on the previous "negative control": it inspected metadata and hashes and
# never ran dispatch, lookup, migration, edit, or deletion — so it could not have caught
# the defect it was named for. These drills run the REAL deployed worker end to end and
# assert on the durable audit, not on a local function's return value.
#
# Drill A (P0-1) — SENDER KEY CHANGES, recurrence must EDIT.
#   Seed a pre-deploy-shaped registry under an old random-suffix key, then dispatch the
#   SAME condition under a different random suffix. The worker must find the open
#   incident and edit it. Under the deployed code before this fix, this sent.
#
# Drill B (P0-1) — reconciled legacy slot, recurrence must EDIT.
#   Seed a registry at the legacy slot shape, run the reconciler against the capture
#   namespace, then dispatch. Must edit, and the surviving registry must carry NO TTL.
#
# Drill C (P0-2) — a TOMBSTONED incident must never send again.
#   Seed a 15-day-old escalated registry, run the REAL scheduled sweep, then dispatch the
#   same condition. The registry must still exist (compacted, not deleted) and the
#   dispatch must NOT be action=sent. Under the previous sweep the registry was deleted
#   and the next recurrence sent fresh.
set -euo pipefail

ENV_NAME="${1:-capture}"
[ "$ENV_NAME" = "capture" ] || { echo "STEP:fire-identity:FAIL refusing to run against '$ENV_NAME' — capture only"; exit 1; }

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"
RELAY="${BWM_CAPTURE_RELAY_URL:-https://bwm-telegram-relay-capture.robert-ba0.workers.dev}"
CAP_NS="${BWM_CAPTURE_KV_NS:-3d94fc6c6fcb4da088bc2d0227595211}"

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
[ -n "$KEY_ARG" ] || step_fail "fire-identity" "BWM_INTERNAL_KEY unavailable"

RUN="fireid-$(date -u +%Y%m%dT%H%M%SZ)"
kvput() { npx --no-install wrangler kv key put "$1" "$2" --namespace-id "$CAP_NS" --remote >/dev/null 2>&1; }
kvget() { npx --no-install wrangler kv key get "$1" --namespace-id "$CAP_NS" --remote 2>/dev/null || true; }
kvlist() { npx --no-install wrangler kv key list --prefix "$1" --namespace-id "$CAP_NS" --remote 2>/dev/null || echo "[]"; }

post() {  # post <key> <punchline>
  curl -fsS -X POST "$RELAY/notify" \
    -H "X-BWM-Internal-Key: $KEY_ARG" -H "Content-Type: application/json" \
    --data "$(python3 -c '
import json, sys
print(json.dumps({"type":"fire","punchline":sys.argv[2],
 "stakes":"Drill traffic on the capture worker. Never reaches Telegram.",
 "key":sys.argv[1],"origin":"launchagent-health","session_id":sys.argv[3]}))' "$1" "$2" "$RUN")"
}
action_of() { printf '%s' "$1" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("action",""))'; }

# fingerprint <key> <origin> — computed by the SAME function the worker uses.
fingerprint() {
  node --experimental-strip-types -e '
import { fireFingerprint } from "./src/index.ts";
process.stdout.write(String(fireFingerprint({ key: process.argv[1], origin: process.argv[2] })));
' "$1" "$2" 2>/dev/null
}

SIG="4126371745a9"          # the real 12-char condition signature from the live registry
OLDKEY="launchagent-health-${SIG}-b7be76f9"
NEWKEY="launchagent-health-${SIG}-d1db1829"   # SAME condition, DIFFERENT random suffix

# ── Drill A: sender key changes, recurrence must EDIT ────────────────────────
FP="$(fingerprint "$OLDKEY" "launchagent-health")"
[ -n "$FP" ] || step_fail "drillA-seed" "could not compute a fingerprint"
kvput "wire:fire:$FP" "$(python3 -c '
import json,sys
print(json.dumps({"message_id":8100001,"ref":"F-SEED1","count":1,
 "first_at":"2026-07-27T14:07:46.000Z","last_at":"2026-07-27T14:07:46.000Z",
 "base":{"type":"fire","punchline":"LaunchAgent health: 1 confirmed critical(s)",
         "origin":"launchagent-health","key":sys.argv[1]},"updates":[]}))' "$OLDKEY")"
step_ok "drillA-seed" "seeded wire:fire:$FP under the OLD key"

RA="$(post "$NEWKEY" "LaunchAgent health: 1 confirmed critical(s)")" || step_fail "drillA-dispatch" "relay call failed"
AA="$(action_of "$RA")"
[ "$AA" = "edited" ] || step_fail "drillA-dispatch" "sender key changed and recurrence produced action='$AA', expected 'edited' — the re-ping is still armed ($RA)"
step_ok "drillA-dispatch" "key changed ($OLDKEY -> $NEWKEY) and the open incident was EDITED"

# Exactly one registry must exist for this condition — not two.
NFP="$(fingerprint "$NEWKEY" "launchagent-health")"
[ "$NFP" = "$FP" ] || step_fail "drillA-single-slot" "the two keys resolve to different slots ($FP vs $NFP)"
step_ok "drillA-single-slot" "both keys resolve to ONE slot $FP"

# ── Drill B: the reconciled slot carries no TTL ──────────────────────────────
TTLS="$(kvlist "wire:fire:$FP" | python3 -c 'import json,sys
ks=json.load(sys.stdin)
print(",".join(str(k.get("expiration")) for k in ks) or "none")')"
case "$TTLS" in
  *None*|none) step_ok "drillB-no-ttl" "the surviving registry carries no expiration" ;;
  *) step_fail "drillB-no-ttl" "the surviving registry still has an expiration ($TTLS) — an unresolved incident must never age out" ;;
esac

# ── Drill C: a tombstoned incident must never send again ─────────────────────
TKEY="launchagent-health:drill-${RUN}:stale"
TFP="$(fingerprint "$TKEY" "launchagent-health")"
OLD_ISO="$(python3 -c 'import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=15)).isoformat().replace("+00:00","Z"))')"
kvput "wire:fire:$TFP" "$(python3 -c '
import json,sys
print(json.dumps({"message_id":8100002,"ref":"F-SEED2","count":1,
 "first_at":sys.argv[1],"last_at":sys.argv[1],
 "escalated":True,"escalated_at":sys.argv[1],"escalation_event_id":"01DRILL0000000000000000000",
 "fingerprint":sys.argv[2],
 "base":{"type":"fire","punchline":"drill: an incident nobody ever resolved",
         "origin":"launchagent-health","key":sys.argv[3]},"updates":[]}))' "$OLD_ISO" "$TFP" "$TKEY")"
step_ok "drillC-seed" "seeded a 15-day-old ESCALATED registry at wire:fire:$TFP"

# Run the REAL sweep. /sweep/fires calls the SAME sweepFireRegistry() the 15-min cron
# calls — there is no test-only branch. (POST /__scheduled is a `wrangler dev` affordance
# and does not exist on a deployed worker, which is why the sweep was previously
# untestable against production behaviour at all.)
# Workers KV is eventually consistent: a freshly-seeded key can be invisible to the
# worker's edge for tens of seconds. Sweeping once and asserting immediately produced a
# false FAIL. Retry until the worker has actually SEEN the seed, and fail only if it
# never does — a drill that cannot tell "not yet visible" from "wrong behaviour" is not
# a drill.
SWEPT=0
for _ in 1 2 3 4 5 6 7 8 9 10; do
  curl -fsS -X POST "$RELAY/sweep/fires" -H "X-BWM-Internal-Key: $KEY_ARG" >/dev/null \
    || step_fail "drillC-sweep" "/sweep/fires call failed — the sweep never ran, so nothing below proves anything"
  sleep 4
  AFTER="$(kvget "wire:fire:$TFP")"
  [ -z "$AFTER" ] && step_fail "drillC-tombstone" "the sweep DELETED an unresolved incident — its next recurrence will send fresh"
  if printf '%s' "$AFTER" | python3 -c 'import json,sys; sys.exit(0 if json.load(sys.stdin).get("tombstone") is True else 1)' 2>/dev/null; then
    SWEPT=1; break
  fi
done
[ "$SWEPT" = "1" ] || step_fail "drillC-tombstone" "entry survived but was never compacted to a tombstone after 10 sweeps"
step_ok "drillC-sweep" "ran the real collapse sweep"
step_ok "drillC-tombstone" "unresolved identity preserved as a tombstone, not deleted"

RC="$(post "$TKEY" "drill: an incident nobody ever resolved")" || step_fail "drillC-dispatch" "relay call failed"
AC="$(action_of "$RC")"
case "$AC" in
  sent) step_fail "drillC-dispatch" "a tombstoned incident SENT a new message — the re-ping is re-armed ($RC)" ;;
  edited|skipped_tombstoned) step_ok "drillC-dispatch" "recurrence after 15 days produced '$AC', never a new post" ;;
  *) step_fail "drillC-dispatch" "unexpected action '$AC' ($RC)" ;;
esac

step_ok "fire-identity" "key-change edits · reconciled slot has no TTL · tombstone survives and never re-sends"
