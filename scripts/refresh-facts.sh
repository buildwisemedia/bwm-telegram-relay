#!/usr/bin/env bash
# refresh-facts.sh — One Wire remediation Phase 1 fact pinner.
#
#   ./scripts/refresh-facts.sh            # survey live sources, WRITE pinned-facts.json
#   ./scripts/refresh-facts.sh --assert   # verify the committed facts still hold (read-only)
#
# The immutable fact set (plan v6 §2.11) lives in scripts/pinned-facts.json and is
# written by this script ONCE, in Phase 1. Per-deploy receipts are runtime-only and
# live in scripts/.runtime/deploy-receipt.json (gitignored) so a deploy never dirties
# the tree and deploy-verified.sh's clean-tree gate holds on every invocation.
#
# Every step emits STEP:<name>:OK|FAIL so test/phase/verifier-selftest.sh can assert
# that a crafted failure lands at the INTENDED step.
set -euo pipefail

MODE="${1:-}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FACTS="$REPO_ROOT/scripts/pinned-facts.json"
OPS_REPO="${BWM_OPS_EVENTS_REPO:-$HOME/bwm-ops-events}"
RELAY_NOTIFIER="$REPO_ROOT/bin/bwm-robert-notify"
INSTALLED_NOTIFIER="${BWM_INSTALLED_NOTIFIER:-$HOME/.local/bin/bwm-robert-notify}"
RESPONDER_RUNSH="${BWM_RESPONDER_RUNSH:-$OPS_REPO/telegram-responder/run.sh}"

step_ok()   { echo "STEP:$1:OK${2:+ $2}"; }
step_fail() { echo "STEP:$1:FAIL${2:+ $2}"; exit 1; }

# ── 1. Git graph of the live ops-events checkout ─────────────────────────────
# The Phase 2 run.py landing procedure is `git merge --ff-only origin/main` on the
# LIVE checkout. That is only safe when the checkout has NO commits of its own that
# are absent from origin/main (the right-hand count). A nonzero right count means a
# peer's work is sitting in the live tree — ABORT rather than fast-forward over it.
git -C "$OPS_REPO" fetch -q origin 2>/dev/null || true
GRAPH="$(git -C "$OPS_REPO" rev-list --left-right --count origin/main...HEAD | tr '\t' ' ')"
GRAPH_AHEAD="$(echo "$GRAPH" | awk '{print $2}')"
GRAPH_BEHIND="$(echo "$GRAPH" | awk '{print $1}')"
if [ "$GRAPH_AHEAD" != "0" ]; then
  step_fail "git-graph" "ops-events checkout has $GRAPH_AHEAD commit(s) not on origin/main (graph '$GRAPH'); --ff-only would be unsafe"
fi
step_ok "git-graph" "origin/main...HEAD = '$GRAPH' (behind=$GRAPH_BEHIND, ahead=0)"

# ── 2. Projector probe: the PROJECTOR must NOT call the relay's /event route ──
# plan v6 §2.11 records "projector does not call /event". Re-prove it, don't assume.
#
# SCOPE (verified 2026-07-28): the only /event callers in bwm-ops-events are the
# Supabase fan-out TRIGGERS in migrations/*.sql (net.http_post from the DB) — that is
# the route's designed caller and is NOT the projector. The probe is therefore scoped
# to the projector component itself; a hit there would mean a second, unaudited
# ingress path into the wire.
PROJECTOR_DIR="$OPS_REPO/bwm-event-projector"
PROJECTOR_HITS=0
if [ -d "$PROJECTOR_DIR" ]; then
  # `|| true` on the grep only: with pipefail set, grep's "no matches" exit 1 —
  # the GREEN outcome here — would otherwise abort the script mid-survey.
  PROJECTOR_HITS="$( { grep -rIl --exclude-dir=.git --exclude-dir=node_modules \
    -e 'telegram-relay[^"]*/event' "$PROJECTOR_DIR" 2>/dev/null || true; } | wc -l | tr -d ' ')"
else
  step_fail "projector-probe" "projector component not found at $PROJECTOR_DIR"
fi
if [ "$PROJECTOR_HITS" != "0" ]; then
  step_fail "projector-probe" "$PROJECTOR_HITS file(s) in $PROJECTOR_DIR post to the relay /event route"
fi
step_ok "projector-probe" "no /event callers in bwm-event-projector"

# ── 3. Notifier copies identical ─────────────────────────────────────────────
if [ ! -f "$INSTALLED_NOTIFIER" ]; then
  step_fail "cli-identical" "installed notifier missing at $INSTALLED_NOTIFIER"
fi
if ! cmp -s "$RELAY_NOTIFIER" "$INSTALLED_NOTIFIER"; then
  step_fail "cli-identical" "$RELAY_NOTIFIER differs from $INSTALLED_NOTIFIER"
fi
step_ok "cli-identical" "$(shasum -a 256 "$RELAY_NOTIFIER" | awk '{print $1}')"

# ── 4. Supabase-backed facts ─────────────────────────────────────────────────
# Credentials resolve exactly the way the real log-event CLI does. Fail CLOSED if
# neither name is present — every acceptance script runs `set -euo pipefail`, so an
# unset variable would abort before the query ran and read as a pass.
if [ -f "$HOME/.bwm_secrets/log-event.env" ]; then
  set -a; . "$HOME/.bwm_secrets/log-event.env"; set +a
fi
SB_KEY="${SUPABASE_SECRET_KEY:-${SUPABASE_SERVICE_ROLE_KEY:-}}"
if [ -z "${SUPABASE_URL:-}" ] || [ -z "$SB_KEY" ]; then
  step_fail "supabase-creds" "no SUPABASE_URL / SUPABASE_SECRET_KEY|SUPABASE_SERVICE_ROLE_KEY"
fi
step_ok "supabase-creds"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# telegram_inbound live column list — the "no schema changes anywhere in this
# project" claim is asserted against THIS, for BOTH responder columns the claim
# lifecycle needs (responder_status AND responder_claimed_at).
curl -fsS "$SUPABASE_URL/rest/v1/telegram_inbound?select=*&limit=1" \
  -H "apikey: $SB_KEY" -H "Authorization: Bearer $SB_KEY" > "$TMP/inbound.json" \
  || step_fail "inbound-schema" "telegram_inbound select failed"
COLS="$(python3 -c '
import json,sys
r=json.load(open(sys.argv[1]))
print(json.dumps(sorted(r[0].keys()) if r else []))' "$TMP/inbound.json")"
echo "$COLS" | grep -q '"responder_status"'     || step_fail "inbound-schema" "responder_status missing"
echo "$COLS" | grep -q '"responder_claimed_at"' || step_fail "inbound-schema" "responder_claimed_at missing"
step_ok "inbound-schema" "both responder columns present"

# 90-day distinct delivery_state survey — the input to scripts/delivery-state-map.json.
SINCE="$(python3 -c 'import datetime;print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=90)).isoformat().replace("+00:00","Z"))')"
curl -fsS "$SUPABASE_URL/rest/v1/operational_events?event_type=eq.build.shipped&occurred_at=gte.$SINCE&select=payload&limit=5000" \
  -H "apikey: $SB_KEY" -H "Authorization: Bearer $SB_KEY" > "$TMP/shipped.json" \
  || step_fail "delivery-state-survey" "build.shipped query failed"
STATES="$(python3 -c '
import json,sys
rows=json.load(open(sys.argv[1]))
vals=set()
for r in rows:
    p=r.get("payload") or {}
    v=p.get("delivery_state")
    if isinstance(v,str) and v:
        vals.add(v)
print(json.dumps(sorted(vals)))' "$TMP/shipped.json")"
step_ok "delivery-state-survey" "$STATES"

# responder WINDOW_HOURS, read from the real daemon.
WINDOW_HOURS=""
if [ -f "$RESPONDER_RUNSH" ]; then
  # Real shape: WINDOW_HOURS="${RESPONDER_WINDOW_HOURS:-24}"  (run.sh:35)
  WINDOW_HOURS="$(sed -n 's/^[[:space:]]*WINDOW_HOURS="\${RESPONDER_WINDOW_HOURS:-\([0-9][0-9]*\)}".*/\1/p' "$RESPONDER_RUNSH" | head -1)"
fi
[ -n "$WINDOW_HOURS" ] || WINDOW_HOURS="null"
step_ok "responder-window" "$WINDOW_HOURS"

MANIFEST="$REPO_ROOT/test/fixtures/jul27/manifest.json"
[ -f "$MANIFEST" ] || step_fail "fixture-manifest" "missing $MANIFEST"
MANIFEST_SHA="$(shasum -a 256 "$MANIFEST" | awk '{print $1}')"
step_ok "fixture-manifest" "$MANIFEST_SHA"

# ── 5. Write or assert ───────────────────────────────────────────────────────
if [ "$MODE" = "--assert" ]; then
  [ -f "$FACTS" ] || step_fail "facts-assert" "missing $FACTS"
  python3 - "$FACTS" "$COLS" "$STATES" "$WINDOW_HOURS" "$MANIFEST_SHA" <<'PY' || exit 1
import json, sys
facts = json.load(open(sys.argv[1]))
cols = json.loads(sys.argv[2]); states = json.loads(sys.argv[3])
window = sys.argv[4]; msha = sys.argv[5]
fail = []
# SCOPE: this script owns LIVE-drift detection only. The claim "pinned facts assert
# both responder columns" has exactly ONE owner — phase1-accept.sh step
# `no-migration-claim`. Duplicating it here made the two checks indistinguishable in
# the verifier self-test (a mutation could not isolate either), which is precisely the
# failure mode a self-test exists to expose.
for c in ("responder_status", "responder_claimed_at"):
    if c not in cols:
        fail.append(f"LIVE telegram_inbound missing {c}")
missing = [s for s in facts.get("delivery_state_values", []) if s not in states]
if missing:
    fail.append(f"pinned delivery_state values no longer observed: {missing}")
new = [s for s in states if s not in facts.get("delivery_state_values", [])]
if new:
    fail.append(f"NEW delivery_state values observed, map may be stale: {new}")
pw = facts.get("responder_window_hours")
if str(pw) != window and not (pw is None and window == "null"):
    fail.append(f"responder_window_hours drift: pinned={pw} live={window}")
if facts.get("fixture_manifest_sha") != msha:
    fail.append(f"fixture_manifest_sha drift: pinned={facts.get('fixture_manifest_sha')} live={msha}")
if fail:
    for f in fail:
        print("STEP:facts-assert:FAIL " + f)
    sys.exit(1)
print("STEP:facts-assert:OK")
PY
  echo "refresh-facts: ASSERT GREEN"
  exit 0
fi

python3 - "$FACTS" "$COLS" "$STATES" "$WINDOW_HOURS" "$MANIFEST_SHA" "$GRAPH" <<'PY'
import json, sys
path = sys.argv[1]
out = {
    "_comment": (
        "One Wire remediation Phase 1 immutable facts (plan v6 §2.11). Written ONCE by "
        "scripts/refresh-facts.sh; re-verified read-only by --assert. Per-deploy values "
        "live in scripts/.runtime/deploy-receipt.json (gitignored) and are NEVER stored here."
    ),
    "telegram_inbound_columns": json.loads(sys.argv[2]),
    "delivery_state_values": json.loads(sys.argv[3]),
    "responder_window_hours": (None if sys.argv[4] == "null" else int(sys.argv[4])),
    "fixture_manifest_sha": sys.argv[5],
    "ops_events_graph_at_pin": sys.argv[6],
}
with open(path, "w") as fh:
    json.dump(out, fh, indent=2, sort_keys=True)
    fh.write("\n")
print("STEP:facts-write:OK " + path)
PY
echo "refresh-facts: WROTE $FACTS"
