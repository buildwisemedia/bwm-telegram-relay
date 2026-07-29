#!/usr/bin/env bash
# verifier-selftest.sh <phase> — prove the acceptance scripts can actually FAIL.
#
#   bash test/phase/verifier-selftest.sh phase1
#   bash test/phase/verifier-selftest.sh phase2
#
# For each crafted failure this builds an isolated sandbox copy of the repo, applies
# ONE mutation, runs the phase's acceptance script inside it, and asserts the run
# failed AND that the failure landed at the INTENDED step marker. A green acceptance
# run under a crafted failure is itself a FAIL: that would mean the check is decorative.
#
# The sandbox is a real copy (node_modules symlinked) so the acceptance script runs
# exactly the code it runs in CI, not a stub of it.
set -euo pipefail

PHASE="${1:-}"
[ -n "$PHASE" ] || { echo "usage: verifier-selftest.sh <phase1|phase2>" >&2; exit 64; }
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

PASS=0
FAILED=0

make_sandbox() {
  local dst="$1"
  mkdir -p "$dst"
  # -a preserves the exec bits the scripts rely on. node_modules is symlinked, not
  # copied: it is large, and the acceptance scripts must resolve the SAME toolchain.
  ( cd "$ROOT" && tar --exclude=node_modules --exclude=.wrangler --exclude=./.git -cf - . ) | tar -xf - -C "$dst"
  ln -sfn "$ROOT/node_modules" "$dst/node_modules"
  # A real git dir so `git check-ignore` behaves as it does in the repo.
  ( cd "$dst" && git init -q && git add -A >/dev/null 2>&1 || true )
}

# run_case <case-name> <expected-step> <mutation-fn>
run_case() {
  local name="$1" expected="$2" mutate="$3"
  local sb; sb="$(mktemp -d)"
  make_sandbox "$sb"
  ( cd "$sb" && "$mutate" "$sb" )
  local out rc=0
  # .selftest-env lets a mutation prepend a failing stub to PATH for exactly one
  # inner command, without touching any repo file.
  out="$(cd "$sb"; [ -f .selftest-env ] && . ./.selftest-env; \
        BWM_SELFTEST=1 BWM_ACCEPT_OFFLINE=1 BWM_ACCEPT_ROOT="$sb" bash "test/phase/${PHASE}-accept.sh" 2>&1)" || rc=$?
  rm -rf "$sb"

  if [ "$rc" -eq 0 ]; then
    echo "SELFTEST:$name:FAIL — acceptance script PASSED under a crafted failure (check is decorative)"
    FAILED=$((FAILED + 1)); return
  fi
  if printf '%s' "$out" | grep -q "STEP:${expected}:FAIL"; then
    echo "SELFTEST:$name:OK — failed at intended marker STEP:${expected}:FAIL"
    PASS=$((PASS + 1))
  else
    echo "SELFTEST:$name:FAIL — expected STEP:${expected}:FAIL, got:"
    printf '%s\n' "$out" | grep -E '^STEP:.*:FAIL' | sed 's/^/    /' || echo "    (no STEP:*:FAIL marker at all)"
    FAILED=$((FAILED + 1))
  fi
}

# ── Mutations ────────────────────────────────────────────────────────────────
# Truncate the manifest AND re-pin its sha, so the drift check in step ① passes and
# the mutation lands on the LENGTH check in step ② — proving the length rule is a real
# rule and not merely a side effect of the sha pin.
m_manifest_truncated() {
  printf '["f01","f02"]\n' > "$1/test/fixtures/jul27/manifest.json"
  local sha; sha="$(shasum -a 256 "$1/test/fixtures/jul27/manifest.json" | awk '{print $1}')"
  python3 - "$1/scripts/pinned-facts.json" "$sha" <<'PY'
import json, sys
p = sys.argv[1]; d = json.load(open(p)); d["fixture_manifest_sha"] = sys.argv[2]
json.dump(d, open(p, "w"), indent=2)
PY
}
m_artifact_mutated()   { printf 'MUTATED\n' >> "$1/test/fixtures/jul27/before-artifact.txt"; }
m_state_map_phrase()   {
  python3 - "$1/scripts/delivery-state-map.json" <<'PY'
import json, sys
p = sys.argv[1]; d = json.load(open(p))
for s in d["states"]:
    if not s["final"]:
        s["phrase"] = "{surface}: {summary} is live."   # the exact thing the lock forbids
        break
json.dump(d, open(p, "w"), indent=2)
PY
}
m_facts_missing_col()  {
  python3 - "$1/scripts/pinned-facts.json" <<'PY'
import json, sys
p = sys.argv[1]; d = json.load(open(p))
d["telegram_inbound_columns"] = [c for c in d["telegram_inbound_columns"] if c != "responder_claimed_at"]
json.dump(d, open(p, "w"), indent=2)
PY
}
m_gitignore_stripped() { grep -v 'scripts/.runtime/' "$1/.gitignore" > "$1/.gitignore.tmp"; mv "$1/.gitignore.tmp" "$1/.gitignore"; rm -f "$1/scripts/.runtime/.gitkeep"; }
m_facts_deploy_leak()  {
  python3 - "$1/scripts/pinned-facts.json" <<'PY'
import json, sys
p = sys.argv[1]; d = json.load(open(p))
d["last_deploy_sha"] = "deadbeef"   # the circular dependency r5 correction 3b removed
json.dump(d, open(p, "w"), indent=2)
PY
}
# A failing stub for ONE inner command, injected into PATH — proves the phase script
# surfaces an inner-tool failure at that tool's own step, not somewhere downstream.
m_stub_cmp() {
  mkdir -p "$1/.stub"
  printf '#!/bin/sh\nexit 1\n' > "$1/.stub/cmp"
  chmod +x "$1/.stub/cmp"
  printf 'export PATH="%s/.stub:$PATH"\n' "$1" >> "$1/.selftest-env"
}

# Valid TypeScript that reintroduces a Robert-visible count template. It must trip the
# SOURCE-mode rule, not the compiler — a mutation that merely breaks the build would
# prove nothing about the count gate.
m_forbidden_token()    { printf '\nexport const __selftestCount = (items: string[]) => `<b>Waiting on you (${items.length}):</b>`;\n' >> "$1/src/index.ts"; }
m_killlist_stripped()  { python3 - "$1/src/index.ts" <<'PY2'
import sys
p = sys.argv[1]; s = open(p).read()
s = s.replace("scrubEmailAsRobert(fitDigest(lines))", "({ text: fitDigest(lines), removed: 0 })")
open(p, "w").write(s)
PY2
}
m_fire_ttl_restored()  { python3 - "$1/src/index.ts" <<'PY2'
import sys
p = sys.argv[1]; s = open(p).read()
s = s.replace("const WIRE_FIRE_STALE_MS =", "const WIRE_FIRE_TTL_SECONDS = 86_400;\nconst WIRE_FIRE_STALE_MS =")
open(p, "w").write(s)
PY2
}
m_sender_random_key()  {
  # Point the check at a throwaway copy of the sender whose key is random again — the
  # real ops-events repo is never mutated by a self-test.
  local fake="$1/.fake-ops/launchagent-health"
  mkdir -p "$fake"
  cp "$HOME/bwm-ops-events/launchagent-health/remediation.py" "$fake/remediation.py"
  python3 - "$fake/remediation.py" <<'PY2'
import sys
p = sys.argv[1]; s = open(p).read()
a = 'return f"launchagent-health:{slug}:{kind}"[:180]'
# ASSERT the mutation landed. A silent no-op here produced an UNMUTATED copy, the check
# under test passed, and the case then died at a later step with no marker at all — a
# self-test that quietly stops testing anything is the exact failure mode this suite
# exists to prevent.
if s.count(a) != 1:
    sys.stderr.write(f"selftest mutation target not found ({s.count(a)} matches)\n")
    raise SystemExit(3)
open(p, "w").write(s.replace(a,
    'import secrets as _s\n        return f"launchagent-health:{slug}:{kind}-{_s.token_hex(4)}"[:180]'))
PY2
  [ $? -eq 0 ] || { echo "SELFTEST-SETUP:FAIL could not apply the sender-random-key mutation" >&2; return 1; }
  printf 'export BWM_OPS_EVENTS_REPO="%s/.fake-ops"\n' "$1" >> "$1/.selftest-env"
}
m_health_no_sha()      { python3 - "$1/src/index.ts" <<'PY'
import sys, re
p = sys.argv[1]; s = open(p).read()
s = s.replace("git_sha: env.GIT_SHA ?? null,", "// git_sha removed by selftest")
open(p, "w").write(s)
PY
}
m_break_tests()        { printf '\nthrow new Error("selftest-injected failure");\n' >> "$1/test/render-preservation.test.ts"; }
# The OTHER partial-read branch: the empty-but-partial line. No unit test reaches it
# (it needs zero visible items AND a failed source) and its shape is not a bold-header
# `.length`, so only executing the composer catches a count here.
m_reintroduce_count()  { python3 - "$1/src/index.ts" <<'PY2'
import sys
p = sys.argv[1]; s = open(p).read()
a = '      ? "<b>Inbox needs you:</b> some sources were unavailable this run \u2014 nothing visible in the rest."'
assert s.count(a) == 1, s.count(a)
b = ('      ? `<b>Inbox needs you:</b> ${[needsThreads, escalations, overdue].filter((x) => x === null).length}'
     ' sources unavailable this run \u2014 nothing visible in the rest.`')
open(p, "w").write(s.replace(a, b))
PY2
}
# Sol QA r2 bypass A: a genuine backlog line added INSIDE scorecardLines(). The first
# exemption was built from whatever that function returned, so the new line exempted
# itself and the checker reported OK.
m_scorecard_backlog()  { python3 - "$1/src/index.ts" <<'PY2'
import sys
p = sys.argv[1]; s = open(p).read()
a = '    `\u2022 Decisions answered: ${med !== null ? `median ${med.toFixed(1)}h over ${sc.decisions_total}` : "none answered this week"} \u00b7 ${sc.unanswered_refs} still unanswered`,'
assert s.count(a) == 1, s.count(a)
open(p, "w").write(s.replace(a, a + '\n    `<b>Waiting on you (${sc.unanswered_refs}):</b>`,'))
PY2
}
# Sol QA r2 bypass B: an inline-computed failed-source count, with an unused copy of the
# APPROVED literal left in the file. The checker searched the source for that literal and
# called the branch verified without ever running it.
m_inline_source_count() { python3 - "$1/src/index.ts" <<'PY2'
import sys
p = sys.argv[1]; s = open(p).read()
a = '    if (partial) inboxSection.push("<i>(some inbox sources were unavailable this run)</i>");'
assert s.count(a) == 1, s.count(a)
b = ('    const nFailed = [needsThreads, escalations, overdue].filter((x) => x === null).length;\n'
     '    const approvedLiteralStillPresent = "<i>(some inbox sources were unavailable this run)</i>";\n'
     '    void approvedLiteralStillPresent;\n'
     '    if (partial) inboxSection.push(`<i>(${nFailed} inbox sources unavailable this run)</i>`);')
open(p, "w").write(s.replace(a, b))
PY2
}
# Sol QA r4 fresh bypasses. Each puts a tally into the Day Ahead partial-status slot in a
# form the previous digit regexes could not see: the count trailing its label, the count
# written as a word, and the count wrapped in markup so the regex and the reader saw
# different documents. All three are now closed by approved-shape pinning.
_mutate_partial_status() { python3 - "$1/src/index.ts" "$2" <<'PY2'
import sys
p, repl = sys.argv[1], sys.argv[2]
s = open(p).read()
a = '    if (partial) inboxSection.push("<i>(some inbox sources were unavailable this run)</i>");'
if s.count(a) != 1:
    sys.stderr.write(f"selftest mutation target not found ({s.count(a)} matches)\n")
    raise SystemExit(3)
open(p, "w").write(s.replace(a, repl))
PY2
}
m_suffix_tally()       { _mutate_partial_status "$1" '    if (partial) inboxSection.push("<i>(some inbox sources were unavailable this run \u2014 failed reads: 2)</i>");' || return 1; }
m_written_tally()      { _mutate_partial_status "$1" '    if (partial) inboxSection.push("<i>(two inbox sources were unavailable this run)</i>");' || return 1; }
m_markup_tally()       { _mutate_partial_status "$1" '    if (partial) inboxSection.push(`<i>(<b>${[needsThreads, escalations, overdue].filter((x) => x === null).length}</b> inbox sources were unavailable this run)</i>`);' || return 1; }

# Restore the sweep's delete-an-unresolved-incident behaviour.
m_sweep_deletes()      { python3 - "$1/src/index.ts" <<'PY2'
import sys
p = sys.argv[1]; s = open(p).read()
s = s.replace("tombstone: true,", "tombstone_disabled: true,")
open(p, "w").write(s)
PY2
}

case "$PHASE" in
  phase1)
    run_case "manifest-truncated"    "manifest-length"    m_manifest_truncated
    run_case "artifact-mutated"      "before-artifact"    m_artifact_mutated
    run_case "state-map-phrase"      "state-map"          m_state_map_phrase
    run_case "facts-missing-column"  "no-migration-claim" m_facts_missing_col
    run_case "gitignore-stripped"    "runtime-ignored"    m_gitignore_stripped
    run_case "facts-deploy-leak"     "facts-immutable"    m_facts_deploy_leak
    run_case "path-stub-cmp"         "cli-identical"      m_stub_cmp
    ;;
  phase2)
    run_case "forbidden-token"       "forbidden-tokens"   m_forbidden_token
    run_case "health-missing-git-sha" "health-git-sha"    m_health_no_sha
    run_case "unit-tests-broken"     "unit-tests"         m_break_tests
    run_case "killlist-layer3-removed" "kill-list-layers" m_killlist_stripped
    run_case "fire-ttl-restored"     "fire-lifecycle"     m_fire_ttl_restored
    run_case "sender-random-key"     "sender-stable-key"  m_sender_random_key
    run_case "count-reintroduced"    "composer-output"    m_reintroduce_count
    run_case "scorecard-backlog-line" "composer-output"   m_scorecard_backlog
    run_case "inline-source-count"   "composer-output"    m_inline_source_count
    run_case "suffix-tally"          "composer-output"    m_suffix_tally
    run_case "written-number-tally"  "composer-output"    m_written_tally
    run_case "markup-wrapped-tally"  "composer-output"    m_markup_tally
    run_case "sweep-deletes-unresolved" "fire-lifecycle"  m_sweep_deletes
    ;;
  *) echo "verifier-selftest: unknown phase '$PHASE'" >&2; exit 64 ;;
esac

echo "SELFTEST SUMMARY: ${PASS} proven-failing, ${FAILED} broken"
if [ "$FAILED" -ne 0 ]; then
  echo "STEP:verifier-selftest:FAIL ${FAILED} case(s) did not fail at their intended marker"
  exit 1
fi
echo "STEP:verifier-selftest:OK ${PASS} cases proven to fail at their intended marker"
