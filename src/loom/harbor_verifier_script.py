"""Dependency-free generated Harbor verifier contract shared by ingest and runtime."""

VERIFIER_SCRIPT_PATH = "verifier/run.sh"


# The transformed Harbor script retains its task-specific setup and pytest args.
# Only its dependency installation moves into the derived task image.
_OFFLINE_VERIFIER_RUN_SH = b"""#!/bin/sh
# Loom native Harbor runner: preserve the original offline test semantics.
set -eu
: "${LOOM_VERIFIER_OUTPUT:?LOOM_VERIFIER_OUTPUT is required}"
task_dir="${LOOM_TASK_DIR:-/app}"
mkdir -p /tests /logs/verifier /loom/verifier
cp -R "$task_dir/tests/." /tests/
rm -f /logs/verifier/reward.txt /logs/verifier/ctrf.json
set +e
bash "$task_dir/verifier/harbor-offline.sh"
rc=$?
set -e
reward=$(cat /logs/verifier/reward.txt)
case "$reward" in
    0) passed=false ;;
    1) passed=true ;;
    *) echo "Harbor verifier reward must be 0 or 1" >&2; exit 1 ;;
esac
mkdir -p "$(dirname "$LOOM_VERIFIER_OUTPUT")"
cat > "$LOOM_VERIFIER_OUTPUT" <<JSONEOF
{"rewards":{"resolved":$reward,"passed":$reward},"checks":[{"name":"harbor_test_sh","passed":$passed,"score":$reward,"message":"exit=$rc"}],"structured":{"exit_code":$rc,"reward":$reward}}
JSONEOF
if [ "$rc" -ne 0 ]; then
    echo "offline Harbor verifier script failed" >&2
fi
exit "$rc"
"""


def offline_verifier_run_sh_bytes() -> bytes:
    """Return the Nebius offline ``verifier/run.sh`` template bytes."""

    return _OFFLINE_VERIFIER_RUN_SH


_NATIVE_VERIFIER_RUN_SH = b"""#!/bin/sh
# Shared native Harbor reward bridge. Native scripts and rewards are unchanged.
set -eu
: "${LOOM_VERIFIER_OUTPUT:?LOOM_VERIFIER_OUTPUT must be set}"
task_dir="${LOOM_TASK_DIR:-/app}"
logs="${LOOM_HARBOR_LOG_DIR:-/logs/verifier}"
tests="${LOOM_HARBOR_TEST_DIR:-/tests}"
mkdir -p "$logs" "$tests" "$(dirname "$LOOM_VERIFIER_OUTPUT")"
rm -f "$logs/reward.txt" "$logs/reward.json" "$LOOM_VERIFIER_OUTPUT"
cp -R "$task_dir/tests/." "$tests/"
set +e
(cd "$task_dir" && bash "$task_dir/tests/test.sh")
rc=$?
set -e
if [ "$rc" -eq 124 ]; then exit 1; fi
python3 - "$logs" "$LOOM_VERIFIER_OUTPUT" "$rc" <<'REWARD_PY'
import json
import math
import sys
from pathlib import Path
logs, output = Path(sys.argv[1]), Path(sys.argv[2])
try:
    if (logs / "reward.txt").exists():
        rewards = {"resolved": float((logs / "reward.txt").read_text().strip())}
    else:
        rewards = json.loads((logs / "reward.json").read_text())
    if not isinstance(rewards, dict) or not rewards:
        raise ValueError("required numeric reward is missing")
    if any(not isinstance(k, str) or isinstance(v, bool) or not isinstance(v, (int, float))
           or not math.isfinite(v) for k, v in rewards.items()):
        raise ValueError("required reward is malformed or non-finite")
except (OSError, ValueError) as exc:
    print("harbor_reward_error=" + str(exc), file=sys.stderr)
    raise SystemExit(1)
output.write_text(json.dumps({"rewards": rewards, "structured": {"test_sh_returncode": int(sys.argv[3])}}) + "\\n")
REWARD_PY
"""


def native_verifier_run_sh_bytes() -> bytes:
    return _NATIVE_VERIFIER_RUN_SH
