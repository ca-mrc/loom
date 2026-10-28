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


# Existing immutable catalog bundles contain this complete V1 template. Keep
# its bytes recognizable while runtime execution uses the corrected bridge.
_NATIVE_VERIFIER_RUN_SH_V1 = b"""#!/bin/sh
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
python_bin=$(command -v python3 || command -v python)
"$python_bin" - "$logs" "$LOOM_VERIFIER_OUTPUT" "$rc" <<'REWARD_PY'
from __future__ import print_function
import json
import math
import numbers
import os
import sys
logs, output = sys.argv[1], sys.argv[2]
try:
    string_types = (basestring,)
except NameError:
    string_types = (str,)
try:
    if os.path.exists(os.path.join(logs, "reward.txt")):
        with open(os.path.join(logs, "reward.txt")) as source:
            rewards = {"resolved": float(source.read().strip())}
    else:
        with open(os.path.join(logs, "reward.json")) as source:
            rewards = json.load(source)
    if not isinstance(rewards, dict) or not rewards:
        raise ValueError("required numeric reward is missing")
    if any(not isinstance(k, string_types) or isinstance(v, bool) or not isinstance(v, numbers.Real)
           or math.isnan(v) or math.isinf(v) for k, v in rewards.items()):
        raise ValueError("required reward is malformed or non-finite")
except (IOError, OSError, ValueError, OverflowError) as exc:
    print("harbor_reward_error=" + str(exc), file=sys.stderr)
    raise SystemExit(1)
with open(output, "w") as target:
    target.write(json.dumps({"rewards": rewards, "structured": {"test_sh_returncode": int(sys.argv[3])}}) + "\\n")
REWARD_PY
"""


_NATIVE_VERIFIER_RUN_SH = _NATIVE_VERIFIER_RUN_SH_V1.replace(
    b"""    if os.path.exists(os.path.join(logs, "reward.txt")):
        with open(os.path.join(logs, "reward.txt")) as source:
            rewards = {"resolved": float(source.read().strip())}
    else:
        with open(os.path.join(logs, "reward.json")) as source:
            rewards = json.load(source)""",
    b"""    if os.path.exists(os.path.join(logs, "reward.json")):
        with open(os.path.join(logs, "reward.json")) as source:
            rewards = json.load(source)
    else:
        with open(os.path.join(logs, "reward.txt")) as source:
            rewards = {"reward": float(source.read().strip())}""",
)


def native_verifier_run_sh_templates() -> tuple[bytes, ...]:
    """Complete known catalog wrappers, including the immutable V1 template."""
    return (_NATIVE_VERIFIER_RUN_SH, _NATIVE_VERIFIER_RUN_SH_V1)


def native_verifier_run_sh_bytes() -> bytes:
    return _NATIVE_VERIFIER_RUN_SH


def native_verifier_workdir_run_sh_bytes() -> bytes:
    """Runtime bridge for private inputs with a separate business working tree.

    The imported wrapper and original test.sh remain immutable. Only the
    platform-owned bridge selects the execution directory independently from
    the private task asset directory; old callers retain their default cwd.
    """
    return _NATIVE_VERIFIER_RUN_SH.replace(
        b'(cd "$task_dir" && bash "$task_dir/tests/test.sh")',
        b'(cd "${LOOM_HARBOR_WORKDIR:-$task_dir}" && bash "$task_dir/tests/test.sh")',
    )
