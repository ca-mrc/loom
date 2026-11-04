"""The batch page's known-field lists stay aligned with the stored contracts."""

import re
from pathlib import Path

from loom.models.trial import TrialConfig
from loom_service.task_filter import FILTER_KEYS

_WEB = Path(__file__).resolve().parents[2] / "web" / "src" / "lib"


def _known_keys(filename: str, name: str) -> set[str]:
    text = (_WEB / filename).read_text()
    match = re.search(rf"const {name} = new Set\(\[(.*?)\]\);", text, re.S)
    assert match is not None, f"{name} is missing from {filename}"
    return set(re.findall(r'"([A-Za-z0-9_]+)"', match.group(1)))


def test_trial_config_display_covers_persisted_fields() -> None:
    missing = set(TrialConfig.model_fields) - _known_keys(
        "humanizeTrialConfig.ts", "KNOWN_TRIAL_CONFIG_KEYS",
    )
    assert missing == set()


def test_task_filter_display_covers_accepted_keys() -> None:
    missing = set(FILTER_KEYS) - _known_keys(
        "humanizeTaskFilter.ts", "KNOWN_TASK_FILTER_KEYS",
    )
    assert missing == set()
