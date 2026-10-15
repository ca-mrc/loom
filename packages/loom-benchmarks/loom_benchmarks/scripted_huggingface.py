"""Data-only equivalents of two reviewed, immutable upstream dataset loaders.

datasets 4+ no longer executes Hub Python scripts. These sources already ship
JSONL: retain the scripts' file order, field defaults and Arrow string schema
without executing downloaded code or changing the catalog's source identity.
Unknown revisions/configurations require review instead of guessing a layout.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import datasets  # type: ignore[import-untyped]
import huggingface_hub

from loom_benchmarks.base import UpstreamSource

_MATH = ("HuggingFaceTB/MATH", "140a673f1f7182daf7923fdc7108e8cdbf97df46")
_LCB = ("livecodebench/code_generation_lite", "0fe84c3912ea0c4d4a78037083943e8f0c4dd505")
_MATH_SUBJECTS = (
    "algebra", "counting_and_probability", "geometry", "intermediate_algebra",
    "number_theory", "prealgebra", "precalculus",
)
_MATH_FIELDS = ("problem", "level", "type", "solution")
_LCB_FIELDS = (
    "question_title", "question_content", "platform", "question_id", "contest_id",
    "contest_date", "starter_code", "difficulty", "public_test_cases",
    "private_test_cases", "metadata",
)


def _layout(src: UpstreamSource) -> tuple[dict[str, list[str]], tuple[str, ...]]:
    identity = (src.locator, src.revision)
    if identity == _MATH and src.subset in (None, "all", *_MATH_SUBJECTS):
        subjects = _MATH_SUBJECTS if src.subset in (None, "all") else (src.subset,)
        return {
            "train": [f"data/{subject}_train.jsonl" for subject in subjects],
            "test": [f"data/{subject}_test.jsonl" for subject in subjects],
            "fewshot": ["data/prompts_4shot.jsonl"],
        }, _MATH_FIELDS
    if identity == _LCB and src.subset in (None, "release_latest"):
        return {"test": ["test.jsonl", *[f"test{i}.jsonl" for i in range(2, 7)]]}, _LCB_FIELDS
    raise ValueError(
        "No reviewed data-only loader for this source revision/configuration; "
        "use a data-only Hugging Face source or review its pinned JSON layout.",
    )


def _rows(paths: list[str], math: bool) -> Iterator[dict[str, Any]]:
    for path in paths:
        with Path(path).open(encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                if math:
                    # Match MATH.py: missing values have defaults, explicit null
                    # remains null. Features below applies the original coercion.
                    yield {
                        "problem": row["problem"], "level": row.get("level", 0),
                        "type": row.get("type", "Unknown"), "solution": row["solution"],
                    }
                else:
                    yield row


def load_scripted_source(src: UpstreamSource, *, cache_dir: Path) -> Any:
    """Fetch only pinned JSONL and build the same typed, ordered split records."""
    layout, fields = _layout(src)
    features = datasets.Features({field: datasets.Value("string") for field in fields})
    result = datasets.DatasetDict()
    for split, filenames in layout.items():
        paths = [
            huggingface_hub.hf_hub_download(
                repo_id=src.locator, filename=filename, revision=src.revision,
                # datasets 3 used the ordinary Hub cache for these hf://
                # inputs. Keep existing downloads reusable, including offline.
                repo_type="dataset",
            )
            for filename in filenames
        ]
        result[split] = datasets.Dataset.from_generator(
            _rows, features=features,
            gen_kwargs={"paths": paths, "math": src.locator == _MATH[0]},
            cache_dir=str(cache_dir / "data-only-arrow"),
        )
    return result
