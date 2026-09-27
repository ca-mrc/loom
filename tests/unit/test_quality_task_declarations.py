"""Reviewed declaration repair never changes task/scoring semantics."""
import tomllib

import pytest
from scripts.ops.repair_quality_task_declarations import (
    JUPYTER,
    JUPYTER_ROOTS,
    POETRY,
    corrected_config,
)

from tests.unit.test_service_execution_terminus_plan import _inputs


@pytest.mark.parametrize("task_id", [POETRY, JUPYTER])
def test_repair_changes_only_required_environment_declarations(task_id):
    task, _, _ = _inputs()
    original = task.model_dump(mode="json")
    original["task"]["id"] = task_id
    original["environment"]["mutable_paths"] = ["/root/.cache/pypoetry"] if task_id == POETRY else [JUPYTER_ROOTS[0]]
    if task_id == JUPYTER:
        original["environment"].update(workspace_reference_files=["/usr/bin/python3", "/usr/bin/python3.10"],
                                     reference_file_symlinks={"/usr/bin/python3": "python3.10"})
    revised = corrected_config(original)
    assert {k:v for k,v in revised.items() if k != "environment"} == {k:v for k,v in original.items() if k != "environment"}
    changed = {k for k in revised["environment"] if revised["environment"].get(k) != original["environment"].get(k)}
    assert changed == ({"workspace_reference_files", "mutable_path_reference_files"} if task_id == POETRY else {"mutable_paths"})
    if task_id == JUPYTER:
        assert revised["environment"]["mutable_paths"] == list(JUPYTER_ROOTS)
    else:
        assert revised["environment"]["workspace_reference_files"] == ["/usr/local/bin/python3.9"]
        assert revised["environment"]["mutable_path_reference_files"] == ["/usr/local/bin/python3.9"]
    assert corrected_config(revised) == revised


def test_prepare_revision_refuses_unreviewed_source_before_copying(tmp_path):
    from scripts.ops.repair_quality_task_declarations import prepare_revision
    source = tmp_path / "source"
    source.mkdir()
    (source / "task.toml").write_text(f'[task]\nid="{POETRY}"\n')
    with pytest.raises(ValueError, match="reviewed immutable"):
        prepare_revision(source, tmp_path / "new")
    assert not (tmp_path / "new").exists()
    assert tomllib.loads((source / "task.toml").read_text())["task"]["id"] == POETRY
