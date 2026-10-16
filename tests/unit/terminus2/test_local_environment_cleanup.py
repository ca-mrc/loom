"""The controller environment descriptor belongs to the runtime's temporary root."""

from pathlib import Path, PurePosixPath
from uuid import uuid4

import pytest

from loom.agent.terminus2.harbor_environment import LoomHarborEnvironment, make_trial_paths
from loom.driver.fake import FakeDriver

pytest.importorskip("harbor")


def test_harbor_environment_descriptor_has_no_independent_temp_directory(tmp_path: Path) -> None:
    paths = make_trial_paths(tmp_path)
    env = LoomHarborEnvironment.create(
        driver=FakeDriver(), trial_paths=paths, workdir=PurePosixPath("/workspace"),
        trial_id=uuid4(), step_id="agent",
    )
    assert env.environment_dir.is_relative_to(tmp_path)
