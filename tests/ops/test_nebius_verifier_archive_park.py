"""The stuck verifier-archive operator binds one platform and previews by default."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from scripts.ops import nebius_verifier_archive_park as operator

TEAM = UUID("00000000-0000-4000-8000-000000000001")


class FakeKube:
    kubeconfig = Path("/kubeconfig")

    def __init__(self, namespace: str = "loom-dev", cluster_id: str = "mk8scluster-dev") -> None:
        self.environment = {"namespace": namespace, "cluster_id": cluster_id}
        self.sql: list[str] = []

    def get(self, kind: str, name: str, namespace: str) -> dict[str, Any]:
        assert (kind, name, namespace) == ("configmap", "loom-platform-config", "loom-dev")
        return {"data": {"environment.json": json.dumps(self.environment)}}

    def run(self, *args: str) -> str:
        assert args[:6] == ("exec", "-n", "loom-dev", "statefulset/loom-postgres", "--", "psql")
        self.sql.append(args[-1])
        return '\n{"status": "preview", "qualified": []}\n'


def _args(*parents: UUID, action: str = "park", apply: bool = False) -> argparse.Namespace:
    return argparse.Namespace(action=action, kubeconfig=Path("/kubeconfig"), namespace="loom-dev",
                              expected_cluster_id="mk8scluster-dev", team_id=TEAM,
                              parent_lease_id=list(parents), apply=apply)


@pytest.fixture(autouse=True)
def _trusted_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(operator, "verify_cluster_identity", lambda *_args: None)


@pytest.mark.parametrize("action", ["park", "requeue"])
def test_default_is_one_read_only_preview_of_the_named_pairs(action: str) -> None:
    kube = FakeKube()
    parent = uuid4()

    result = operator.run(_args(parent, parent, action=action), kube=kube)  # type: ignore[arg-type]

    assert result == {"action": action, "applied": False, "status": "preview", "qualified": []}
    [sql] = kube.sql
    assert sql.startswith("BEGIN READ ONLY;") and sql.endswith("ROLLBACK;")
    assert "UPDATE" not in sql and sql.count(str(parent)) == 1


@pytest.mark.parametrize("action", ["park", "requeue"])
def test_apply_changes_every_named_pair_or_none(action: str) -> None:
    parents = (uuid4(), uuid4())

    sql = operator.apply_sql(action, TEAM, parents)

    assert sql.startswith("BEGIN;") and sql.rstrip().endswith("COMMIT;")
    assert f"IF changed <> {len(parents)} THEN" in sql
    assert "rollout_guard_held" in sql and "FOR UPDATE OF p, c, a" in sql
    # Neither step rewrites Trials or a materialization attempt count.
    writes = sql.split(" LOOP", 1)[1].split("changed := changed + 1", 1)[0]
    assert "UPDATE trials" not in writes and "materialization_attempts" not in writes.replace(
        "'parent_materialization_attempts', target.parent_attempts", "").replace(
        "'child_materialization_attempts', target.child_attempts", "")


def test_requeue_is_bound_to_the_park_audit_and_installs_one_timestamp() -> None:
    sql = operator.apply_sql("requeue", TEAM, (uuid4(),))

    assert "'verifier_settlement_park'->>'child_lease_id' = c.id::text" in sql
    assert "NOT a.metadata ? 'verifier_settlement_requeue'" in sql
    assert "materialization_recovery_requested_at = now()" in sql
    assert "materialization_next_attempt_at = now()" in sql


@pytest.mark.parametrize(
    ("kube", "parents", "message"),
    [
        (FakeKube(namespace="loom-staging"), (uuid4(),), "platform binding differs"),
        (FakeKube(cluster_id="mk8scluster-other"), (uuid4(),), "platform binding differs"),
        (FakeKube(), tuple(uuid4() for _ in range(operator.MAX_PARENTS + 1)), "between 1 and"),
    ],
)
def test_refuses_another_platform_or_an_unbounded_batch(
    kube: FakeKube, parents: tuple[UUID, ...], message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        operator.run(_args(*parents), kube=kube)  # type: ignore[arg-type]
    assert kube.sql == []
