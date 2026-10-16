from __future__ import annotations

import hashlib
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import TaskImageMaterialization, Team, TeamMembership, TeamQuota, Token, User


@pytest.fixture(autouse=True)
def _cleanup_seeded_rows(postgres_url: str) -> Iterator[None]:
    # Each seed run adds a team with a team token. Later modules on the same
    # integration shard delete every team, which that token would block.
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    seeded_team = Team.name.like("e2e-%")
    with session_factory() as session:
        existing = set(session.scalars(select(Team.id).where(seeded_team)))
    yield
    with session_factory() as session:
        session.execute(
            delete(TaskImageMaterialization).where(
                TaskImageMaterialization.task_id == "hello-world"
            )
        )
        teams = [team for team in session.scalars(select(Team.id).where(seeded_team)) if team not in existing]
        users = list(session.scalars(
            select(TeamMembership.user_id).where(TeamMembership.team_id.in_(teams))
        ))
        session.execute(delete(Token).where(Token.team_id.in_(teams)))
        session.execute(delete(TeamMembership).where(TeamMembership.team_id.in_(teams)))
        session.execute(delete(TeamQuota).where(TeamQuota.team_id.in_(teams)))
        session.execute(delete(Team).where(Team.id.in_(teams)))
        session.execute(delete(User).where(User.id.in_(users)))
        session.commit()
    engine.dispose()


def test_seed_test_data_does_not_create_db_admin_tokens(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as s:
        s.execute(delete(Token))
        s.commit()

    repo_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "scripts/seed_test_data.py",
            "--db-url",
            postgres_url,
            "--mode",
            "test",
            "--print",
            "all",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    printed = {
        line.split(":", 1)[0].strip()
        for line in result.stdout.splitlines()
        if ":" in line
    }
    assert printed == {"team", "worker", "builder"}
    printed_tokens = dict(
        line.split(":", 1)
        for line in result.stdout.splitlines()
        if ":" in line
    )
    assert printed_tokens["team"].strip().startswith("loom_api_")
    assert printed_tokens["worker"].strip().startswith("loom_w_")
    assert printed_tokens["builder"].strip().startswith("loom_tib_")

    with session_factory() as s:
        rows = s.execute(
            select(Token.type, Token.scopes, Token.created_by_user_id),
        ).all()
        submitting_token = next(row for row in rows if "submit" in row.scopes)
        assert submitting_token.created_by_user_id is not None
        user = s.get(User, submitting_token.created_by_user_id)
        assert user is not None
        assert user.status == "active"
        membership = s.execute(
            select(TeamMembership).where(
                TeamMembership.user_id == submitting_token.created_by_user_id,
            ),
        ).scalar_one()
        assert membership.role == "owner"

    assert rows
    assert all(row.type != "admin" for row in rows)
    assert all(
        not any(scope.startswith("admin:") for scope in row.scopes)
        for row in rows
    )

    engine.dispose()


def test_seed_test_data_system_output_includes_least_privilege_builder_token(
    postgres_url: str,
) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "scripts/seed_test_data.py",
            "--db-url",
            postgres_url,
            "--mode",
            "test",
            "--print",
            "system",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    team_token, worker_token, builder_token = result.stdout.strip().splitlines()
    assert team_token.startswith("loom_api_")
    assert worker_token.startswith("loom_w_")
    assert builder_token.startswith("loom_tib_")

    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as session:
        builder_row = session.execute(
            select(Token.type, Token.scopes).where(
                Token.token_hash == hashlib.sha256(builder_token.encode()).digest()
            )
        ).one()
        materializations = session.execute(
            select(
                TaskImageMaterialization.cpu_arch,
                TaskImageMaterialization.state,
            ).where(TaskImageMaterialization.task_id == "hello-world")
        ).all()
    assert builder_row == ("worker", ["task-image:build"])
    assert set(materializations) == {("x86_64", "queued")}
    engine.dispose()
