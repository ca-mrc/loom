#!/usr/bin/env python3
"""Park, then requeue, separate-verifier archives stuck before the settlement fix.

Before #2474, settling a verifier child violated
``execution_leases_materialization_group_check``, so its parent's archive rolled
back on every retry. Both leases stay ``pending`` forever, and the idle rollout
guard counts them as active, so the fix itself cannot be installed.

``park`` runs against the installed schema before rollout. It moves each named
parent and its verifier child from ``pending`` to ``unavailable`` and records the
previous attempts and error on the parent's artifact. ``requeue`` runs once the
fix and migration 0177 are installed. It returns the pair to ``pending`` so the
ordinary materializer archives the parent and settles the child.

Both default to a read-only preview; ``--apply`` writes in one transaction that
changes every named pair or none. Trial results, execution attempts and source
evidence are never changed; materialization attempts continue from their count.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path[:0] = [str(ROOT), str(ROOT / "src")]

from scripts.ops.deploy_nebius_platform import Kubectl, verify_cluster_identity  # noqa: E402

from loom.nebius_rollout_guard import LOCK_KEY  # noqa: E402
from loom_control_plane.pending_archive_retry import (  # noqa: E402
    VERIFIER_PARK_AUDIT_KEY,
    VERIFIER_PARK_ERROR_CODE,
    VERIFIER_REQUEUE_AUDIT_KEY,
)

MAX_PARENTS = 8
PARK_MESSAGE = ("parked: verifier settlement violated execution_leases_materialization_group_check "
                "before the fix; requeue once it is installed")


def _settled(alias: str) -> str:
    """Runtime work and cleanup are finished; nothing is archived or cleaned yet."""
    return (f"{alias}.revoked_at IS NOT NULL AND {alias}.deleted_at IS NOT NULL"
            f" AND {alias}.desired_state = 'deleted' AND {alias}.observed_state = 'deleted'"
            f" AND {alias}.cleanup_state = 'complete' AND {alias}.output_commit_state = 'committed'"
            f" AND {alias}.output_generation = {alias}.resource_generation"
            f" AND {alias}.canonical_trajectory_sha256 IS NULL AND {alias}.canonical_atif_sha256 IS NULL"
            f" AND {alias}.source_cleanup_state = 'not_ready'"
            f" AND {alias}.materialization_recovery_requested_at IS NULL")


def _pairs(team_id: UUID, parent_ids: tuple[UUID, ...]) -> str:
    ids = ", ".join(f"'{parent_id}'" for parent_id in parent_ids)
    return f"""
        FROM execution_leases p
        JOIN trials t ON t.id = p.trial_id AND t.team_id = p.team_id
        JOIN execution_leases c ON c.parent_lease_id = p.id AND c.team_id = p.team_id AND c.trial_id = p.trial_id
        JOIN artifacts a ON a.control_producer_kind = 'service_execution'
            AND a.control_producer_id = p.id AND a.team_id = p.team_id
        WHERE p.team_id = '{team_id}' AND p.id IN ({ids})
          AND p.execution_role = 'attempt' AND p.parent_lease_id IS NULL AND p.finalized_at IS NOT NULL
          AND c.execution_role = 'verifier'
          AND t.result->'verifier_execution'->>'lease_id' = c.id::text
          AND NOT EXISTS (SELECT 1 FROM execution_leases x WHERE x.parent_lease_id = p.id AND x.id <> c.id)
          AND {_settled('p')} AND {_settled('c')}"""


def park_targets(team_id: UUID, parent_ids: tuple[UUID, ...]) -> str:
    return f"""
        SELECT p.id AS parent_id, c.id AS child_id, a.id AS artifact_id, t.id AS trial_id,
               t.state AS trial_state, p.materialization_attempts AS parent_attempts,
               c.materialization_attempts AS child_attempts,
               p.materialization_error_code AS error_code,
               left(p.materialization_error_message, 500) AS error_message,
               p.materialization_next_attempt_at AS parent_next_attempt_at,
               c.materialization_next_attempt_at AS child_next_attempt_at
        {_pairs(team_id, parent_ids)}
          AND p.materialization_state = 'pending' AND p.materialization_attempts >= 1
          AND p.materialization_error_code = 'transient_materialization_error'
          AND strpos(p.materialization_error_message, 'execution_leases_materialization_group_check') > 0
          AND c.materialization_state = 'pending' AND c.materialization_attempts = 0
          AND NOT (COALESCE(a.metadata, '{{}}'::jsonb)
                   ?| ARRAY['{VERIFIER_PARK_AUDIT_KEY}', '{VERIFIER_REQUEUE_AUDIT_KEY}'])"""


def requeue_targets(team_id: UUID, parent_ids: tuple[UUID, ...]) -> str:
    park = f"a.metadata->'{VERIFIER_PARK_AUDIT_KEY}'"
    return f"""
        SELECT p.id AS parent_id, c.id AS child_id, a.id AS artifact_id, t.id AS trial_id,
               t.state AS trial_state, p.materialization_attempts AS parent_attempts,
               c.materialization_attempts AS child_attempts
        {_pairs(team_id, parent_ids)}
          AND p.materialization_state = 'unavailable' AND c.materialization_state = 'unavailable'
          AND p.materialization_error_code = '{VERIFIER_PARK_ERROR_CODE}'
          AND c.materialization_error_code = '{VERIFIER_PARK_ERROR_CODE}'
          AND {park}->>'parent_lease_id' = p.id::text AND {park}->>'child_lease_id' = c.id::text
          AND {park}->>'team_id' = p.team_id::text AND {park}->>'trial_id' = t.id::text
          AND ({park}->>'parent_materialization_attempts')::int = p.materialization_attempts
          AND ({park}->>'child_materialization_attempts')::int = c.materialization_attempts
          AND NOT a.metadata ? '{VERIFIER_REQUEUE_AUDIT_KEY}'"""


def _readback(parent_ids: tuple[UUID, ...]) -> str:
    ids = ", ".join(f"'{parent_id}'" for parent_id in parent_ids)
    return f"""SELECT json_build_object('leases', COALESCE((SELECT json_agg(json_build_object(
        'id', id, 'parent_lease_id', parent_lease_id, 'execution_role', execution_role,
        'materialization_state', materialization_state,
        'materialization_attempts', materialization_attempts,
        'materialization_error_code', materialization_error_code,
        'materialization_recovery_requested_at', materialization_recovery_requested_at)
        ORDER BY parent_lease_id NULLS FIRST, id)
        FROM execution_leases WHERE id IN ({ids}) OR parent_lease_id IN ({ids})), '[]'::json))"""


def preview_sql(action: str, team_id: UUID, parent_ids: tuple[UUID, ...]) -> str:
    targets = park_targets if action == "park" else requeue_targets
    return ("BEGIN READ ONLY; SET LOCAL statement_timeout = '10s'; "
            f"SELECT json_build_object('status', 'preview', 'action', '{action}', 'requested', {len(parent_ids)}, "
            "'guard_held', EXISTS (SELECT 1 FROM nebius_rollout_guard), "
            "'qualified', COALESCE(json_agg(row_to_json(q)), '[]'::json)) "
            f"FROM ({targets(team_id, parent_ids)}) q; ROLLBACK;")


def apply_sql(action: str, team_id: UUID, parent_ids: tuple[UUID, ...]) -> str:
    if action == "park":
        targets = park_targets(team_id, parent_ids)
        writes = f"""
            UPDATE artifacts SET metadata = COALESCE(metadata, '{{}}'::jsonb) || jsonb_build_object(
                '{VERIFIER_PARK_AUDIT_KEY}', jsonb_build_object(
                    'team_id', '{team_id}', 'trial_id', target.trial_id,
                    'parent_lease_id', target.parent_id, 'child_lease_id', target.child_id,
                    'trial_state', target.trial_state,
                    'parent_materialization_attempts', target.parent_attempts,
                    'child_materialization_attempts', target.child_attempts,
                    'previous_error_code', target.error_code,
                    'previous_error_message', target.error_message,
                    'parent_previous_next_attempt_at', target.parent_next_attempt_at,
                    'child_previous_next_attempt_at', target.child_next_attempt_at,
                    'schema_revision', (SELECT version_num FROM alembic_version),
                    'parked_at', now()))
             WHERE id = target.artifact_id;
            UPDATE execution_leases SET materialization_state = 'unavailable',
                   materialization_next_attempt_at = NULL,
                   materialization_error_code = '{VERIFIER_PARK_ERROR_CODE}',
                   materialization_error_message = '{PARK_MESSAGE}', updated_at = now()
             WHERE id IN (target.parent_id, target.child_id);"""
    else:
        targets = requeue_targets(team_id, parent_ids)
        # Migration 0177 admits each lease only against this audit's timestamp.
        writes = f"""
            UPDATE artifacts SET metadata = metadata || jsonb_build_object(
                '{VERIFIER_REQUEUE_AUDIT_KEY}', jsonb_build_object(
                    'requested_at', now(), 'schema_revision', (SELECT version_num FROM alembic_version)))
             WHERE id = target.artifact_id;
            UPDATE execution_leases SET materialization_state = 'pending',
                   materialization_next_attempt_at = now(),
                   materialization_recovery_requested_at = now(), updated_at = now()
             WHERE id IN (target.parent_id, target.child_id);"""
    return f"""BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10s';
DO $verifier_archive$
DECLARE target record; changed integer := 0;
BEGIN
  IF NOT pg_try_advisory_xact_lock_shared({LOCK_KEY}) OR EXISTS (SELECT 1 FROM nebius_rollout_guard) THEN
    RAISE EXCEPTION 'rollout_guard_held';
  END IF;
  FOR target IN {targets}
      FOR UPDATE OF p, c, a LOOP
    {writes}
    changed := changed + 1;
  END LOOP;
  IF changed <> {len(parent_ids)} THEN
    RAISE EXCEPTION '{action}_not_eligible: % of {len(parent_ids)} parent leases qualified', changed;
  END IF;
END $verifier_archive$;
{_readback(parent_ids)};
COMMIT;"""


def run(args: argparse.Namespace, *, kube: Kubectl | None = None) -> dict[str, Any]:
    parent_ids = tuple(dict.fromkeys(args.parent_lease_id))
    if not 1 <= len(parent_ids) <= MAX_PARENTS:
        raise ValueError(f"name between 1 and {MAX_PARENTS} parent leases")
    kube = kube or Kubectl(args.kubeconfig)
    config = json.loads(kube.get("configmap", "loom-platform-config", args.namespace)["data"]["environment.json"])
    if config["namespace"] != args.namespace or config["cluster_id"] != args.expected_cluster_id:
        raise ValueError("platform binding differs")
    verify_cluster_identity(kube, config, args.expected_cluster_id)
    sql = (apply_sql if args.apply else preview_sql)(args.action, args.team_id, parent_ids)
    output = kube.run("exec", "-n", args.namespace, "statefulset/loom-postgres", "--", "psql",
                      "-X", "-qAt", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "loom", "-c", sql)
    result: dict[str, Any] = json.loads(output.strip().splitlines()[-1])
    return {"action": args.action, "applied": args.apply, **result}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("action", choices=("park", "requeue"))
    parser.add_argument("--kubeconfig", type=Path, required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--expected-cluster-id", required=True)
    parser.add_argument("--team-id", type=UUID, required=True)
    parser.add_argument("--parent-lease-id", type=UUID, action="append", required=True)
    parser.add_argument("--apply", action="store_true", help="write; the default is a read-only preview")
    print(json.dumps(run(parser.parse_args()), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
