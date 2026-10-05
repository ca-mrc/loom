"""Stdlib-only exact-source management tooling and fixed forced-SSH actions."""
from __future__ import annotations

import fcntl
import hashlib
import io
import json
import os
import re
import stat
import sys
import zipfile
from pathlib import Path
from typing import Any
from uuid import UUID

from scripts.ops.nebius_certificate_gateway import _directory, _read, _write, run_private

SOURCES = (*( "scripts/ops/" + name + ".py" for name in (
    "nebius_certificate_gateway", "nebius_certificates", "nebius_dns_challenge", "nebius_dns_publication",
    "nebius_ingress_bootstrap", "nebius_ingress_gateway", "nebius_ingress_stage", "nebius_ingress_cutover",
    "nebius_ingress_image", "nebius_ingress_operation", "nebius_ingress_probe",
    "nebius_management_gateway", "nebius_management_entry", "nebius_management_transport",
    "nebius_management_bootstrap", "nebius_management_material", "nebius_management_stage",
    "nebius_management_authority_stage", "nebius_management_authority_probe", "nebius_management_supplied",
    "nebius_management_storage", "nebius_management_install", "nebius_management_evidence",
    "nebius_management_proofs", "nebius_management_live", "nebius_management_capacity",
    "nebius_management_cloud_scope", "nebius_management_prerequisites",
    "nebius_application_setup", "nebius_application_cloud_scope", "nebius_application_upgrade_prerequisites",
    "nebius_management_switch", "nebius_management_upgrade", "nebius_management_upgrade_live",
    "nebius_management_retirement", "nebius_management_retirement_entry",
    "nebius_retirement_startup_probe", "nebius_management_retirement_diagnostic",
    "nebius_management_retirement_diagnostic_entry", "nebius_management_retirement_diagnostic_live",
    "nebius_retirement_recovery_runner", "nebius_management_retirement_recovery",
    "nebius_management_retirement_recovery_entry", "nebius_management_retirement_recovery_live",
    "nebius_management_refresh", "nebius_management_refresh_switch", "nebius_management_refresh_live",
    "nebius_management_refresh_resources", "nebius_management_refresh_evidence", "nebius_management_refresh_backup",
    "nebius_management_refresh_install", "nebius_management_refresh_predecessor", "nebius_management_refresh_connected",
    "nebius_management_refresh_entry", "nebius_management_refresh_supersession",
    "deploy_nebius_platform", "nebius_pool_application_delivery", "nebius_pool_activation_database", "nebius_pool_activation_live",
    "nebius_pool_activation_stage", "nebius_pool_completion", "nebius_pool_cutover",
    "nebius_pool_cutover_entry", "nebius_pool_cutover_live", "nebius_pool_dormant",
    "nebius_pool_gateway_authority", "nebius_pool_gateway_probe", "nebius_pool_gateway_retirement",
    "nebius_pool_guard_activation", "nebius_pool_legacy_authority", "nebius_pool_legacy_reopening",
    "nebius_pool_legacy_restart", "nebius_pool_legacy_settings", "nebius_pool_machine_database",
    "nebius_pool_machine_retirement", "nebius_pool_material", "nebius_pool_migration",
    "nebius_pool_migration_guard", "nebius_pool_operation", "nebius_pool_origin_history", "nebius_pool_platform_authority",
    "nebius_pool_predecessor", "nebius_pool_projection", "nebius_pool_recovery_database", "nebius_pool_recovery_release",
    "nebius_pool_refresh", "nebius_pool_refresh_live", "nebius_pool_registration",
    "nebius_pool_retirement", "nebius_pool_retirement_live", "nebius_pool_role_fencing",
    "nebius_pool_role_fencing_live", "nebius_pool_role_restoration", "nebius_pool_runtime",
    "nebius_pool_runtime_settings", "nebius_pool_shutdown", "nebius_pool_startup",
    "nebius_pool_startup_capacity", "nebius_pool_startup_database", "nebius_pool_startup_fence",
    "nebius_pool_startup_live", "nebius_pool_template_restoration",
)), "deploy/k8s/nebius-execution-actuator.yaml", "deploy/k8s/nebius-capacity-collector.yaml")
LIMITS = {**dict.fromkeys(SOURCES, 262144), "uv": 80 * 1024**2,
          "requirements.txt": 262144, "operation.json": 16384, "manifest.json": 16384}
MAX_BUNDLE, MAX_WHEEL = 100 * 1024**2, 16 * 1024**2
COMMANDS = {"loom-nebius-management-preflight-v1": "preflight", "loom-nebius-management-install-v1": "install",
    "loom-nebius-pool-rollback-v1": "rollback"}
POOL_PHASES = frozenset({'cutover', 'startup', 'activation', 'startup-fence', 'shutdown', 'machine-retirement',
    'gateway-retirement', 'template-restoration', 'role-restoration', 'legacy-restart', 'legacy-reopening'})
REFRESH_RETAINED_PREFLIGHT_STAGES = frozenset({
    "recovery", "cluster_identity", "resource_inventory", "persistent_storage", "prerequisites",
    "foundation", "shared_material", "platform_capacity", "publication", "cloud_identity", "public_route",
})
OPTIONAL_TELEMETRY_STAGES = frozenset({
    'tls_kubelet', 'kubelet_authorization', 'kubelet_network', 'kubelet_http', 'counters',
    *(f'tls_kubelet_verify_{code}' for code in range(256)),
})
DIAGNOSTIC_STAGES = frozenset({"operation", "connection", "render", "cluster_identity", "prerequisites",
    "foundation", "resource_inventory", "platform_capacity", "storage_class", "persistent_storage",
    "publication", "cloud_identity", "provider_quota", "backup_access", "public_route",
    "recovery", "runtime_authority", "public_authentication", "backup_execution", "backup_object",
    "install_bootstrap", "install_config", "install_authority", "install_supplied", "install_database",
    "install_storage", "install_migration", "install_backup", "install_schedule", "install_service", "install_public",
    "ready_database", "ready_migration", "ready_backup", "ready_service",
    "backup_job", "backup_pod_list", "backup_pod_identity", "backup_pod_template", "backup_pod_status",
    "backup_log", "backup_readback", "shared_material",
    "upgrade_config", "upgrade_admission", "upgrade_permissions", "upgrade_network", "upgrade_material",
    "upgrade_database", "upgrade_retirement", "upgrade_migration", "upgrade_retire", "upgrade_activate", "retirement",
    "diagnostic_original", "diagnostic_stage", "diagnostic_job", "diagnostic_pod", "diagnostic_log", "diagnostic_readback",
    "diagnostic_pod_list", "diagnostic_pod_identity", "diagnostic_pod_owner", "diagnostic_pod_observation",
    "diagnostic_pod_labels", "diagnostic_pod_template", "diagnostic_pod_security", "diagnostic_pod_status",
    "diagnostic_container_status", "diagnostic_container_shape",
    "recovery_original", "recovery_dns", "recovery_stage", "recovery_runtime",
    "refresh_connection", "refresh_predecessor", "refresh_retained_installation", "refresh_retained_application",
    "refresh_manager", "refresh_recovery", "refresh_prerequisites", "refresh_config", "refresh_retire",
    "refresh_manager_probe", "refresh_shared_probe", "refresh_backup", "refresh_migration",
    "refresh_post_migration_probe", "refresh_activate", "refresh_activation", "refresh_public",
    "refresh_public_authentication", "refresh_completion", "refresh_supersession", "refresh_pool_authority",
    *("refresh_" + stage for stage in REFRESH_RETAINED_PREFLIGHT_STAGES),
    *('pool_' + stage.replace('-', '_') for stage in POOL_PHASES | {
        'operation', 'connection', 'preflight', 'cancellation', 'completion',
        'publication', 'operator_readers', 'runtime_databases', 'runtime_telemetry',
        *('runtime_telemetry_' + detail for detail in {
            'binding', 'pod', 'nodes', 'probe', 'recheck', 'settings', 'client', 'tls',
            'authorization', 'network', 'http', 'reader', 'counters', 'close',
            'identity', 'address', 'authority', 'payload', 'tls_api', 'tls_kubelet',
            *OPTIONAL_TELEMETRY_STAGES,
            *(f'tls_{transport}_verify_{code}' for transport in ('api', 'kubelet', 'unknown') for code in range(256))}),
        'management_database', 'provider', 'connected_scope', 'private_inputs'})})
_ENTRY = "import sys; sys.path.insert(0, sys.argv[1]); from scripts.ops.nebius_management_entry import main; raise SystemExit(main(sys.argv[2], sys.argv[3]))"


class GatewayError(RuntimeError):
    """Payload-free failure; preserve credentials, installed resources and state."""


def validate_operation(value: dict[str, Any]) -> None:
    try:
        fields = {"schema", "source_sha", "candidate", "installation_id", "namespace",
                  "state_dir", "anchor_dir", "inputs_path", "inputs_sha256"}
        refresh = value.get('schema') == 'loom.nebius-management-refresh-operation.v1'
        pool = value.get('schema') == 'loom.nebius-pool-cutover-operation.v1'
        if refresh or pool:
            fields.add('operation_id')
        if set(value) != fields or any(not isinstance(item, str) or not 0 < len(item) <= 1024 for item in value.values()):
            raise ValueError()
        if value["schema"] not in {"loom.nebius-management-operation.v1", "loom.nebius-management-upgrade-operation.v1",
                                   "loom.nebius-management-retirement-operation.v1",
                                   "loom.nebius-management-retirement-diagnostic-operation.v1",
                                   "loom.nebius-management-retirement-recovery-operation.v1",
                                   "loom.nebius-management-refresh-operation.v1", "loom.nebius-pool-cutover-operation.v1"}:
            raise ValueError()
        if any(not re.fullmatch(r"[0-9a-f]{40}", value[key]) for key in ("source_sha", "candidate")):
            raise ValueError()
        if not re.fullmatch(r"[0-9a-f]{64}", value["inputs_sha256"]):
            raise ValueError()
        if str(UUID(value["installation_id"])) != value["installation_id"] or UUID(value["installation_id"]).int == 0:
            raise ValueError()
        if len(value["namespace"]) > 53 or not re.fullmatch(r"loom-nebius-management(?:-[a-z0-9]+(?:-[a-z0-9]+)*)?", value["namespace"]):
            raise ValueError()
        for key in ("state_dir", "anchor_dir", "inputs_path"):
            path = Path(value[key])
            if not path.is_absolute() or path != path.resolve() or not re.fullmatch(r"/[A-Za-z0-9_./-]+", str(path)):
                raise ValueError()
        state = Path(value["state_dir"])
        root = state.parent
        if refresh or pool:
            operation_id = UUID(value['operation_id'])
            if (not operation_id.int or str(operation_id) != value['operation_id'] or root.name != str(operation_id)
                    or root.parent.name != ('pool-cutover' if pool else 'refresh') or value['source_sha'] != value['candidate']):
                raise ValueError()
            root = root.parent.parent
        separated = {"loom.nebius-management-upgrade-operation.v1": "upgrade",
                     "loom.nebius-management-retirement-operation.v1": "retirement",
                     "loom.nebius-management-retirement-diagnostic-operation.v1": "retirement-diagnostic",
                     "loom.nebius-management-retirement-recovery-operation.v1": "retirement-recovery"}
        if value["schema"] in separated:
            if root.name != separated[value["schema"]]:
                raise ValueError()
            root = root.parent
        if (state.name != "state" or root.name != "nebius-management"
                or Path(value["anchor_dir"]) != state.parent / "anchor"
                or Path(value["inputs_path"]) != state.parent / "inputs.json"):
            raise ValueError()
    except Exception:
        raise GatewayError("invalid management operation metadata") from None


def validate_action(action: str, operation: dict[str, Any]) -> None:
    validate_operation(operation)
    if (action not in {'qualify', 'preflight', 'install', 'rollback'}
            or (action == 'rollback' and operation['schema'] != 'loom.nebius-pool-cutover-operation.v1')):
        raise GatewayError('management action outside fixed authority')


def unpack_bundle(content: bytes) -> tuple[dict[str, bytes], dict[str, Any]]:
    try:
        if not 0 < len(content) <= MAX_BUNDLE:
            raise ValueError()
        with zipfile.ZipFile(io.BytesIO(content)) as archive:
            entries = archive.infolist()
            names = [entry.filename for entry in entries]
            wheels = {name for name in names if name.startswith("wheels/")}
            if (len(names) != len(set(names)) or set(names) != set(LIMITS) | wheels or len(wheels) != 2
                    or not all(sum(bool(re.fullmatch(r"wheels/" + package + r"-[0-9][0-9.]*-py3-none-any\.whl", name))
                                   for name in wheels) == 1 for package in ("loom", "loom_bundle_checksum"))):
                raise ValueError()
            if any(entry.file_size > LIMITS.get(entry.filename, MAX_WHEEL) or entry.is_dir()
                   or stat.S_ISLNK(entry.external_attr >> 16) for entry in entries):
                raise ValueError()
            files = {entry.filename: archive.read(entry) for entry in entries}
        if json.loads(files["manifest.json"]) != {
            name: hashlib.sha256(value).hexdigest() for name, value in files.items() if name != "manifest.json"
        }:
            raise ValueError()
        operation = json.loads(files["operation.json"])
        validate_operation(operation)
        return files, operation
    except Exception:
        raise GatewayError("invalid management tooling bundle") from None


def command(release: Path, action: str) -> list[str]:
    if action not in {"qualify", "preflight", "install", "rollback"}:
        raise GatewayError("management action outside fixed authority")
    return [str(release / "venv/bin/python"), "-I", "-c", _ENTRY,
            str(release), str(release / "operation.json"), action]


def prepare_release(content: bytes) -> Path:
    files, operation = unpack_bundle(content)
    root = Path(operation["state_dir"]).parent
    try:
        _directory(root)
        lock_path = root / "tooling.lock"
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(descriptor, "rb+") as lock:
            _read(lock_path, 1)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            releases = root / "releases"
            _directory(releases)
            release = releases / hashlib.sha256(content).hexdigest()
            if release.exists() or release.is_symlink():
                _directory(release)
                if _read(release / "complete", 64) != b"complete":
                    raise ValueError()
                for name, expected in files.items():
                    if _read(release / name, LIMITS.get(name, MAX_WHEEL)) != expected:
                        raise ValueError()
                return release
            if sum(1 for _ in releases.iterdir()) >= 8:
                raise ValueError()
            for path in (release, release / "scripts", release / "scripts/ops", release / "wheels",
                         release / "deploy", release / "deploy/k8s"):
                _directory(path)
            for name, value in files.items():
                _write(release / name, value, executable=name == "uv")
            uv, python = str(release / "uv"), str(release / "venv/bin/python")
            run_private([uv, "venv", "--no-config", "--no-cache", "--no-python-downloads",
                         "--python", "/usr/bin/python3", str(release / "venv")], timeout=90)
            run_private([uv, "pip", "sync", "--no-config", "--no-cache", "--python", python,
                         "--require-hashes", "--only-binary", ":all:", "--index-url", "https://pypi.org/simple",
                         str(release / "requirements.txt")], timeout=600)
            run_private([uv, "pip", "install", "--no-config", "--no-cache", "--python", python,
                         "--offline", "--no-deps", *sorted(str(release / name) for name in files if name.startswith("wheels/"))], timeout=90)
            run_private(command(release, "qualify"), timeout=60)
            _write(release / "complete", b"complete")
            return release
    except Exception:
        raise GatewayError("management tooling incomplete; retain private state") from None


def validate_startup_report(value: dict[str, Any]) -> dict[str, Any]:
    """Closed payload contract; no raw rows, provider messages or credential data."""
    try:
        fields = {"schema", "status", "stage", "checks", "operations"}
        success = value["status"] == "observed"
        steps = ["database_binding", "kubernetes_ca", "kubernetes_token", "database", "kubernetes"]
        completed = {"settings": 0, "database_binding": 0, "kubernetes_ca": 1, "kubernetes_token": 2,
                     "database": 3, "kubernetes_get": 4, "kubernetes_identity": 4, "complete": 5}
        if (set(value) != fields | (set() if success else {"error_type", "http_status"})
                or value["schema"] != "loom.nebius-retirement-startup-probe.v1"
                or value["status"] not in {"observed", "unavailable"}
                or success != (value["stage"] == "complete")
                or value["checks"] != steps[:completed[value["stage"]]]):
            raise ValueError
        operations = value["operations"]
        if (not isinstance(operations, list) or len(operations) > 16
                or bool(operations) != ("database" in value["checks"])):
            raise ValueError
        seen = set()
        for row in operations:
            if (set(row) != {"operation_id", "phase", "runner_epoch", "lease_present", "error_present", "resource_count", "effects_started"}
                    or str(UUID(row["operation_id"])) != row["operation_id"] or not UUID(row["operation_id"]).int
                    or row["operation_id"] in seen or row["phase"] not in {"pending", "running", "blocked", "completed"}
                    or any(type(row[key]) is not bool for key in ("lease_present", "error_present", "effects_started"))
                    or any(type(row[key]) is not int or not 0 <= row[key] < 2**63 for key in ("runner_epoch", "resource_count"))):
                raise ValueError
            seen.add(row["operation_id"])
        if not success:
            errors = {"ValueError", "KeyError", "ValidationError", "FileNotFoundError", "PermissionError", "SSLError",
                "TimeoutError", "OperationalError", "ProgrammingError", "InternalError", "StatementError",
                "HTTPStatusError", "ConnectError", "ConnectTimeout", "ReadTimeout", "RemoteProtocolError",
                "ProviderBlockedError", "ProviderRetryError", "OtherError"}
            if value["error_type"] not in errors:
                raise ValueError
            code = value["http_status"]
            if ((code is not None and (type(code) is not int or not 100 <= code <= 599))
                    or (code is not None) != (value["error_type"] == "HTTPStatusError")):
                raise ValueError
        return value
    except Exception:
        raise GatewayError("invalid retirement startup observation") from None


def validate_recovery_report(value: dict[str, Any]) -> dict[str, Any]:
    """A completed Pod is insufficient: require exact, storage-preserving proof."""
    try:
        if (set(value) != {"schema", "status", "stage", "retirement_started", "error_type", "startup", "operations"}
                or value["schema"] != "loom.nebius-retirement-recovery-report.v1"
                or value["status"] not in {"completed", "blocked"}
                or type(value["retirement_started"]) is not bool):
            raise ValueError
        complete = value["status"] == "completed"
        stage = value["stage"]
        if (stage not in {"settings", "startup", "operation_state", "reservation_before", "retirement", "completion", "complete"}
                or complete != (stage == "complete")
                or value["retirement_started"] != (stage in {"retirement", "completion", "complete"})):
            raise ValueError
        startup = value["startup"]
        if startup is not None:
            validate_startup_report(startup)
        if stage not in {"settings", "startup"} and (startup is None or startup["status"] != "observed"):
            raise ValueError
        if stage in {"reservation_before", "retirement", "completion", "complete"}:
            if any(row["phase"] != "pending" or row["runner_epoch"] != 0 or row["lease_present"]
                   or row["error_present"] or row["effects_started"] for row in startup["operations"]):
                raise ValueError
        if complete:
            if value["error_type"] is not None:
                raise ValueError
            operations = value["operations"]
            if (not isinstance(operations, list) or len(operations) != len(startup["operations"])
                    or {row["operation_id"] for row in operations} != {row["operation_id"] for row in startup["operations"]}):
                raise ValueError
            for row in operations:
                if (set(row) != {"operation_id", "phase", "non_storage_released", "storage_preserved"}
                        or row["phase"] != "completed" or row["non_storage_released"] is not True
                        or row["storage_preserved"] is not True):
                    raise ValueError
        else:
            errors = {"ValueError", "KeyError", "ValidationError", "FileNotFoundError", "PermissionError", "SSLError",
                "TimeoutError", "OperationalError", "ProgrammingError", "InternalError", "StatementError",
                "HTTPStatusError", "ConnectError", "ConnectTimeout", "ReadTimeout", "RemoteProtocolError",
                "ProviderBlockedError", "ProviderRetryError", "ManagementError", "OtherError"}
            if value["operations"] != [] or value["error_type"] not in errors:
                raise ValueError
        return value
    except Exception:
        raise GatewayError("invalid retirement recovery report") from None


def validate_capacity_report(value: dict[str, Any]) -> dict[str, Any]:
    """Only closed failure phases and scheduler totals, never Pod configuration."""
    try:
        if (set(value) != {'schema', 'stage', 'kind', 'error_type', 'nodes'}
                or value['schema'] != 'loom.nebius-platform-capacity-diagnostic.v1'
                or value['stage'] not in {'render', 'inventory', 'autoscaling', 'validation',
                    'controller_decode', 'controller_count', 'pod_decode', 'node_decode',
                    'node_eligibility', 'placement', 'accounting', 'capacity'}
                or value['kind'] not in {None, 'Deployment', 'StatefulSet', 'ReplicaSet', 'DaemonSet',
                    'Job', 'CronJob', 'Pod', 'Node', 'HorizontalPodAutoscaler'}
                or value['error_type'] not in {'ValueError', 'KeyError', 'TypeError', 'AttributeError',
                    'ManagementCapacityError', 'ManagementPrerequisiteError', 'OtherError'}
                or not isinstance(value['nodes'], list) or len(value['nodes']) > 64):
            raise ValueError
        seen = set()
        for node in value['nodes']:
            uid = node['node_uid']
            if (set(node) != {'node_uid', 'placement_matches', 'allocatable', 'required'}
                    or str(UUID(uid)) != uid or not UUID(uid).int or uid in seen
                    or type(node['placement_matches']) is not bool):
                raise ValueError
            seen.add(uid)
            for key in ('allocatable', 'required'):
                totals = node[key]
                if not node['placement_matches']:
                    if totals is not None:
                        raise ValueError
                elif (not isinstance(totals, dict)
                        or set(totals) != {'cpu_millis', 'memory_mib', 'ephemeral_storage_mib', 'pods'}
                        or any(type(number) is not int or not 0 <= number < 2**63 for number in totals.values())):
                    raise ValueError
        return value
    except Exception:
        raise GatewayError('invalid platform capacity diagnostic') from None


def validate_telemetry_report(value: Any) -> dict[str, Any]:
    """Bounded sampling availability only; never capacity or readiness authority."""
    try:
        if not isinstance(value, dict) or set(value) != {'status', 'checks', 'unavailable', 'reasons'}:
            raise ValueError
        status, checks, unavailable, reasons = (value[key] for key in ('status', 'checks', 'unavailable', 'reasons'))
        if (type(checks) is not int or type(unavailable) is not int or not 0 <= unavailable <= checks < 2**31
                or not isinstance(reasons, list) or len(reasons) > len(OPTIONAL_TELEMETRY_STAGES)
                or any(not isinstance(reason, str) or reason not in OPTIONAL_TELEMETRY_STAGES for reason in reasons)
                or reasons != sorted(set(reasons)) or len(reasons) > unavailable):
            raise ValueError
        if status == 'not_observed':
            if checks or unavailable or reasons:
                raise ValueError
        elif status == 'available':
            if not checks or unavailable or reasons:
                raise ValueError
        elif status == 'unavailable':
            if not unavailable or not reasons:
                raise ValueError
        else:
            raise ValueError
        return {'status': status, 'checks': checks, 'unavailable': unavailable, 'reasons': list(reasons)}
    except Exception:
        raise GatewayError('invalid pool telemetry report') from None


def safe_report(raw: bytes, operation: dict[str, Any]) -> dict[str, Any]:
    try:
        validate_operation(operation)
        if len(raw) > 65536:
            raise ValueError()
        value = json.loads(raw)
        status = value["status"]
        upgrade = operation["schema"] == "loom.nebius-management-upgrade-operation.v1"
        retirement = operation["schema"] == "loom.nebius-management-retirement-operation.v1"
        diagnostic = operation["schema"] == "loom.nebius-management-retirement-diagnostic-operation.v1"
        recovery = operation["schema"] == "loom.nebius-management-retirement-recovery-operation.v1"
        refresh = operation['schema'] == 'loom.nebius-management-refresh-operation.v1'
        pool = operation['schema'] == 'loom.nebius-pool-cutover-operation.v1'
        success = ('pool_cutover_completed' if pool else "retirement_recovered" if recovery else "retirement_diagnostic_observed" if diagnostic else "management_retired" if retirement
            else "management_refreshed" if refresh else "management_upgraded" if upgrade else "management_installed")
        if status not in {"preflight_qualified", "pending", success, "blocked"}:
            raise ValueError()
        result = {"status": status}
        for key in ("source_sha", "candidate", "installation_id", "namespace"):
            if value[key] != operation[key]:
                raise ValueError()
            result[key] = value[key]
        if refresh or pool:
            if value.get('operation_id') != operation['operation_id']:
                raise ValueError()
            result['operation_id'] = value['operation_id']
        if status == "blocked":
            if not isinstance(value["stage"], str) or value["stage"] not in DIAGNOSTIC_STAGES:
                raise ValueError()
            result["stage"] = value["stage"]
        if pool:
            if 'telemetry' in value:
                result['telemetry'] = validate_telemetry_report(value['telemetry'])
            if status == 'pending':
                if value.get('phase') not in POOL_PHASES:
                    raise ValueError()
                result['phase'] = value['phase']
            elif status == success:
                if (value.get('outcome') not in {'global', 'legacy'} or value.get('acceptance_verified') is not False
                        or not isinstance(value.get('completion_sha256'), str)
                        or not re.fullmatch(r'[0-9a-f]{64}', value['completion_sha256'])):
                    raise ValueError()
                result.update(outcome=value['outcome'], completion_sha256=value['completion_sha256'], acceptance_verified=False)
            return result
        if 'capacity' in value:
            if not refresh or status != 'blocked' or value['stage'] != 'refresh_platform_capacity':
                raise ValueError()
            result['capacity'] = validate_capacity_report(value['capacity'])
        if status in {"pending", success}:
            uid, revision = value["namespace_uid"], value["revision"]
            if str(UUID(uid)) != uid or UUID(uid).int == 0 or not re.fullmatch(r"sha256:[0-9a-f]{64}", revision):
                raise ValueError()
            result.update(namespace_uid=uid, revision=revision)
        if status == "pending":
            phases = ({"retirement-recovery"} if recovery else {"retirement-diagnostic"} if diagnostic else {"retirement"} if retirement
                else {'config', 'retire', 'manager-probe', 'shared-probe', 'backup', 'migration', 'post-migration-probe', 'activate', 'public'}
                if refresh
                else {"admission", "authority", "database", "retirement", "retire", "migration", "activate", "service"}
                if upgrade else {"database", "migration", "backup", "service"})
            if value["phase"] not in phases:
                raise ValueError()
            result["phase"] = value["phase"]
        if status == "retirement_diagnostic_observed":
            result["probe"] = validate_startup_report(value["probe"])
        if recovery and (status == "retirement_recovered" or (status == "blocked" and value["stage"] == "recovery_runtime")):
            report = validate_recovery_report(value["recovery"])
            if (report["status"] == "completed") != (status == "retirement_recovered"):
                raise ValueError
            result["recovery"] = report
        if status == "management_installed":
            backup = value["backup"]
            uid, checksum, size, key = (backup[name] for name in ("job_uid", "sha256", "bytes", "key"))
            if (str(UUID(uid)) != uid or UUID(uid).int == 0 or not re.fullmatch(r"[0-9a-f]{64}", checksum)
                    or type(size) is not int or not 0 < size <= 1024**4
                    or not re.fullmatch(re.escape(operation["namespace"]) + r"/[0-9]{4}/[0-9]{2}/[0-9]{2}/[0-9]{6}-"
                                        + checksum[:12] + r"\.dump", key)):
                raise ValueError()
            result["backup"] = {"job_uid": uid, "sha256": checksum, "bytes": size, "key": key}
        return result
    except Exception:
        raise GatewayError("invalid management operation report") from None


def authorized_main(expected_sha256: str) -> int:
    action = COMMANDS.get(os.environ.get("SSH_ORIGINAL_COMMAND", ""))
    if action is None or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256):
        return 126
    content = sys.stdin.buffer.read(MAX_BUNDLE + 1)
    if not 0 < len(content) <= MAX_BUNDLE or hashlib.sha256(content).hexdigest() != expected_sha256:
        return 126
    try:
        _, operation = unpack_bundle(content)
        validate_action(action, operation)
        release = prepare_release(content)
        report = safe_report(run_private(command(release, action), timeout=1800), operation)
        if report["status"] != "blocked" and (action == "preflight") != (report["status"] == "preflight_qualified"):
            raise ValueError()
        if action == 'rollback' and report['status'] == 'pool_cutover_completed' and report['outcome'] != 'legacy':
            raise ValueError()
        print(json.dumps(report, sort_keys=True))
        return 0
    except Exception:
        print("protected management operation incomplete; preserve private recovery state", file=sys.stderr)
        return 1
