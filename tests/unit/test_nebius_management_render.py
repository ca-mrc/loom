"""Management installation must not become another task-execution stack."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from tests.unit.test_nebius_application_image_renderer import build_inputs as build_inputs
from tests.unit.test_nebius_application_render import inputs as application_inputs
from tests.unit.test_nebius_environment_contract import foundation_from
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def management_inputs(platform_inputs):
    config, candidate, profile = platform_inputs
    candidate["source_ref"] = "refs/heads/dev"
    candidate.update(schema_version="loom.nebius-candidate.v1", workflow_path=".github/workflows/nebius-candidate.yml",
                     run_id=123, registry_prefix="cr.eu-north1.nebius.cloud/test")
    installation = {
        "schema_version": "loom.nebius-management-installation.v1",
        "foundation": foundation_from(config).model_dump(mode="json"),
        "registry_prefix": "cr.eu-north1.nebius.cloud/test",
        "keyring": {"schema_version": 1, "keys": []},
        "publications": [],
        "platform_budget": {"cpu_millis": 2000, "memory_mib": 6000,
                            "storage_mib": 30000, "ephemeral_storage_mib": 30000},
        "provider_runtime": {
            "concurrency": 4, "poll_seconds": 5,
            "kubernetes": {"endpoint": config["kubernetes_api_server"],
                           "ca_file": "/var/run/loom-management-kubernetes/ca.crt",
                           "credentials_file": "/var/run/loom-management-kubernetes/credentials.json"},
            "cloud_credentials_file": "/var/run/loom-management-cloud/credentials.json",
        },
    }
    installation["foundation"]["provisioning_project_id"] = "project-managed-storage"
    deployment = {
        "schema_version": "loom.nebius-management-deployment.v1",
        "installation_id": "30000000-0000-4000-8000-000000000001",
        "namespace": "loom-nebius-management",
        "public_host": "manage.example.com",
        "postgres_storage_gi": 10,
        "backup_bucket": "loom-management-backup",
        "installation": installation,
    }
    return deployment, candidate, profile


def render(inputs):
    from loom_service.environment_management.deployment import (
        ManagementDeployment,
        render_management,
    )

    config, candidate, profile = inputs
    return render_management(ManagementDeployment.model_validate(config), candidate=candidate,
                             profile=profile, repo_root=ROOT)


def documents(result):
    return [doc for docs in result.files.values() for doc in docs]


def test_absent_pool_binding_preserves_historical_serialized_inputs(management_inputs):
    from loom_service.environment_management.deployment import ManagementDeployment

    raw, _, _ = management_inputs
    original = ManagementDeployment.model_validate(raw)
    explicit = ManagementDeployment.model_validate({**raw, 'pool_catalog_operation_id': None,
        'application_builder_machine_id': None})
    assert original.model_dump(mode='json') == explicit.model_dump(mode='json')
    assert 'pool_catalog_operation_id' not in original.model_dump(mode='json')
    assert json.loads(original.model_dump_json()) == original.model_dump(mode='json')
    assert render(management_inputs) == render(({**raw, 'pool_catalog_operation_id': None}, *management_inputs[1:]))


def test_pool_catalog_binding_rejects_nil_operation(management_inputs):
    from loom_service.environment_management.deployment import ManagementDeployment

    with pytest.raises(ValueError):
        ManagementDeployment.model_validate({**management_inputs[0], 'pool_catalog_operation_id': '00000000-0000-0000-0000-000000000000'})


def pod(doc):
    spec = doc["spec"]
    if doc["kind"] == "CronJob":
        spec = spec["jobTemplate"]["spec"]
    return spec["template"]["spec"]


def test_manager_has_only_its_own_database_api_backup_and_shared_ingress(management_inputs):
    before = copy.deepcopy(management_inputs)
    result = render(management_inputs)
    docs = documents(result)
    assert [(d["kind"], d["metadata"]["name"]) for d in docs
            if d["kind"] in {"Deployment", "StatefulSet", "Namespace"}] == [
        ("Namespace", "loom-nebius-management"), ("StatefulSet", "loom-postgres"),
        ("Deployment", "loom-service"),
    ]
    assert all(d["metadata"].get("namespace", "loom-nebius-management") == "loom-nebius-management" for d in docs)
    assert not any(d["kind"] in {"Secret", "ClusterRole", "ClusterRoleBinding"} for d in docs)
    assert not any(d.get("spec", {}).get("type") == "LoadBalancer" for d in docs)
    assert len([d for d in docs if d["kind"] == "Job"]) == 1
    assert len([d for d in docs if d["kind"] == "CronJob"]) == 1
    ingress = next(d for d in docs if d["kind"] == "Ingress")
    assert ingress["spec"]["ingressClassName"] == "loom-shared"
    assert ingress["spec"]["rules"] == [{"host": "manage.example.com", "http": {"paths": [{
        "path": "/", "pathType": "Prefix", "backend": {"service": {"name": "loom-service", "port": {"number": 8090}}},
    }]}}]
    assert ingress["spec"]["tls"] == [{"hosts": ["manage.example.com"]}]
    assert management_inputs == before


@pytest.mark.parametrize('guest', [False, True])
def test_manager_accepts_current_publication_without_inheriting_standalone_task_policy(management_inputs, guest):
    deployment, _, profile = management_inputs
    foundation = deployment["installation"]["foundation"]
    config = json.loads(foundation["platform_config_json"])
    config["task_identity_policy"] = {"mode": "private-root-v1", "target_id": config["target_id"],
                                      "execution_namespace": config["execution_namespace"]}
    if guest:
        config['guest_execution_target'] = {'target_id': 'nebius-guest-current'}
        config['emulated_auth_execution_target'] = {'target_id': 'nebius-auth-current'}
        profile.update(guest_runtime='qemu-tcg-v1', guest_runtime_volume_mib=1024,
                       guest_max_artifact_bytes=64 * 1024**2, supports_emulated_pkcs11=True)
    foundation["platform_config_json"] = json.dumps(config)
    profile["supports_task_identity"] = True
    before = copy.deepcopy(management_inputs)
    result = render(management_inputs)
    assert management_inputs == before
    assert not any(doc["kind"].startswith("ValidatingAdmissionPolicy") for doc in documents(result))
    assert all(doc["metadata"].get("namespace", deployment["namespace"]) == deployment["namespace"]
               for doc in documents(result))
    assert "task_identity_policy" not in result.config
    assert 'guest_execution_target' not in result.config
    assert 'emulated_auth_execution_target' not in result.config
    namespace = next(doc for doc in documents(result) if doc["kind"] == "Namespace")
    assert namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"


def test_runtime_mounts_only_explicit_separate_authorities(management_inputs):
    docs = documents(render(management_inputs))
    service = next(d for d in docs if d["kind"] == "Deployment")
    template = pod(service)
    env = {row["name"]: row for row in template["containers"][0]["env"]}
    assert env["LOOM_SVC_SERVICE_MODE"]["value"] == "management"
    assert env["LOOM_SVC_AUTH_LOCAL_HTTP"]["value"] == "false"
    assert env["LOOM_SVC_PUBLIC_BASE_URL"]["value"] == "https://manage.example.com"
    assert env["LOOM_SVC_ENVIRONMENT_MANAGEMENT_CONFIG_FILE"]["value"] == "/var/run/loom-management/installation.json"
    assert env["LOOM_SVC_ENVIRONMENT_MANAGEMENT_GITHUB_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": "loom-management-publications", "key": "token",
    }
    assert env["LOOM_SVC_DB_URL"]["valueFrom"]["secretKeyRef"]["key"] == "service-url"
    assert not any("SOURCE" in name or "MINIO" in name or "BATCH_RUNNER" in name for name in env)
    assert template["automountServiceAccountToken"] is False
    volumes = {v["name"]: v for v in template["volumes"]}
    assert volumes["management-cloud"]["secret"]["secretName"] == "loom-management-cloud"
    assert volumes["management-kubernetes"]["secret"]["secretName"] == "loom-management-kubernetes"
    assert volumes["management-cloud"]["secret"]["defaultMode"] == 0o440
    assert template["containers"][0]["readinessProbe"]["httpGet"]["path"] == "/api/v1/health/ready"
    for doc in docs:
        if doc["kind"] in {"StatefulSet", "Job", "CronJob"}:
            text = json.dumps(pod(doc))
            assert "loom-management-cloud" not in text
            assert "loom-management-kubernetes" not in text
            assert "loom-management-publications" not in text
            for volume in pod(doc).get("volumes", []):
                if volume.get("configMap", {}).get("name") == "loom-platform-config":
                    assert volume["configMap"]["items"] == [{"key": "environment.json", "path": "environment.json"}]


def test_projected_identity_is_renewed_and_mounted_only_into_management_api(management_inputs):
    runtime = management_inputs[0]["installation"]["provider_runtime"]
    runtime["kubernetes"].pop("credentials_file")
    runtime["kubernetes"].update(kind="projected_service_account", token_file="/var/run/loom-management-kubernetes/token")
    docs = documents(render(management_inputs))
    accounts = [d["metadata"]["name"] for d in docs if d["kind"] == "ServiceAccount"]
    assert set(accounts) == {"loom-platform", "loom-management-provisioner"}
    for doc in docs:
        if doc["kind"] not in {"Deployment", "StatefulSet", "Job", "CronJob"}:
            continue
        template = pod(doc)
        assert template["automountServiceAccountToken"] is False
        volumes = {v["name"]: v for v in template["volumes"]}
        if doc["kind"] == "Deployment":
            assert template["serviceAccountName"] == "loom-management-provisioner"
            assert volumes["management-kubernetes"] == {"name": "management-kubernetes", "projected": {
                "defaultMode": 0o440, "sources": [
                    {"serviceAccountToken": {"path": "token", "expirationSeconds": 3600}},
                    {"configMap": {"name": "kube-root-ca.crt", "items": [{"key": "ca.crt", "path": "ca.crt"}]}},
                ],
            }}
        else:
            assert template["serviceAccountName"] == "loom-platform"
            assert "management-kubernetes" not in volumes


def test_projected_render_rejects_unmounted_token_path(management_inputs):
    runtime = management_inputs[0]["installation"]["provider_runtime"]
    runtime["kubernetes"].pop("credentials_file")
    runtime["kubernetes"].update(kind="projected_service_account", token_file="/var/run/ambient/token")
    with pytest.raises(ValueError, match="mounted credentials"):
        render(management_inputs)


def test_migration_and_backup_never_start_task_authorities_or_share_child_data(management_inputs):
    docs = documents(render(management_inputs))
    job = next(d for d in docs if d["kind"] == "Job")
    container = pod(job)["containers"][0]
    assert container["command"] == ["python", "-m", "loom.nebius_platform_bootstrap", "management-database"]
    assert {e["name"] for e in container["env"]} == {"LOOM_PLATFORM_CONFIG", "LOOM_DB_URL", "LOOM_DB_SERVICE_PASSWORD"}
    db = next(d for d in docs if d["kind"] == "StatefulSet")
    assert db["spec"]["volumeClaimTemplates"][0]["spec"]["resources"]["requests"]["storage"] == "10Gi"
    assert db["spec"]["persistentVolumeClaimRetentionPolicy"] == {"whenDeleted": "Retain", "whenScaled": "Retain"}
    backup = next(d for d in docs if d["kind"] == "CronJob")
    backup_pod = pod(backup)
    assert backup["spec"]["concurrencyPolicy"] == "Forbid"
    assert backup_pod["initContainers"][0]["command"][0] == "pg_dump"
    assert all("PGPASSWORD" != e["name"] for e in backup_pod["containers"][0]["env"])
    assert not any("LOOM_BACKUP" in e["name"] for e in backup_pod["initContainers"][0]["env"])
    configmap = next(d for d in docs if d["kind"] == "ConfigMap")
    config = json.loads(configmap["data"]["environment.json"])
    assert config["namespace"] == "loom-nebius-management"
    assert config["buckets"]["backup"] == "loom-management-backup"
    assert json.loads(configmap["data"]["installation.json"]) == management_inputs[0]["installation"]


def test_only_shared_ingress_can_reach_management_and_database_is_namespace_local(management_inputs):
    docs = documents(render(management_inputs))
    policies = {d["metadata"]["name"]: d["spec"] for d in docs if d["kind"] == "NetworkPolicy"}
    assert policies["default-deny-ingress"]["ingress"] == []
    assert policies["management-api"]["ingress"] == [{"from": [{
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "loom-ingress"}},
        "podSelector": {"matchLabels": {"app.kubernetes.io/name": "loom-ingress"}},
    }], "ports": [{"protocol": "TCP", "port": 8090}]}]
    assert policies["postgres-private"]["ingress"][0]["from"] == [{
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "loom-nebius-management"}},
    }]


def test_management_envelope_counts_rollout_migration_database_and_backup_scratch(management_inputs):
    result = render(management_inputs)
    # Service steady+surge (2*100), database100, migration100, backup max100.
    assert result.platform_envelope.cpu_millis == 500
    assert result.platform_envelope.memory_mib == 1280
    assert result.platform_envelope.storage_mib == 10240
    assert result.platform_envelope.ephemeral_storage_mib == 11264
    for doc in documents(result):
        if doc["kind"] in {"Deployment", "StatefulSet", "Job", "CronJob"}:
            p = pod(doc)
            assert p["nodeSelector"] == {"loom.nebius/node-role": "system", "loom.nebius/platform": "integration"}
            assert p["automountServiceAccountToken"] is False
            for c in p.get("initContainers", []) + p["containers"]:
                assert int(c["resources"]["requests"]["ephemeral-storage"].removesuffix("Mi")) > 0


@pytest.mark.parametrize("path,value", [
    (("namespace",), "loom-nebius-platform"),
    (("public_host",), "alice.dev.example.com"),
    (("public_host",), "nebius.yylx.world"),
    (("backup_bucket",), "loom-integration-backup"),
    (("installation_id",), "00000000-0000-0000-0000-000000000000"),
    (("postgres_storage_gi",), True),
    (("installation", "provider_runtime"), None),
    (("installation", "provider_runtime", "cloud_credentials_file"), "/root/operator.json"),
    (("installation", "provider_runtime", "kubernetes", "endpoint"), "https://other.example.com"),
    (("installation", "provider_runtime", "kubernetes", "credentials_file"), "/root/kubeconfig"),
])
def test_rejects_shared_bindings_unmounted_credentials_and_wrong_cluster(management_inputs, path, value):
    data = management_inputs[0]
    for key in path[:-1]:
        data = data[key]
    data[path[-1]] = value
    with pytest.raises(ValueError):
        render(management_inputs)


def test_management_config_changes_roll_service_and_migration_together(management_inputs):
    first = render(management_inputs)
    management_inputs[0]["installation"]["platform_budget"]["cpu_millis"] += 100
    second = render(management_inputs)
    assert first.revision != second.revision
    for result in (first, second):
        for doc in documents(result):
            assert doc["metadata"]["labels"]["loom.nebius/management-installation"] == "30000000-0000-4000-8000-000000000001"
            if doc["kind"] in {"Deployment", "StatefulSet", "Job", "CronJob"}:
                spec = doc["spec"]["jobTemplate"]["spec"] if doc["kind"] == "CronJob" else doc["spec"]
                assert spec["template"]["metadata"]["annotations"]["loom.nebius/configuration-revision"] == result.revision


@pytest.mark.parametrize("source", ["refs/heads/feature/test", "refs/heads/codex/nebius-main"])
def test_manager_cannot_be_installed_from_personal_or_retired_source(management_inputs, source):
    management_inputs[1]["source_ref"] = source
    with pytest.raises(ValueError, match="protected dev"):
        render(management_inputs)


def test_management_material_is_fresh_and_does_not_include_worker_or_cloud_credentials():
    from loom_service.environment_management.credentials import generate_management_material

    first = generate_management_material(namespace="loom-nebius-management")
    second = generate_management_material(namespace="loom-nebius-management")
    assert set(first) == {"loom-platform-db", "loom-management-db-tls", "loom-platform-auth", "loom-admin-secret"}
    assert set(first["loom-platform-db"]) == {"ca.crt", "postgres-password", "admin-url", "service-password", "service-url"}
    assert set(first["loom-platform-auth"]) == {"secret-store-master-key"}
    assert first["loom-platform-auth"] != second["loom-platform-auth"]
    assert first["loom-admin-secret"] != second["loom-admin-secret"]
    assert first["loom-platform-db"]["service-password"] != second["loom-platform-db"]["service-password"]


@pytest.mark.parametrize("image", ["cr.eu-north1.nebius.cloud/test/service:dev",
                                  "cr.eu-north1.nebius.cloud/other/service@sha256:" + "b" * 64])
def test_management_image_requires_bound_registry_and_immutable_digest(management_inputs, image):
    management_inputs[1]["images"]["service"]["image_ref"] = image
    management_inputs[2]["task_image_ref"] = image
    with pytest.raises(ValueError, match="management image"):
        render(management_inputs)


@pytest.fixture
def application_management_inputs(management_inputs, platform_inputs):
    from loom.nebius_application_authority import ApplicationNamespaceAuthorityV1

    data = management_inputs[0]
    _, release, shared, _ = application_inputs(platform_inputs)
    installation = data['installation']
    installation['provider_runtime'] = None
    installation['applications'] = {
        'shared': shared.model_dump(mode='json'), 'releases': [release.model_dump(mode='json')],
        'authority': ApplicationNamespaceAuthorityV1(installation_id=data['installation_id'],
            namespace=data['namespace'], cluster_id=shared.cluster_id,
            data_environment_id=shared.data_environment_id,
            shared_namespace=shared.platform_namespace).model_dump(mode='json'),
        'storage': {'data_environment_id': str(shared.data_environment_id), 'project_id': 'project-managed-storage',
            'data_group_id': 'group-shared-data', 'source_group_id': 'group-shared-source'},
        'runtime': {'concurrency': 4, 'poll_seconds': 5, 'kubernetes': {'kind': 'projected_service_account',
            'endpoint': platform_inputs[0]['kubernetes_api_server'],
            'ca_file': '/var/run/loom-management-kubernetes/ca.crt',
            'token_file': '/var/run/loom-management-kubernetes/token'},
            'cloud_credentials_file': '/var/run/loom-applications-cloud/credentials.json',
            'database_connection_file': '/var/run/loom-applications-shared/manager-dsn',
            'shared_credentials_file': '/var/run/loom-applications-shared/shared.json'},
    }
    return management_inputs


def test_application_manager_uses_its_own_account_and_versioned_configuration(application_management_inputs):
    before = copy.deepcopy(application_management_inputs)
    result = render(application_management_inputs)
    docs = documents(result)
    suffix = result.revision[7:19]
    accounts = {doc['metadata']['name'] for doc in docs if doc['kind'] == 'ServiceAccount'}
    assert accounts == {'loom-platform', 'loom-application-provisioner'}
    service = next(doc for doc in docs if doc['kind'] == 'Deployment')
    template = pod(service)
    assert template['serviceAccountName'] == 'loom-application-provisioner'
    assert template['automountServiceAccountToken'] is False
    volumes = {volume['name']: volume for volume in template['volumes']}
    assert volumes['management-kubernetes']['projected']['sources'][0] == {
        'serviceAccountToken': {'path': 'token', 'expirationSeconds': 3600}}
    configs = {doc['metadata']['name']: doc for doc in docs if doc['kind'] == 'ConfigMap'}
    name = 'loom-management-applications-' + suffix
    assert configs[name]['immutable'] is True
    assert json.loads(configs[name]['data']['installation.json'])['applications'] == before[0]['installation']['applications']
    assert 'installation.json' not in configs['loom-platform-config']['data']
    assert volumes['management-config']['configMap']['name'] == name
    assert volumes['management-cloud']['secret']['secretName'] == 'loom-applications-cloud-' + suffix
    assert volumes['application-shared']['secret']['secretName'] == 'loom-applications-shared-' + suffix
    assert volumes['application-shared']['secret']['items'] == [
        {'key': key, 'path': key} for key in ('manager-dsn', 'shared.json', 'ca.crt')]
    mounts = {mount['name']: mount for mount in template['containers'][0]['volumeMounts']}
    assert mounts['management-cloud']['mountPath'] == '/var/run/loom-applications-cloud'
    assert mounts['application-shared'] == {'name': 'application-shared',
        'mountPath': '/var/run/loom-applications-shared', 'readOnly': True}
    env = {row['name']: row for row in template['containers'][0]['env']}
    assert env['LOOM_SVC_DB_URL']['valueFrom']['secretKeyRef'] == {'name': 'loom-platform-db', 'key': 'service-url'}
    assert env['LOOM_SECRET_STORE_MASTER_KEY']['valueFrom']['secretKeyRef'] == {
        'name': 'loom-platform-auth', 'key': 'secret-store-master-key'}
    assert not any(doc['kind'] in {'Secret', 'PersistentVolumeClaim'} for doc in docs)
    assert [doc['metadata']['name'] for doc in docs if doc['kind'] == 'StatefulSet'] == ['loom-postgres']
    for doc in docs:
        if doc['kind'] in {'StatefulSet', 'Job', 'CronJob'}:
            assert not any('application' in volume['name'] or 'management' in volume['name']
                           for volume in pod(doc).get('volumes', []))
    assert application_management_inputs == before


@pytest.fixture
def source_management_inputs(application_management_inputs):
    data = application_management_inputs[0]
    data['installation']['applications']['runtime']['source_upload'] = {
        'credentials_file': '/var/run/loom-application-source-credentials/credentials.json',
        'spool_directory': '/var/run/loom-application-source/spool', 'max_inflight': 2,
    }
    return application_management_inputs


@pytest.fixture
def builder_management_inputs(source_management_inputs, build_inputs):
    data = source_management_inputs[0]
    application = data['installation']['applications']
    platform = json.loads(data['installation']['foundation']['platform_config_json'])
    claim = build_inputs[0]
    data['pool_catalog_operation_id'] = str(uuid4())
    data['application_builder_machine_id'] = str(uuid4())
    application['runtime']['build'] = {
        'binding': {
            'source': {'installation_id': data['installation_id'],
                'data_environment_id': application['shared']['data_environment_id'],
                'cluster_id': application['shared']['cluster_id'], 'source_bucket': platform['buckets']['source']},
            'recipe': claim.recipe.model_copy(update={'schema_revision': application['shared']['schema_revision']}).model_dump(mode='json'),
            'storage_endpoint': platform['storage_endpoint'], 'storage_region': platform['region'],
            'registry_repository': claim.registry_repository, 'pool_id': str(uuid4()),
            'participant_id': str(uuid4()), 'profile_id': str(uuid4()), 'target_id': 'application-builder',
            'admission_epoch': 1, 'participant_revision': 1,
        },
        'management_origin': 'https://' + data['public_host'],
        'bearer_token_file': '/var/run/loom-pool-token/token',
    }
    return source_management_inputs


def test_source_runtime_has_private_bounded_spool_and_management_only_material(source_management_inputs, tmp_path):
    result = render(source_management_inputs)
    docs = documents(result)
    service, = [doc for doc in docs if doc['kind'] == 'Deployment']
    template = pod(service)
    volumes = {item['name']: item for item in template['volumes']}
    assert volumes['application-source']['emptyDir'] == {'sizeLimit': '4096Mi'}
    assert volumes['application-source-credentials']['secret'] == {
        'secretName': 'loom-applications-source-' + result.revision[7:19], 'defaultMode': 0o440,
        'items': [{'key': 'credentials.json', 'path': 'credentials.json'}]}
    main, = template['containers']
    assert main['resources']['requests']['ephemeral-storage'] == '4352Mi'
    assert main['resources']['limits']['ephemeral-storage'] == '4352Mi'
    initializer, = [item for item in template['initContainers'] if item['name'] == 'prepare-application-source']
    assert initializer['securityContext']['runAsNonRoot'] is True
    assert not initializer['securityContext']['allowPrivilegeEscalation']
    command = initializer['command']
    directory = tmp_path / 'private-spool'
    subprocess.run([sys.executable, *command[1:-1], str(directory)], check=True)
    directory.chmod(0o755)
    rejected = subprocess.run([sys.executable, *command[1:-1], str(directory)], capture_output=True)
    assert rejected.returncode != 0
    directory.chmod(0o700)
    linked = tmp_path / 'linked-spool'
    linked.symlink_to(directory, target_is_directory=True)
    rejected = subprocess.run([sys.executable, *command[1:-1], str(linked)], capture_output=True)
    assert rejected.returncode != 0
    without_source = copy.deepcopy(source_management_inputs)
    without_source[0]['installation']['applications']['runtime'].pop('source_upload')
    # Compare equal Recreate strategies: rolling management also reserves surge.
    without_source[0]['pool_catalog_operation_id'] = str(uuid4())
    assert result.platform_envelope.ephemeral_storage_mib == render(without_source).platform_envelope.ephemeral_storage_mib + 4096
    assert directory.stat().st_mode & 0o777 == 0o700
    # Restarting an init container must preserve the private directory safely.
    subprocess.run([sys.executable, *command[1:-1], str(directory)], check=True)
    for doc in docs:
        if doc['kind'] in {'StatefulSet', 'Job', 'CronJob'}:
            assert not any(item['name'].startswith('application-source') for item in pod(doc)['volumes'])


def test_builder_runtime_uses_dedicated_private_token_and_readonly_native_observation(builder_management_inputs):
    data = builder_management_inputs[0]
    result = render(builder_management_inputs)
    docs = documents(result)
    service, = [doc for doc in docs if doc['kind'] == 'Deployment']
    template = pod(service)
    volumes = {item['name']: item for item in template['volumes']}
    assert volumes['pool-token-source']['secret']['secretName'] == (
        'loom-pool-machine-' + data['application_builder_machine_id'].replace('-', ''))
    assert service['spec']['strategy'] == {'type': 'Recreate'}
    assert any(item['name'] == 'prepare-pool-token' for item in template['initContainers'])
    platform = json.loads(data['installation']['foundation']['platform_config_json'])
    reader, = [doc for doc in docs if doc['kind'] == 'Role']
    assert reader['metadata']['namespace'] == platform['execution_namespace'] + '-build'
    assert reader['rules'] == [
        {'apiGroups': ['batch'], 'resources': ['jobs'], 'verbs': ['get']},
        {'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list']},
        {'apiGroups': [''], 'resources': ['pods/log'], 'verbs': ['get']},
    ]
    binding, = [doc for doc in docs if doc['kind'] == 'RoleBinding']
    assert binding['subjects'] == [{'kind': 'ServiceAccount', 'name': 'loom-application-provisioner',
        'namespace': data['namespace']}]
    assert binding['roleRef']['name'] == reader['metadata']['name']
    assert binding['metadata']['namespace'] == reader['metadata']['namespace']


@pytest.mark.parametrize('damage', ['credentials', 'spool', 'token', 'origin', 'machine', 'catalog', 'nil-machine'])
def test_builder_delivery_rejects_unmounted_or_unbound_runtime(builder_management_inputs, damage):
    from loom_service.environment_management.deployment import ManagementDeployment

    data = builder_management_inputs[0]
    runtime = data['installation']['applications']['runtime']
    if damage in {'credentials', 'spool'}:
        runtime['source_upload']['credentials_file' if damage == 'credentials' else 'spool_directory'] = '/ambient/path'
    elif damage == 'token':
        runtime['build']['bearer_token_file'] = '/ambient/token'
    elif damage == 'origin':
        runtime['build']['management_origin'] = 'https://foreign.example.com'
    elif damage == 'nil-machine':
        data['application_builder_machine_id'] = '00000000-0000-0000-0000-000000000000'
    else:
        data.pop('application_builder_machine_id' if damage == 'machine' else 'pool_catalog_operation_id')
    with pytest.raises(ValueError):
        ManagementDeployment.model_validate(data)


@pytest.mark.parametrize('path,value', [
    (('authority', 'installation_id'), '30000000-0000-4000-8000-000000000002'),
    (('authority', 'namespace'), 'loom-nebius-management-foreign'),
    (('runtime', 'kubernetes', 'endpoint'), 'https://foreign.example.com'),
    (('runtime', 'kubernetes', 'ca_file'), '/ambient/ca.crt'),
    (('runtime', 'kubernetes', 'token_file'), '/ambient/token'),
    (('runtime', 'cloud_credentials_file'), '/ambient/cloud.json'),
    (('runtime', 'database_connection_file'), '/ambient/manager-dsn'),
    (('runtime', 'shared_credentials_file'), '/ambient/shared.json'),
])
def test_application_management_rejects_wrong_authority_and_unmounted_material(application_management_inputs, path, value):
    config = application_management_inputs[0]['installation']['applications']
    for key in path[:-1]:
        config = config[key]
    config[path[-1]] = value
    with pytest.raises(ValueError, match='application management'):
        render(application_management_inputs)
