#!/usr/bin/env python3
"""Connected private ingress operation: live capacity before staged installation."""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Protocol
from uuid import UUID

from cryptography import x509
from scripts.ops import nebius_certificates as private_state
from scripts.ops import nebius_ingress_probe as probes
from scripts.ops.nebius_dns_publication import validate_target
from scripts.ops.nebius_ingress_cutover import MARKER, KubectlCutoverAPI, cutover, rollback
from scripts.ops.nebius_ingress_cutover import _identity as resource_identity
from scripts.ops.nebius_ingress_cutover import _stable as stable_resource
from scripts.ops.nebius_ingress_gateway import (
    ControllerAPI,
    IngressError,
    TLSBinding,
    _forward_port,
    deliver_tls,
    qualify_controller,
)
from scripts.ops.nebius_ingress_image import DIGEST
from scripts.ops.nebius_ingress_stage import KubectlStageAPI, StageAPI, stage_controller
from scripts.ops.nebius_ingress_stage import _snapshot as staged_snapshot

from loom.nebius_environment_contract import FoundationBinding
from loom.nebius_shared_ingress import SharedIngressInstallation
from loom_execution_capacity_collector import kubernetes as accounting

LIVE_POD_FIELD_SELECTOR = "status.phase!=Succeeded,status.phase!=Failed"


def _origin_snapshot(document: dict[str, Any]) -> dict[str, Any]:
    """Preserve origin identity/configuration checks across ordinary kubectl apply."""
    snapshot = staged_snapshot(document)
    metadata = snapshot["metadata"]
    annotations = metadata.get("annotations", {})
    # The ordinary platform deployer also owns this Service's unchanged routing
    # declaration. Its client-side apply adds/updates bookkeeping after initial
    # create-only staging. Ignore only that inert annotation, never the stage
    # operation/installation markers or any runtime fields. Do not edit journals.
    annotations.pop("kubectl.kubernetes.io/last-applied-configuration", None)
    if not annotations:
        metadata.pop("annotations", None)
    return snapshot


class OperationError(RuntimeError):
    """Payload-free installation failure; never expose inventory or credentials."""


class InstallationAPI(ControllerAPI, Protocol):
    binding: TLSBinding
    candidate: str
    image: str

    def foundation(self) -> FoundationBinding: ...
    def capacity(self) -> dict[str, Any]: ...
    def staging(self, installation: SharedIngressInstallation) -> StageAPI: ...
    def read(self) -> tuple[dict[str, Any], dict[str, Any]]: ...
    def patch(self, before: dict[str, Any], after: dict[str, Any]) -> None: ...
    def guard(self, action: str, owner: str, candidate: str) -> dict[str, Any]: ...
    def probe_legacy_pod(self, pod: dict[str, Any]) -> None: ...
    def probe_public(self, receipt: dict[str, Any]) -> None: ...
    def restore(self, before: dict[str, Any], after: dict[str, Any], owner: str) -> None: ...
    def probe_original_backend(self, origin: dict[str, Any]) -> None: ...
    def probe_original_public(self) -> None: ...


class _InstalledIngress:
    """Connect cutover's checks to the exact objects just staged by this operation."""

    def __init__(self, api: InstallationAPI, installation: SharedIngressInstallation,
                 tls: dict[str, Any], stage: dict[str, Any], staging: StageAPI, state: Path):
        self.api, self.installation = api, installation
        self.tls, self.stage, self.staging, self.state = tls, stage, staging, state
        self.deployment_uid = str(stage["resource_uids"][f"Deployment:{api.binding.namespace}:loom-shared-ingress"])

    def controller(self) -> dict[str, Any]:
        return qualify_controller(binding=self.api.binding, api=self.api, deployment_uid=self.deployment_uid,
                                  image=self.api.image, tls_receipt=self.tls)

    def qualify(self) -> None:
        if self.api.foundation() != self.installation.foundation:
            raise OperationError("live configuration changed during ingress installation")
        self.api.capacity()
        journal = self.state / (self.api.binding.installation_id + ".json")
        record = json.loads(private_state._private_read(journal, limit=1024 * 1024))
        if record.get("status") != "controller_staged":
            raise OperationError("completed initial staging journal required for cutover")
        if stage_controller(self.installation, binding=self.api.binding, api=self.staging, state_dir=self.state) != self.stage:
            raise OperationError("staging identity changed before cutover")
        proof = self.controller()
        pods = self.api.list_controller_pods(self.api.binding.namespace)
        if [p["metadata"]["uid"] for p in pods] != proof["pod_uids"]:
            raise OperationError("controller membership changed before legacy probe")
        for pod in pods:
            self.api.probe_legacy_pod(pod)
        if [p["metadata"]["uid"] for p in self.api.list_controller_pods(self.api.binding.namespace)] != proof["pod_uids"]:
            raise OperationError("controller membership changed during legacy probe")

    def read(self) -> tuple[dict[str, Any], dict[str, Any]]:
        return self.api.read()

    def patch(self, before: dict[str, Any], after: dict[str, Any]) -> None:
        self.api.patch(before, after)

    def guard(self, action: str, owner: str, candidate: str) -> dict[str, Any]:
        return self.api.guard(action, owner, candidate)

    def public_probe(self) -> None:
        self.api.probe_public(self.tls)


class _RecoveryIngress:
    def __init__(self, api: InstallationAPI, origin: dict[str, Any]):
        self.api, self.origin = api, origin

    def read(self) -> tuple[dict[str, Any], dict[str, Any]]:
        self.api.foundation()
        return self.api.read()

    def guard(self, action: str, owner: str, candidate: str) -> dict[str, Any]:
        return self.api.guard(action, owner, candidate)

    def restore(self, before: dict[str, Any], after: dict[str, Any], owner: str) -> None:
        self.api.restore(before, after, owner)

    def probe_original_backend(self) -> None:
        self.api.probe_original_backend(self.origin)

    def probe_original_public(self) -> None:
        self.api.probe_original_public()


def rollback_ingress(*, api: InstallationAPI, state_dir: Path) -> dict[str, Any]:
    """Explicit paused recovery using retained original-route ownership evidence."""
    try:
        with private_state._locked_state(state_dir):
            api.foundation()
            record = json.loads(private_state._private_read(
                state_dir / "stage" / (api.binding.installation_id + ".json"), limit=1024 * 1024,
            ))
            if record["status"] != "controller_staged" or record["binding"] != asdict(api.binding):
                raise OperationError("original ingress staging identity is unavailable")
            origin = record["resources"][f"Service:{api.binding.namespace}:loom-web-origin"]
            if origin["status"] != "created" or not origin["uid"] or not origin["observed"]:
                raise OperationError("retained legacy origin identity is unavailable")
            return rollback(api=_RecoveryIngress(api, origin), state_dir=state_dir / "cutover",
                            installation_id=api.binding.installation_id, candidate=api.candidate, namespace=api.binding.namespace)
    except OperationError:
        raise
    except Exception:
        raise OperationError("ingress recovery incomplete; preserve journals and any owned pause") from None


def install_ingress(*, api: InstallationAPI, certificate_config: dict[str, Any], state_dir: Path,
                    qualification_timeout: int = 180, now: datetime | None = None,
                    roots: Sequence[x509.Certificate] | None = None) -> dict[str, Any]:
    """Deliver → stage → prove current Pods/legacy route → guarded public cutover.

    Trust roots/clock are injectable for disposable qualification only; installed
    callers use the real clock and system trust. The protected transport is the
    authority for all arguments. No arbitrary manifests or tenant inputs enter.
    """
    try:
        if type(qualification_timeout) is not int or not 1 <= qualification_timeout <= 600:
            raise OperationError("invalid ingress qualification deadline")
        with private_state._locked_state(state_dir):
            foundation = api.foundation()
            api.capacity()
            tls = deliver_tls(certificate_config, binding=api.binding, api=api, now=now, roots=roots)
            if api.foundation() != foundation:
                raise OperationError("live foundation changed before staging")
            installation = SharedIngressInstallation(
                installation_id=UUID(api.binding.installation_id), foundation=foundation,
                image=api.image, tls_secret_name=tls["secret_name"],
            )
            staging = api.staging(installation)
            stage_state = state_dir / "stage"
            stage = stage_controller(installation, binding=api.binding, api=staging, state_dir=stage_state)
            connected = _InstalledIngress(api, installation, tls, stage, staging, stage_state)
            deadline = time.monotonic() + qualification_timeout
            while True:
                try:
                    connected.controller()
                    break
                except IngressError:
                    if time.monotonic() >= deadline:
                        raise OperationError("current ingress Pods did not qualify before deadline") from None
                    time.sleep(min(2, max(0, deadline - time.monotonic())))
            # Qualification includes the legacy route BEFORE any guard or public
            # mutation. cutover rechecks it again before each mutation/release.
            result = cutover(api=connected, state_dir=state_dir / "cutover", installation_id=api.binding.installation_id,
                             candidate=api.candidate, namespace=api.binding.namespace)
            return {**result, "controller_uid": connected.deployment_uid, "secret_uid": tls["secret_uid"],
                    "fingerprint_sha256": tls["fingerprint_sha256"]}
    except OperationError:
        raise
    except Exception:
        raise OperationError("ingress installation incomplete; preserve journals and any owned rollout pause") from None


def qualify_dns_target(*, api: InstallationAPI, certificate_config: dict[str, Any],
                       state_dir: Path) -> dict[str, Any]:
    """Observe an installed route; never deliver, stage, cut over or pause it.

    Historical cutover owns the public allocation, not the current application
    revision. The exact installed authority and fresh HTTPS probes bind the
    current candidate, allowing ordinary approved application upgrades.
    """
    try:
        binding = api.binding
        root = Path(certificate_config["state_dir"])
        if (not state_dir.is_dir() or not root.is_dir()
                or certificate_config["installation_id"] != binding.certificate_installation_id
                or certificate_config["child_domain"] != binding.child_domain
                or certificate_config["management_host"] != binding.management_host):
            raise OperationError("installed ingress and certificate state required")
        # Match installation's lock order. Never invoke its mutating entrypoint.
        with private_state._locked_state(state_dir), private_state._locked_state(root):
            if private_state.load_installation(root / "installation.json") != certificate_config:
                raise OperationError("certificate installation state differs")
            record = json.loads(private_state._private_read(state_dir / "cutover/cutover.json", limit=2 * 1024 * 1024))
            identity = record["binding"]
            if (record["phase"] != "complete"
                    or identity != {"installation_id": binding.installation_id,
                                    "namespace": binding.namespace, "candidate": identity["candidate"]}
                    or not re.fullmatch(r"[0-9a-f]{40}", identity["candidate"])
                    or str(UUID(record["owner"])) != record["owner"] or UUID(record["owner"]).int == 0):
                raise OperationError("completed bound ingress cutover required")
            stage = json.loads(private_state._private_read(
                state_dir / "stage" / (binding.installation_id + ".json"), limit=1024 * 1024,
            ))
            if (stage["schema"] != "loom.nebius-ingress-stage.v1" or stage["status"] != "controller_staged"
                    or stage["binding"] != asdict(binding)):
                raise OperationError("completed bound controller staging required")
            controller = stage["resources"][f"Deployment:{binding.namespace}:loom-shared-ingress"]
            if controller["status"] != "created" or not controller["uid"]:
                raise OperationError("recorded controller identity required")
            selected = private_state._selected(root)
            if selected is None:
                raise OperationError("selected delivered certificate required")
            tls = json.loads(private_state._private_read(
                root / "deliveries" / (binding.installation_id + "-" + selected["generation"] + ".json"),
            ))
            if (tls["schema"] != "loom.nebius-ingress-tls.v1" or tls["binding"] != asdict(binding)
                    or tls["status"] != "tls_delivered" or tls["certificate_generation"] != selected["generation"]
                    or tls["fingerprint_sha256"] != selected["fingerprint_sha256"]
                    or selected["sans"] != ["*." + binding.child_domain, binding.management_host]):
                raise OperationError("selected certificate delivery is unqualified")

            def observe() -> tuple[dict[str, Any], dict[str, Any]]:
                api.verify_identity(binding)
                service, config = api.read()
                for current, key, kind, name in ((service, "service_after", "Service", "loom-web"),
                                                 (config, "config_after", "ConfigMap", "loom-platform-config")):
                    resource_identity(current, kind=kind, name=name, namespace=binding.namespace)
                    if (current["metadata"]["uid"] != record[key]["metadata"]["uid"]
                            or current["metadata"].get("annotations", {}).get(MARKER) != record["owner"]):
                        raise OperationError("completed public routing ownership differs")
                environment = json.loads(config["data"]["environment.json"])
                previous = json.loads(record["config_before"]["data"]["environment.json"])
                if (service["spec"] != record["service_after"]["spec"]
                        or service["spec"]["type"] != "LoadBalancer"
                        or service["spec"]["selector"] != {"app": "loom-shared-ingress"}
                        or service["status"]["loadBalancer"] != record["service_before"]["status"]["loadBalancer"]
                        or environment.get("shared_ingress_enabled") is not True
                        or environment["public_host"] != previous["public_host"]
                        or json.loads(config["data"]["profile.json"])["candidate_sha"] != api.candidate):
                    raise OperationError("completed ingress allocation or live configuration differs")
                return service, config

            foundation = api.foundation()
            before = observe()
            addresses = before[0]["status"]["loadBalancer"]["ingress"]
            if len(addresses) != 1 or set(addresses[0]) - {"ip", "ipMode"}:
                raise OperationError("one retained public IPv4 allocation required")
            target = {"installation_id": binding.installation_id, "service_uid": before[0]["metadata"]["uid"],
                      "candidate": api.candidate, "fingerprint_sha256": tls["fingerprint_sha256"],
                      "zone": certificate_config["zone"], "child_domain": binding.child_domain,
                      "management_host": binding.management_host, "address": addresses[0]["ip"]}
            validate_target(target)
            proof = qualify_controller(binding=binding, api=api, deployment_uid=controller["uid"],
                                       image=api.image, tls_receipt=tls)
            pods = api.list_controller_pods(binding.namespace)
            if [pod["metadata"]["uid"] for pod in pods] != proof["pod_uids"]:
                raise OperationError("controller membership changed before legacy proof")
            for pod in pods:
                api.probe_legacy_pod(pod)
            origin = stage["resources"][f"Service:{binding.namespace}:loom-web-origin"]
            if origin["status"] != "created" or not origin["uid"] or not origin["observed"]:
                raise OperationError("recorded legacy origin identity required")
            api.probe_original_backend(origin)
            api.probe_public(tls)
            if (qualify_controller(binding=binding, api=api, deployment_uid=controller["uid"],
                                   image=api.image, tls_receipt=tls) != proof
                    or api.foundation() != foundation
                    or tuple(map(stable_resource, observe())) != tuple(map(stable_resource, before))
                    or private_state._selected(root) != selected):
                raise OperationError("installed ingress changed during DNS qualification")
            return target
    except OperationError:
        raise
    except Exception:
        raise OperationError("installed DNS target unavailable; preserve ingress and certificate state") from None


def publish_ingress_dns(*, api: InstallationAPI, certificate_config: dict[str, Any],
                        state_dir: Path) -> dict[str, Any]:
    """Fixed protected DNS action; credentials stay in the private gateway."""
    from scripts.ops.nebius_dns_challenge import load_token
    from scripts.ops.nebius_dns_publication import (
        GoDaddyPublication,
        publish_dns,
        qualify_authority,
        qualify_public_routes,
        wait_for_addresses,
    )

    try:
        def installed() -> dict[str, Any]:
            return qualify_dns_target(api=api, certificate_config=certificate_config, state_dir=state_dir)

        target = installed()
        credential = Path(certificate_config["credential_file"])
        token = load_token(credential)

        def qualify() -> dict[str, Any]:
            if load_token(credential) != token:
                raise OperationError("DNS credential changed during publication")
            current = installed()
            qualify_authority(current)
            if load_token(credential) != token:
                raise OperationError("DNS credential changed during publication")
            return current

        def wait(current: dict[str, Any]) -> None:
            wait_for_addresses(current)
            qualify_public_routes(current)

        with GoDaddyPublication(target, token) as provider:
            return publish_dns(provider, target=target, state_dir=state_dir / "dns", qualify=qualify, wait=wait)
    except OperationError:
        raise
    except Exception:
        raise OperationError("DNS publication incomplete; preserve private ingress and DNS journals") from None


class LiveIngressAPI(KubectlCutoverAPI):
    def __init__(self, kubeconfig: Path, *, binding: TLSBinding, executable: Path, candidate: str,
                 cluster_id: str, api_server: str, ingress_class: str, image: str):
        super().__init__(kubeconfig, binding=binding, executable=executable, candidate=candidate)
        self.cluster_id, self.api_server = cluster_id, api_server
        self.ingress_class, self.image = ingress_class, image
        self.kubeconfig, self.executable = kubeconfig, executable

    def staging(self, installation: SharedIngressInstallation) -> StageAPI:
        return KubectlStageAPI(self.kubeconfig, binding=self.binding, executable=self.executable, installation=installation)

    def probe_legacy_pod(self, pod: dict[str, Any]) -> None:
        try:
            meta = pod["metadata"]
            if meta["namespace"] != self.binding.namespace or meta.get("labels", {}).get("app") != "loom-shared-ingress":
                raise OperationError("legacy probe Pod outside ingress binding")

            def check_pod() -> None:
                current = self.get_pod(self.binding.namespace, meta["name"])
                observed = (current or {}).get("metadata", {})
                if (observed.get("uid") != meta["uid"] or observed.get("resourceVersion") != meta["resourceVersion"]
                        or observed.get("deletionTimestamp") is not None):
                    raise OperationError("legacy probe Pod identity changed")

            self._forward_legacy("pod/" + meta["name"], 8443, check_pod)
        except OperationError:
            raise
        except Exception:
            raise OperationError("staged ingress legacy HTTPS proof failed") from None

    def probe_original_backend(self, origin: dict[str, Any]) -> None:
        try:
            def check_origin() -> None:
                self.verify_identity(self.binding)
                current = self._get(["get", "service", "loom-web-origin", "-n", self.binding.namespace])
                if (current is None or current["metadata"]["uid"] != origin["uid"]
                        or _origin_snapshot(current) != _origin_snapshot(origin["observed"])):
                    raise OperationError("original legacy origin differs from staged ownership")
            self._forward_legacy("service/loom-web-origin", 443, check_origin)
        except OperationError:
            raise
        except Exception:
            raise OperationError("original legacy backend proof failed") from None

    def _forward_legacy(self, target: str, remote_port: int, check: Callable[[], None]) -> None:
        check()
        config = self.foundation().platform_config
        process = subprocess.Popen(
            [*self.prefix, "port-forward", "--address=127.0.0.1", "-n", self.binding.namespace, target, ":" + str(remote_port)],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env={"PATH": os.defpath, "LANG": "C.UTF-8"},
        )
        try:
            port = _forward_port(process)
            probes.probe_legacy(address="127.0.0.1", port=port, hostname=config["public_host"],
                                environment=config["environment"], candidate=self.candidate)
            check()
            if process.poll() is not None:
                raise OperationError("private legacy forwarder exited during qualification")
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)

    def probe_original_public(self) -> None:
        self._probe_public(None)

    def read(self) -> tuple[dict[str, Any], dict[str, Any]]:
        try:
            self.verify_identity(self.binding)
            service = self._get(["get", "service", "loom-web", "-n", self.binding.namespace])
            config = self._get(["get", "configmap", "loom-platform-config", "-n", self.binding.namespace])
            if service is None or config is None:
                raise OperationError("public routing resources unavailable")
            resource_identity(service, kind="Service", name="loom-web", namespace=self.binding.namespace)
            resource_identity(config, kind="ConfigMap", name="loom-platform-config", namespace=self.binding.namespace)
            if json.loads(config["data"]["profile.json"])["candidate_sha"] != self.candidate:
                raise OperationError("live candidate changed during ingress operation")
            return service, config
        except OperationError:
            raise
        except Exception:
            raise OperationError("live public routing snapshot is unqualified") from None

    def capacity(self) -> dict[str, Any]:
        try:
            self.verify_identity(self.binding)
            rows = []
            # Terminal Pods are already excluded by qualify_capacity. Exclude
            # them at the API too: retained Job history must not consume the
            # bounded response budget needed to observe current workloads.
            for kind, arguments in (("Node", ["get", "nodes"]),
                                    ("Pod", ["get", "pods", "--all-namespaces", "--field-selector", LIVE_POD_FIELD_SELECTOR])):
                # --ignore-not-found suppresses the entire JSON response for
                # an empty filtered collection. Require the actual List so an
                # empty inventory stays distinguishable from a missing read.
                listing = json.loads(self._run([*arguments, "-o", "json"]))
                if (listing is None or listing.get("kind") not in {"List", kind + "List"} or listing.get("apiVersion") != "v1"
                        or listing.get("metadata", {}).get("continue") or not isinstance(listing.get("items"), list)
                        or any(not isinstance(item, dict) or item.get("kind") != kind or item.get("apiVersion") != "v1"
                               for item in listing["items"])):
                    raise OperationError("complete live capacity inventory required")
                rows.append(listing["items"])
            result = qualify_capacity(nodes=rows[0], pods=rows[1])
            self.verify_identity(self.binding)
            return result
        except OperationError:
            raise
        except Exception:
            raise OperationError("live ingress capacity observation unavailable") from None

    def probe_public(self, receipt: dict[str, Any]) -> None:
        self._probe_public(receipt)

    def _probe_public(self, receipt: dict[str, Any] | None) -> None:
        try:
            before = self.read()
            config = self.foundation().platform_config
            service = before[0]
            ports = [p for p in service["spec"]["ports"] if p["port"] == 443 and p["targetPort"] == 8443
                     and p.get("protocol", "TCP") == "TCP"]
            addresses = service["status"]["loadBalancer"]["ingress"]
            if service["spec"]["type"] != "LoadBalancer" or len(ports) != 1 or not addresses:
                raise OperationError("public HTTPS allocation unavailable")
            if receipt is None and service["spec"]["selector"] != {"app": "loom-web"}:
                raise OperationError("original public selector is not restored")
            for endpoint in addresses:
                if receipt is not None:
                    probes.probe_management(address=endpoint["ip"], port=443, hostname=self.binding.management_host,
                                            fingerprint=receipt["fingerprint_sha256"])
                probes.probe_legacy(address=endpoint["ip"], port=443, hostname=config["public_host"],
                                    environment=config["environment"], candidate=self.candidate)
            if tuple(map(stable_resource, self.read())) != tuple(map(stable_resource, before)):
                raise OperationError("public routing identity changed during HTTPS proof")
        except OperationError:
            raise
        except Exception:
            raise OperationError("public ingress HTTPS proof failed") from None

    def foundation(self) -> FoundationBinding:
        """Read and validate current deployment configuration before rendering."""
        try:
            self.verify_identity(self.binding)
            view = json.loads(self._run(["config", "view", "--minify", "-o", "json"]))
            clusters = view["clusters"]
            if (len(clusters) != 1 or clusters[0]["cluster"]["server"] != self.api_server
                    or not clusters[0]["name"].endswith(self.cluster_id.removeprefix("mk8s"))
                    or clusters[0]["cluster"].get("insecure-skip-tls-verify")
                    or not (clusters[0]["cluster"].get("certificate-authority")
                            or clusters[0]["cluster"].get("certificate-authority-data"))):
                raise OperationError("Kubernetes context does not match trusted ingress binding")
            row = self._get(["get", "configmap", "loom-platform-config", "-n", self.binding.namespace])
            if row is None:
                raise OperationError("live platform configuration unavailable")
            resource_identity(row, kind="ConfigMap", name="loom-platform-config", namespace=self.binding.namespace)
            data = row["data"]
            config, profile = json.loads(data["environment.json"]), json.loads(data["profile.json"])
            if (config["namespace"] != self.binding.namespace or config["cluster_id"] != self.cluster_id
                    or config["kubernetes_api_server"] != self.api_server or profile["candidate_sha"] != self.candidate
                    or not re.fullmatch(r"cr\." + re.escape(config["region"]) + r"\.nebius\.cloud/[A-Za-z0-9_-]+/loom-shared-ingress@"
                                        + re.escape(DIGEST), self.image)):
                raise OperationError("live platform differs from installed ingress authority")
            # Validate before normalization so an invalid flag is not hidden.
            foundation = FoundationBinding(
                platform_config_json=json.dumps(config, sort_keys=True), public_dns_zone=self.binding.child_domain,
                ingress_class_name=self.ingress_class, ingress_namespace=self.binding.namespace,
                ingress_controller_label="loom-shared-ingress",
            )
            # The flag controls the existing public Service, not these staged
            # resources. Canonicalize this one operation-owned field so replay
            # after cutover keeps the initial staging render and journal intact.
            foundation = foundation.model_copy(update={
                "platform_config_json": json.dumps({**config, "shared_ingress_enabled": False}, sort_keys=True),
            })
            SharedIngressInstallation(installation_id=UUID(self.binding.installation_id), foundation=foundation,
                                      image=self.image, tls_secret_name="loom-ingress-pending")
            self.verify_identity(self.binding)
            return foundation
        except OperationError:
            raise
        except Exception:
            raise OperationError("trusted live ingress foundation is unavailable") from None


def qualify_capacity(*, nodes: list[dict[str, Any]], pods: list[dict[str, Any]]) -> dict[str, Any]:
    """Prove room for two additional ingress Pods without resizing anything.

    Count all live bound workloads, including terminating/foreign Pods, and all
    pending competitors that could use the same node. Deliberately conservative:
    no capacity from deleting nodes, resize uncertainty or future autoscaling.
    Existing ingress Pods are not subtracted without exact ownership proof.
    """
    from kubernetes import client

    try:
        for rows in (nodes, pods):
            uids = [row["metadata"]["uid"] for row in rows]
            names = [(row["metadata"].get("namespace", ""), row["metadata"]["name"]) for row in rows]
            if len(set(uids)) != len(uids) or len(set(names)) != len(names):
                raise OperationError("duplicate Kubernetes inventory")
            if any(str(UUID(uid)) != uid or UUID(uid).int == 0 for uid in uids):
                raise OperationError("Kubernetes inventory identity unavailable")
        with client.ApiClient() as decoder:
            decoded_nodes = decoder.deserialize(SimpleNamespace(data=json.dumps({
                "apiVersion": "v1", "kind": "NodeList", "items": nodes,
            }).encode()), "V1NodeList").items
        decoded_pods = accounting._decode_pods(SimpleNamespace(data=json.dumps({
            "apiVersion": "v1", "kind": "PodList", "items": pods,
        }).encode())).items
        ingress = SimpleNamespace(spec=SimpleNamespace(template=SimpleNamespace(spec=SimpleNamespace(
            node_selector={"loom.nebius/node-role": "system", "loom.nebius/platform": "integration"},
            tolerations=[{"key": "loom.nebius/platform", "operator": "Equal", "value": "integration", "effect": "NoSchedule"}],
        ))))
        for node in decoded_nodes:
            if (not accounting._node_ready(node) or accounting._node_draining(node)
                    or not accounting._daemonset_matches_node(ingress, node)
                    or any(c.type in {"DiskPressure", "MemoryPressure", "PIDPressure", "NetworkUnavailable"}
                           and c.status != "False" for c in node.status.conditions or [])):
                continue
            if (not re.fullmatch(r"nebius://computeinstance-[a-z0-9]+", node.spec.provider_id or "")
                    or node.metadata.name != node.spec.provider_id.removeprefix("nebius://")):
                raise OperationError("eligible system node provider identity differs")
            allocatable = accounting._required_node_resources(node.status.allocatable, name="system")
            slots = accounting._positive_int(node.status.allocatable.get("pods"), name="system Pod slots")
            requests = []
            for pod in decoded_pods:
                if pod.status.phase in {"Succeeded", "Failed"}:
                    continue
                if pod.spec.node_name:
                    if pod.spec.node_name != node.metadata.name:
                        continue
                elif not accounting._daemonset_matches_node(
                    SimpleNamespace(spec=SimpleNamespace(template=SimpleNamespace(spec=pod.spec))), node,
                ):
                    continue
                requests.append(accounting._pod_request(pod))
            used = accounting._add(*requests)
            if (slots - len(requests) >= 2
                    and allocatable.cpu_millis - used.cpu_millis >= 200
                    and allocatable.memory_mib - used.memory_mib >= 256
                    and allocatable.storage_mib - used.storage_mib >= 128):
                return {"node_uid": node.metadata.uid, "reserved_pods": 2,
                        "cpu_millis": 200, "memory_mib": 256, "storage_mib": 128}
        raise OperationError("eligible system capacity cannot fit the ingress envelope")
    except OperationError:
        raise
    except Exception:
        raise OperationError("full live ingress capacity inventory is unqualified") from None
