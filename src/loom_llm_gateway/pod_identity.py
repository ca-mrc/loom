"""Verify a rotating Pod-bound token against the lease's explicit native cluster."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
from pydantic import TypeAdapter

from loom.llm_http_client import create_model_http_client
from loom.nebius_kubernetes import NebiusKubernetesConnection, NebiusKubernetesCredentials
from loom_control_plane.service_execution_output import (
    ServiceExecutionBrokerError,
    VerifiedExecutionPod,
)


class ExecutionPodReviewer:
    def __init__(self, connections: dict[str, NebiusKubernetesConnection]) -> None:
        self._connections = connections
        self._credentials: dict[str, NebiusKubernetesCredentials] = {}
        self._clients: dict[str, httpx.AsyncClient] = {}

    @classmethod
    def from_file(cls, path: Path | None) -> ExecutionPodReviewer:
        connections = (
            {}
            if path is None
            else TypeAdapter(dict[str, NebiusKubernetesConnection]).validate_python(
                json.loads(path.read_text())
            )
        )
        return cls(connections)

    async def close(self) -> None:
        for client in self._clients.values():
            await client.aclose()
        for credentials in self._credentials.values():
            await credentials.close()

    async def review(
        self,
        *,
        cluster_scope_id: str,
        namespace: str,
        service_account: str,
        audience: str,
        token: str,
    ) -> VerifiedExecutionPod:
        connection = self._connections.get(cluster_scope_id)
        if connection is None:
            raise ServiceExecutionBrokerError("execution_pod_review_unavailable")
        if not token or len(token) > 16384 or any(character.isspace() for character in token):
            raise ServiceExecutionBrokerError("execution_pod_identity_invalid")
        try:
            if cluster_scope_id not in self._credentials:
                credentials = NebiusKubernetesCredentials(connection)
                self._credentials[cluster_scope_id] = credentials
                self._clients[cluster_scope_id] = create_model_http_client(
                    verify=credentials.ssl_context,
                    timeout_sec=10.0,
                    follow_redirects=False,
                    trust_env=False,
                )
            credential = await self._credentials[cluster_scope_id].get_token()
            response = await self._clients[cluster_scope_id].post(
                connection.endpoint + "/apis/authentication.k8s.io/v1/tokenreviews",
                headers={"Authorization": "Bearer " + credential},
                json={
                    "apiVersion": "authentication.k8s.io/v1",
                    "kind": "TokenReview",
                    "spec": {"token": token, "audiences": [audience]},
                },
            )
            response.raise_for_status()
            payload: Any = response.json()
        except Exception:
            # Native errors may contain headers or endpoint details. Expose only
            # this bounded reason; never stringify the provider/HTTP exception.
            raise ServiceExecutionBrokerError("execution_pod_review_unavailable") from None
        status = payload.get("status", {}) if isinstance(payload, dict) else {}
        user = status.get("user", {}) if isinstance(status, dict) else {}
        extra = user.get("extra", {}) if isinstance(user, dict) else {}
        pod_uids = (
            extra.get("authentication.kubernetes.io/pod-uid", []) if isinstance(extra, dict) else []
        )
        if (
            not isinstance(status, dict)
            or status.get("authenticated") is not True
            or not isinstance(status.get("audiences"), list)
            or audience not in status["audiences"]
            or not isinstance(user, dict)
            or user.get("username") != f"system:serviceaccount:{namespace}:{service_account}"
            or not isinstance(pod_uids, list)
            or len(pod_uids) != 1
            or not isinstance(pod_uids[0], str)
            or not pod_uids[0]
        ):
            raise ServiceExecutionBrokerError("execution_pod_identity_invalid")
        return VerifiedExecutionPod(
            cluster_scope_id=cluster_scope_id,
            namespace=namespace,
            service_account=service_account,
            pod_uid=pod_uids[0],
            audience=audience,
        )
