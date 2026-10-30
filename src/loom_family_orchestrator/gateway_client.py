"""Minimal httpx-backed gateway client for the family-run orchestrator (#672 PR-3).

The orchestrator's ``skill_patcher_llm`` adapter needs a callable
matching the ``_GatewayClient`` protocol in
``loom.family_run.skill_patcher_llm``:

``async def chat_completion(*, model, messages, dialect, max_tokens,
timeout_sec, team_id, trial_id, provider_connection_id) -> dict[str, Any]``.

The adapter runs in the orchestrator process after a real trial completes. It
exchanges a dedicated ``family:evolve`` credential for an ``llm:call`` step
JWT bound to that trial, its persisted batch team, and the selected evolver
provider connection. The ``family_evolver`` dialect keeps adapter spend
separate from agent spend while preserving authoritative attribution.

Kept as a separate module so the orchestrator entrypoint stays a
thin wire-up shell and the client's HTTP shape is unit-testable
without instantiating the full loop.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from math import ceil
from typing import Any

import httpx

from loom.llm_http_client import create_model_http_client


@dataclass
class OrchestratorGatewayClient:
    """Thin OpenAI-compatible ``/v1/chat/completions`` client.

    The orchestrator first exchanges its dedicated worker credential for a
    step JWT at the Control Plane.  The JWT binds the real completed trial,
    represented team, and evolver provider connection before the request is
    sent to the Gateway.
    """

    base_url: str
    control_plane_url: str
    worker_token: str
    timeout_sec: float = 300.0
    _client: httpx.AsyncClient | None = None
    _control_plane_client: httpx.AsyncClient | None = None
    _owned_gateway: bool = field(default=False, init=False)
    _owned_control_plane: bool = field(default=False, init=False)

    def _http(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = create_model_http_client(
                base_url=self.base_url,
                timeout_sec=self.timeout_sec,
            )
            self._owned_gateway = True
        return self._client

    def _control_plane(self) -> httpx.AsyncClient:
        if self._control_plane_client is None:
            # Every model call exchanges a scoped bearer here before dispatch;
            # keep this authority check out of an implicit HTTP pool queue too.
            self._control_plane_client = create_model_http_client(
                base_url=self.control_plane_url,
                timeout_sec=self.timeout_sec,
            )
            self._owned_control_plane = True
        return self._control_plane_client

    async def aclose(self) -> None:
        if self._client is not None and self._owned_gateway:
            await self._client.aclose()
            self._client = None
            self._owned_gateway = False
        if self._control_plane_client is not None and self._owned_control_plane:
            await self._control_plane_client.aclose()
            self._control_plane_client = None
            self._owned_control_plane = False

    async def cancel_trial(self, trial_id: str) -> None:
        """Cancel one family trial through the Control Plane authority."""

        response = await self._control_plane().post(
            f"/trials/{trial_id}/cancel",
            headers={"Authorization": f"Bearer {self.worker_token}"},
        )
        response.raise_for_status()

    async def chat_completion(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        dialect: str,
        max_tokens: int,
        timeout_sec: float,
        team_id: str,
        trial_id: str,
        provider_connection_id: str | None = None,
    ) -> dict[str, Any]:
        """POST /v1/chat/completions with the orchestrator attribution
        block. Raises :class:`httpx.HTTPStatusError` on non-2xx so the
        adapter's failure policy sees a plain exception.

        When ``provider_connection_id`` is set, the request is routed
        via the caller's BYO provider connection (#178). The gateway's
        ``/v1/chat/completions`` route consumes it from
        ``loom.provider_connection_id`` in the body (see
        ``loom_llm_gateway/routes/chat.py``). We ALSO forward it as the
        ``x-loom-provider-connection-id`` header so requests remain
        routable when future gateway paths adopt the facade-style
        header-only auth (#672 blocker #695).
        """
        # Sending provider_connection_id explicitly, including JSON null,
        # tells the Control Plane this is the evolver route rather than the
        # trial's agent route.  The CP validates any configured connection
        # against the represented trial team before minting the JWT.
        ttl_sec = max(60, min(7200, ceil(timeout_sec) + 60))
        token_response = await self._control_plane().post(
            "/admin/step-tokens",
            json={
                "team_id": team_id,
                "trial_id": trial_id,
                "step_id": "family_evolver",
                "ttl_sec": ttl_sec,
                "provider_connection_id": provider_connection_id,
            },
            headers={"Authorization": f"Bearer {self.worker_token}"},
            timeout=timeout_sec,
        )
        token_response.raise_for_status()
        step_token = token_response.json()["token"]
        loom_block: dict[str, Any] = {
            "team_id": team_id,
            "trial_id": trial_id,
            "step_id": "family_evolver",
            "dialect": dialect,
        }
        if provider_connection_id:
            loom_block["provider_connection_id"] = provider_connection_id
        body: dict[str, Any] = {
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "loom": loom_block,
        }
        headers: dict[str, str] = {"content-type": "application/json"}
        headers["Authorization"] = f"Bearer {step_token}"
        if provider_connection_id:
            headers["x-loom-provider-connection-id"] = provider_connection_id
        client = self._http()
        resp = await client.post(
            "/v1/chat/completions",
            json=body,
            headers=headers,
            timeout=timeout_sec,
        )
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]
