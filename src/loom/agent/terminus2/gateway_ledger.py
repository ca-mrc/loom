"""Match Harbor episodes to persisted Loom Gateway llm_calls rows (#744 Gate 2)."""

from __future__ import annotations

from typing import Any, Protocol
from uuid import UUID


class CheckpointBridgeError(Exception):
    """Raised when Harbor checkpoint bridging cannot satisfy join invariants."""


class _CpClient(Protocol):
    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, Any]]: ...


def harbor_metrics_tokens(metrics: dict[str, Any]) -> tuple[int, int]:
    """Normalize Harbor ATIF metrics keys to (input_tokens, output_tokens)."""
    input_tokens = metrics.get("input_tokens")
    if input_tokens is None:
        input_tokens = metrics.get("prompt_tokens")
    output_tokens = metrics.get("output_tokens")
    if output_tokens is None:
        output_tokens = metrics.get("completion_tokens")
    return int(input_tokens or 0), int(output_tokens or 0)


def _row_is_failed_upstream(row: dict[str, Any]) -> bool:
    extras = row.get("provider_extras") or {}
    if not isinstance(extras, dict):
        return False
    return extras.get("_loom_call_status") == "failed"


class GatewayCallLedger:
    """Resolve Harbor steps to authoritative ``llm_calls.id`` gateway rows."""

    def __init__(self, *, trial_id: UUID, step_id: str) -> None:
        self._trial_id = trial_id
        self._step_id = step_id
        self._rows: list[dict[str, Any]] | None = None
        self._consumed_ids: set[str] = set()

    async def refresh(self, cp_client: _CpClient) -> None:
        self._rows = await cp_client.get_trial_llm_calls(self._trial_id)

    def _available(self) -> list[dict[str, Any]]:
        if self._rows is None:
            raise CheckpointBridgeError(
                "gateway ledger not refreshed before bridging Harbor step",
            )
        return [
            row
            for row in self._rows
            if str(row.get("id")) not in self._consumed_ids
            and str(row.get("step_id") or "") == self._step_id
            and not _row_is_failed_upstream(row)
        ]

    def resolve_for_metrics(
        self,
        metrics: dict[str, Any],
        *,
        episode: int | None = None,
        client_call_id: str | None = None,
    ) -> dict[str, Any]:
        """Return the matching llm_calls row or raise fail-closed."""
        available = self._available()
        if client_call_id:
            hits = [
                row for row in available
                if str(row.get("client_call_id") or "") == str(client_call_id)
            ]
            if len(hits) == 1:
                self._consumed_ids.add(str(hits[0]["id"]))
                return hits[0]
            raise CheckpointBridgeError(
                f"no llm_calls row matches client_call_id={client_call_id}",
            )

        correlated = [
            row for row in available
            if str(row.get("correlation_status") or "") == "correlated"
        ]
        if episode is not None and correlated:
            by_episode = [
                row for row in correlated
                if int(row.get("episode") or -1) == int(episode)
            ]
            if len(by_episode) > 1:
                # Harbor's native summarization makes several LLM calls in
                # one episode. Its main turn must not bind to the first
                # summarization call merely because it has a lower ordinal.
                input_tokens, output_tokens = harbor_metrics_tokens(metrics)
                by_episode = [
                    row for row in by_episode
                    if int(row.get("input_tokens") or 0) == input_tokens
                    and int(row.get("output_tokens") or 0) == output_tokens
                ]
                if not by_episode:
                    raise CheckpointBridgeError(
                        "no correlated llm_calls row matches Harbor episode metrics "
                        f"(step_id={self._step_id}, episode={episode}, "
                        f"input_tokens={input_tokens}, output_tokens={output_tokens})",
                    )
                if len(by_episode) > 1:
                    raise CheckpointBridgeError(
                        "ambiguous correlated llm_calls rows match Harbor episode metrics "
                        f"(step_id={self._step_id}, episode={episode}, "
                        f"input_tokens={input_tokens}, output_tokens={output_tokens}, "
                        f"matches={len(by_episode)})",
                    )
            if by_episode:
                row = by_episode[0]
                self._consumed_ids.add(str(row["id"]))
                return row
            raise CheckpointBridgeError(
                "no correlated llm_calls row for Harbor episode "
                f"(step_id={self._step_id}, episode={episode})",
            )

        input_tokens, output_tokens = harbor_metrics_tokens(metrics)
        candidates = [
            row
            for row in available
            if str(row.get("correlation_status") or "legacy_uncorrelated")
            != "correlated"
            and int(row.get("input_tokens") or 0) == input_tokens
            and int(row.get("output_tokens") or 0) == output_tokens
        ]
        if not candidates:
            raise CheckpointBridgeError(
                "no llm_calls row matches Harbor episode metrics "
                f"(step_id={self._step_id}, input_tokens={input_tokens}, "
                f"output_tokens={output_tokens})",
            )
        if len(candidates) > 1:
            raise CheckpointBridgeError(
                "ambiguous llm_calls rows match Harbor episode metrics "
                f"(step_id={self._step_id}, input_tokens={input_tokens}, "
                f"output_tokens={output_tokens}, matches={len(candidates)})",
            )
        row = candidates[0]
        self._consumed_ids.add(str(row["id"]))
        return row
