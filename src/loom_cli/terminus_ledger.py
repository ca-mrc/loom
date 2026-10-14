"""Capture local Harbor responses before its reduced UsageInfo loses billing facts.

The ledger is per trial and contains observed local calls, never invented CP
rows. Its shape lets the existing episode join retain its fail-closed rules.
Harbor's own retries remain owned by Harbor; this records responses observed at
its usage boundary and terminal call failures, not invisible HTTP attempts.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from loom.agent.direct_provider import DirectProviderConnection, DirectProviderLogger
from loom.errors import AgentError
from loom.models.types import ModelSpec
from loom.request_params import normalize_request_params
from loom.trajectory.llm_call_events import llm_call_row_to_event
from loom.trajectory.writer import TrajectoryWriter
from loom_cli.rate_cards import RateCardTable, compute_cost_usd, load_rate_cards, lookup_entry


class LocalHarborLedger:
    def __init__(self, *, trial_id: UUID, model: ModelSpec,
                 connection: DirectProviderConnection, rate_cards: RateCardTable | None = None) -> None:
        self.trial_id = trial_id
        self.model = model
        self.connection = connection
        self._table = rate_cards
        self._rows: list[dict[str, Any]] = []
        self._ordinal = 0

    def _price(self, usage: dict[str, Any]) -> tuple[float, str]:
        if self._table is None:
            self._table = load_rate_cards()
        provider, name = self.model.provider, self.model.name
        if provider == "local":
            server, _, name = name.partition("/")
            provider = f"local:{server}"
        entry = lookup_entry(self._table, provider=provider, model=name, default_zero=self.model.provider == "local")
        # LiteLLM prompt totals include cached tokens; Anthropic includes cache
        # writes too. Rate-card buckets are disjoint for every provider.
        input_tokens = int(usage.get("prompt_tokens") or 0)
        details = usage.get("prompt_tokens_details") or {}
        cached = int(details.get("cached_tokens") or 0)
        written = int(details.get("cache_creation_tokens") or 0)
        input_tokens = max(0, input_tokens - cached - written)
        cost = compute_cost_usd(entry, input_tokens=input_tokens,
                                output_tokens=int(usage.get("completion_tokens") or 0),
                                cached_input_tokens=cached, cache_write_tokens=written)
        snapshot = tuple((e.provider, e.model, e.input_per_mtok, e.output_per_mtok,
                          e.cache_read_per_mtok, e.cache_write_per_mtok) for e in self._table.entries)
        fingerprint = hashlib.sha256(repr(snapshot).encode()).hexdigest()
        return cost, fingerprint

    def install(self, agent: Any, *, step_id: str, trajectory: TrajectoryWriter) -> None:
        # A retry creates a fresh Harbor trajectory whose episodes start at one.
        # Prior call events are already persisted; only this invocation's rows
        # may satisfy its strict episode joins.
        self._rows.clear()
        llm = agent._llm
        agent.logger = DirectProviderLogger(agent.logger, self.connection)
        llm._logger = DirectProviderLogger(llm._logger, self.connection)
        original_call = llm.call
        original_usage = llm._extract_usage_info
        original_base_kwargs = llm._build_base_kwargs
        pending: list[dict[str, Any]] = []
        wire_usage_status: str | None = None
        request_params = normalize_request_params(None)

        def build_base_kwargs(*args: Any, **kwargs: Any) -> dict[str, Any]:
            nonlocal wire_usage_status, request_params
            wire_usage_status = None
            request_params = normalize_request_params(None)
            result: dict[str, Any] = original_base_kwargs(*args, **kwargs)
            original_logger = result.get("logger_fn")

            def observe_wire_response(details: dict[str, Any]) -> None:
                nonlocal wire_usage_status, request_params
                request_params = normalize_request_params(details)
                if details.get("log_event_type") == "post_api_call":
                    raw = details.get("original_response")
                    if isinstance(raw, str):
                        try:
                            raw = json.loads(raw)
                        except (ValueError, TypeError):
                            raw = None
                    if isinstance(raw, dict):
                        provider = self.connection.model.provider
                        usage_key = "usageMetadata" if provider == "google" else "usage"
                        usage = raw.get(usage_key)
                        fields = {
                            "anthropic": ("input_tokens", "output_tokens"),
                            "google": ("promptTokenCount", "candidatesTokenCount"),
                        }.get(provider, ("prompt_tokens", "completion_tokens"))
                        if not isinstance(usage, dict) or not usage:
                            wire_usage_status = "missing"
                        elif any(usage.get(field) is None for field in fields):
                            wire_usage_status = "partial"
                        else:
                            wire_usage_status = None
                if callable(original_logger):
                    original_logger(self.connection.redact(details))

            # LiteLLM's per-request logger observes the original response before
            # it synthesizes zero-valued Usage fields. No global SDK hooks.
            result["logger_fn"] = observe_wire_response
            return result

        def extract_usage(response: Any) -> Any:
            raw = getattr(response, "usage", None)
            usage = raw.model_dump() if raw is not None and hasattr(raw, "model_dump") else raw
            usage = usage if isinstance(usage, dict) else {}
            normalized = original_usage(response)
            cost, rate_hash = self._price(usage)
            if normalized is not None:
                normalized.cost_usd = cost
            details = usage.get("prompt_tokens_details") or {}
            output_details = usage.get("completion_tokens_details") or {}
            extras: dict[str, Any] = {
                "cache_read_input_tokens": int(details.get("cached_tokens") or 0),
                "cache_creation_input_tokens": int(details.get("cache_creation_tokens") or 0),
                "reasoning_tokens": int(output_details.get("reasoning_tokens") or 0),
            }
            if wire_usage_status is not None:
                extras["_loom_usage_status"] = wire_usage_status
            elif not usage:
                extras["_loom_usage_status"] = "missing"
            elif any(usage.get(key) is None for key in ("prompt_tokens", "completion_tokens")):
                extras["_loom_usage_status"] = "partial"
            pending.append({
                "id": str(uuid4()), "trial_id": str(self.trial_id), "step_id": step_id,
                "model": self.model.name, "requested_model": self.model.name,
                "response_model": getattr(response, "model", None),
                "captured_at": datetime.now(UTC).isoformat(),
                "input_tokens": int(usage.get("prompt_tokens") or 0),
                "output_tokens": int(usage.get("completion_tokens") or 0),
                "provider_extras": extras, "cost_usd": cost, "rate_card_hash": rate_hash,
                "correlation_status": "correlated", "episode": int(agent._n_episodes),
                "call_ordinal": self._ordinal, "request_params": request_params,
            })
            return normalized

        async def call(prompt: str, **kwargs: Any) -> Any:
            self._ordinal += 1
            pending.clear()
            started = time.monotonic()
            succeeded = False
            try:
                result = await original_call(prompt, **kwargs)
                succeeded = True
                return result
            finally:
                # Cancellation propagates unchanged. The attempt-owned writer
                # fences late appends; no background callback survives the call.
                if not pending and not succeeded:
                    pending.append({
                        "id": str(uuid4()), "trial_id": str(self.trial_id), "step_id": step_id,
                        "model": self.model.name, "call_status": "failed", "cost_usd": 0,
                        "provider_extras": {"_loom_call_status": "failed", "_loom_usage_status": "missing"},
                        "captured_at": datetime.now(UTC).isoformat(),
                    })
                for row in pending:
                    row = self.connection.redact(row)
                    event = llm_call_row_to_event(row, trial_id=self.trial_id, seq=0).model_copy(update={
                        "model": self.model, "duration_sec": time.monotonic() - started,
                    })
                    try:
                        await trajectory.append(event)
                    except Exception:
                        task = asyncio.current_task()
                        if task is None or not task.cancelling():
                            raise
                if succeeded and pending:
                    # Only the response returned to Harbor can join its step.
                    # Prior observed responses remain separate usage events.
                    self._rows.append(dict(self.connection.redact(pending[-1])))

        llm._extract_usage_info = extract_usage
        llm._build_base_kwargs = build_base_kwargs
        llm.call = call

    async def get_trial_llm_calls(self, trial_id: UUID) -> list[dict[str, Any]]:
        if trial_id != self.trial_id:
            raise AgentError("local usage ledger belongs to a different trial")
        return [dict(row) for row in self._rows]
