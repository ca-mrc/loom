"""Student/teacher Terminus2 LLM router (#1380).

Harbor ``run()`` talks to ``agent._llm`` (a ``BaseLLM``). Loom installs two
fully constructed Harbor LiteLLM delegates and a router that implements the
same interface — no ``_model_name`` mutation, no Harbor fork.

Mix policies:

- ``student_teacher_student``: episodes ``1 .. K1-1`` student, ``K1 .. K2-1``
  teacher, ``>= K2`` student.
- ``beta_mixture``: each Harbor episode draws ``hash(seed, trial_id, episode)``
  in ``[0, 1)``; teacher if ``draw < beta``, else student. Parse retries stay
  on the same episode (and therefore the same actor). This is who *drives*
  the session, not DAgger teacher labels.
- ``student_to_teacher_turns``: each ``LoomRoleRouter.call()`` (``call_ordinal``)
  is a schedule turn. Student before ``step_start``, rising β until
  ``step_end``, then force teacher with one-way latch. Not DAgger labels.
"""

from __future__ import annotations

import hashlib
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4

from loom.errors import AgentError

Role = Literal["student", "teacher"]
MixMode = Literal[
    "student_teacher_student",
    "beta_mixture",
    "student_to_teacher_turns",
]

# Matches dagger-tb TerminalWorld segmented latch default.
HAZARD_POWER = 1.65


class RoleRouterEventSink(Protocol):
    async def on_switch(
        self, *, switch_episode: int, from_role: Role, to_role: Role,
    ) -> None: ...

    async def on_llm_started(
        self,
        *,
        client_call_id: str,
        episode: int,
        call_ordinal: int,
        role: Role,
        requested_model: str,
        first_of_role: bool,
    ) -> None: ...

    async def on_llm_completed(
        self,
        *,
        client_call_id: str,
        episode: int,
        call_ordinal: int,
        role: Role,
        requested_model: str,
        response_model: str | None,
    ) -> None: ...

    async def on_llm_failed(
        self,
        *,
        client_call_id: str,
        episode: int,
        call_ordinal: int,
        role: Role,
        requested_model: str,
        error: str,
    ) -> None: ...

try:
    from harbor.llms.base import BaseLLM as _HarborBaseLLM
except ImportError:  # unit tests / hosts without the worker Harbor pin
    class _HarborBaseLLM:  # type: ignore[no-redef]
        pass


def assert_terminus2_switch_contract(agent: Any) -> None:
    """Fail closed if the pinned Harbor Terminus2 shape is missing."""
    missing = [
        name
        for name in ("_llm", "_n_episodes", "_init_llm")
        if not hasattr(agent, name)
    ]
    if missing:
        raise AgentError(
            "terminus-2 multi_model switch requires Harbor Terminus2 "
            f"attributes {missing}; worker Harbor pin may have changed",
        )
    if getattr(agent, "_llm", None) is None:
        raise AgentError(
            "terminus-2 multi_model switch requires agent._llm to be set",
        )


def role_for_episode(
    episode: int,
    *,
    first_switch_episode: int,
    return_switch_episode: int,
) -> Role:
    if episode < 1:
        return "student"
    if first_switch_episode <= episode < return_switch_episode:
        return "teacher"
    return "student"


def seed_fingerprint(seed: str) -> str:
    """Non-secret handle for trajectory events (do not log the raw seed)."""
    return hashlib.sha256(seed.encode()).hexdigest()[:16]


def deterministic_episode_draw(seed: str, trial_id: str, episode: int) -> float:
    """Replay-safe number in [0, 1). Same as dagger-tb, Loom identities."""
    digest = hashlib.sha256(f"{seed}:{trial_id}:{episode}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def deterministic_turn_draw(seed: str, trial_id: str, turn: int) -> float:
    """Replay-safe number in [0, 1) for turn-grain schedules."""
    digest = hashlib.sha256(f"{seed}:{trial_id}:turn:{turn}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


def role_for_beta_episode(
    episode: int,
    *,
    beta: float,
    seed: str,
    trial_id: str,
) -> Role:
    draw = deterministic_episode_draw(seed, str(trial_id), episode)
    return "teacher" if draw < beta else "student"


def turn_beta(
    turn: int,
    *,
    step_start: int,
    step_end: int,
    power: float = HAZARD_POWER,
) -> float:
    """Rising teacher probability inside ``[step_start, step_end)``."""
    if turn < step_start:
        return 0.0
    if turn >= step_end:
        return 1.0
    denom = step_end - step_start
    if denom <= 0:
        return 1.0
    progress = float(turn - step_start) / float(denom)
    beta = progress ** float(power)
    return float(beta)


def role_at_turn(
    turn: int,
    *,
    step_start: int,
    step_end: int,
    seed: str,
    trial_id: str,
    latched: bool,
    power: float = HAZARD_POWER,
) -> Role:
    """Single-turn role given current latch (does not advance latch)."""
    if latched or turn >= step_end:
        return "teacher"
    if turn < step_start:
        return "student"
    beta = turn_beta(turn, step_start=step_start, step_end=step_end, power=power)
    draw = deterministic_turn_draw(seed, str(trial_id), turn)
    return "teacher" if draw < beta else "student"


def role_for_turn_schedule(
    turn: int,
    *,
    step_start: int,
    step_end: int,
    seed: str,
    trial_id: str,
    power: float = HAZARD_POWER,
) -> Role:
    """Replay turns ``1..turn`` with one-way latch (gateway correlation)."""
    if turn < 1:
        return "student"
    latched = False
    role: Role = "student"
    for t in range(1, turn + 1):
        role = role_at_turn(
            t,
            step_start=step_start,
            step_end=step_end,
            seed=seed,
            trial_id=str(trial_id),
            latched=latched,
            power=power,
        )
        if role == "teacher":
            latched = True
    return role


class LoomRoleRouter(_HarborBaseLLM):  # type: ignore[misc]
    """Instance-local ``BaseLLM`` that forwards to student or teacher LiteLLM."""

    def __init__(
        self,
        *,
        agent: Any,
        student: Any,
        teacher: Any,
        first_switch_episode: int | None = None,
        return_switch_episode: int | None = None,
        mix_mode: MixMode = "student_teacher_student",
        beta: float | None = None,
        seed: str | None = None,
        trial_id: str | None = None,
        step_start: int | None = None,
        step_end: int | None = None,
        student_model_name: str | None = None,
        teacher_model_name: str | None = None,
        agent_execution_id: str | None = None,
        agent_run_attempt_id: str | None = None,
        event_sink: RoleRouterEventSink | None = None,
        starting_call_ordinal: int = 0,
    ) -> None:
        if mix_mode == "beta_mixture":
            if beta is None or seed is None or not trial_id:
                raise AgentError(
                    "beta_mixture requires beta, seed, and trial_id",
                )
            if not 0.0 <= float(beta) <= 1.0:
                raise AgentError(f"multi_model.beta must be in [0, 1], got {beta}")
        elif mix_mode == "student_to_teacher_turns":
            if (
                step_start is None
                or step_end is None
                or seed is None
                or not trial_id
            ):
                raise AgentError(
                    "student_to_teacher_turns requires step_start, step_end, "
                    "seed, and trial_id",
                )
            if step_start < 2:
                raise AgentError(
                    "multi_model.step_start must be >= 2, "
                    f"got {step_start}",
                )
            if step_end <= step_start:
                raise AgentError(
                    "multi_model.step_end must be > step_start "
                    f"(start={step_start}, end={step_end})",
                )
        else:
            if first_switch_episode is None or return_switch_episode is None:
                raise AgentError(
                    "student_teacher_student requires K1 and K2",
                )
            if first_switch_episode < 2:
                raise AgentError(
                    "multi_model.switch_episode (K1) must be >= 2, "
                    f"got {first_switch_episode}",
                )
            if return_switch_episode <= first_switch_episode:
                raise AgentError(
                    "multi_model.return_switch_episode (K2) must be > K1 "
                    f"(K1={first_switch_episode}, K2={return_switch_episode})",
                )
        self.agent = agent
        self.student = student
        self.teacher = teacher
        self.mix_mode: MixMode = mix_mode
        self.first_switch_episode = first_switch_episode
        self.return_switch_episode = return_switch_episode
        self.beta = beta
        self.seed = seed
        self.trial_id = trial_id
        self.step_start = step_start
        self.step_end = step_end
        self.student_model_name = student_model_name or ""
        self.teacher_model_name = teacher_model_name or ""
        self.agent_execution_id = agent_execution_id
        self.agent_run_attempt_id = agent_run_attempt_id
        self.event_sink = event_sink
        self._call_ordinal = starting_call_ordinal
        self._last_role: Role = "student"
        self._latched_teacher = False
        self.applied_switches: list[dict[str, Any]] = []
        self._emitted_switch_episodes: set[int] = set()

    def role(self) -> Role:
        episode = int(getattr(self.agent, "_n_episodes", 0) or 0)
        if self.mix_mode == "beta_mixture":
            assert self.beta is not None and self.seed is not None and self.trial_id
            return role_for_beta_episode(
                episode,
                beta=self.beta,
                seed=self.seed,
                trial_id=self.trial_id,
            )
        if self.mix_mode == "student_to_teacher_turns":
            assert (
                self.step_start is not None
                and self.step_end is not None
                and self.seed is not None
                and self.trial_id
            )
            if self._latched_teacher:
                return "teacher"
            if self._call_ordinal < 1:
                return "student"
            return role_for_turn_schedule(
                self._call_ordinal,
                step_start=self.step_start,
                step_end=self.step_end,
                seed=self.seed,
                trial_id=self.trial_id,
            )
        assert self.first_switch_episode is not None
        assert self.return_switch_episode is not None
        return role_for_episode(
            episode,
            first_switch_episode=self.first_switch_episode,
            return_switch_episode=self.return_switch_episode,
        )

    def _role_for_next_call(self) -> Role:
        if self.mix_mode != "student_to_teacher_turns":
            return self.role()
        assert (
            self.step_start is not None
            and self.step_end is not None
            and self.seed is not None
            and self.trial_id
        )
        turn = self._call_ordinal + 1
        return role_at_turn(
            turn,
            step_start=self.step_start,
            step_end=self.step_end,
            seed=self.seed,
            trial_id=self.trial_id,
            latched=self._latched_teacher,
        )

    def _active(self, role: Role | None = None) -> Any:
        active = role if role is not None else self.role()
        return self.teacher if active == "teacher" else self.student

    def _note_role_change(self, new_role: Role) -> bool:
        """Record a switch; return True if this call is the first in the new role."""
        if new_role == self._last_role:
            return False
        episode = int(getattr(self.agent, "_n_episodes", 0) or 0)
        if episode not in self._emitted_switch_episodes:
            self.applied_switches.append(
                {
                    "switch_episode": episode,
                    "from_role": self._last_role,
                    "to_role": new_role,
                },
            )
            self._emitted_switch_episodes.add(episode)
        self._last_role = new_role
        return True

    def get_model_context_limit(self) -> int:
        return int(self._active().get_model_context_limit())

    def get_model_output_limit(self) -> int | None:
        limit = self._active().get_model_output_limit()
        if limit is None:
            return None
        return int(limit)

    def requested_model_name(self, role: Role | None = None) -> str:
        active = role if role is not None else self.role()
        if active == "teacher":
            return self.teacher_model_name
        return self.student_model_name

    async def call(self, prompt: str, **kwargs: Any) -> Any:
        new_role = self._role_for_next_call()
        if self.mix_mode == "student_to_teacher_turns" and new_role == "teacher":
            self._latched_teacher = True
        switched = self._note_role_change(new_role)
        if switched:
            kwargs.pop("previous_response_id", None)
            if self.event_sink is not None:
                await self.event_sink.on_switch(
                    switch_episode=int(getattr(self.agent, "_n_episodes", 0) or 0),
                    from_role=self.applied_switches[-1]["from_role"],
                    to_role=new_role,
                )
        episode = int(getattr(self.agent, "_n_episodes", 0) or 0)
        self._call_ordinal += 1
        client_call_id = str(uuid4())
        requested_model = self.requested_model_name(new_role)
        if self.agent_execution_id and self.agent_run_attempt_id:
            extra = dict(kwargs.get("extra_body") or {})
            extra.update(
                {
                    "loom_client_call_id": client_call_id,
                    "loom_agent_execution_id": self.agent_execution_id,
                    "loom_agent_run_attempt_id": self.agent_run_attempt_id,
                    "loom_episode": episode,
                    "loom_call_ordinal": self._call_ordinal,
                    "loom_requested_model": requested_model,
                    "loom_role": new_role,
                },
            )
            kwargs["extra_body"] = extra
        if self.event_sink is not None:
            await self.event_sink.on_llm_started(
                client_call_id=client_call_id,
                episode=episode,
                call_ordinal=self._call_ordinal,
                role=new_role,
                requested_model=requested_model,
                first_of_role=switched,
            )
        try:
            result = await self._active(new_role).call(prompt, **kwargs)
        except Exception as exc:
            if self.event_sink is not None:
                await self.event_sink.on_llm_failed(
                    client_call_id=client_call_id,
                    episode=episode,
                    call_ordinal=self._call_ordinal,
                    role=new_role,
                    requested_model=requested_model,
                    error=str(exc),
                )
            raise
        response_model = None
        if isinstance(result, dict):
            response_model = result.get("model")
        elif hasattr(result, "model"):
            response_model = getattr(result, "model", None)
        if self.event_sink is not None:
            await self.event_sink.on_llm_completed(
                client_call_id=client_call_id,
                episode=episode,
                call_ordinal=self._call_ordinal,
                role=new_role,
                requested_model=requested_model,
                response_model=str(response_model) if response_model else None,
            )
        return result


def _student_llm_for_auth(agent: Any) -> Any:
    """Return the student Harbor LiteLLM (unwrap router if already installed)."""
    llm = getattr(agent, "_llm", None)
    if llm is None:
        return None
    if isinstance(llm, LoomRoleRouter):
        return llm.student
    return llm


def _agent_option(agent: Any, name: str, default: Any = None) -> Any:
    """Read Harbor 0.24 options; retain 0.18 support for existing adapters."""
    options = getattr(agent, "options", None)
    if options is not None:
        return getattr(options, name)
    return getattr(agent, f"_{name}", default)


def gateway_llm_kwargs_for_teacher(agent: Any) -> dict[str, Any]:
    """Resolve LiteLLM kwargs for the teacher, including the step JWT.

    Harbor dumps ``agent.options.llm_kwargs`` (``agent._llm_kwargs`` in 0.18)
    into trajectory.json, so Loom redacts ``api_key`` there. The live student
    LiteLLM keeps the same JWT in
    ``student._llm_kwargs`` (constructor kwargs). Teacher construction must
    reuse that credential even if the dump field was already scrubbed.

    Raises:
        AgentError: if no step JWT / api_key can be recovered. Fail closed so
        multi-model never boots a teacher that will 401 at first call.
    """
    llm_kwargs = dict(_agent_option(agent, "llm_kwargs") or {})
    api_key = llm_kwargs.get("api_key")
    if isinstance(api_key, str) and api_key:
        return llm_kwargs

    student = _student_llm_for_auth(agent)
    student_kwargs = getattr(student, "_llm_kwargs", None) if student is not None else None
    if isinstance(student_kwargs, dict):
        recovered = student_kwargs.get("api_key")
        if isinstance(recovered, str) and recovered:
            llm_kwargs = {**llm_kwargs, "api_key": recovered}
            return llm_kwargs

    raise AgentError(
        "multi-model teacher construction missing gateway api_key / step JWT; "
        "refusing to install a teacher LiteLLM without credentials "
        "(do not redact agent LLM options before construct_teacher_llm; "
        "student LiteLLM also lacked a recoverable api_key)",
    )


def construct_teacher_llm(agent: Any, *, teacher_model_name: str) -> Any:
    """Build a second Harbor LiteLLM with the same gateway/JWT as the student."""
    student = _student_llm_for_auth(agent)
    llm_kwargs = gateway_llm_kwargs_for_teacher(agent)
    from harbor.llms.base import LLMBackend

    resolved = agent._resolve_model_info(teacher_model_name, None)
    return agent._init_llm(
        llm_backend=LLMBackend.LITELLM,
        model_name=teacher_model_name,
        temperature=_agent_option(agent, "temperature"),
        collect_rollout_details=bool(
            _agent_option(agent, "collect_rollout_details", False),
        ),
        llm_kwargs=llm_kwargs,
        api_base=_agent_option(agent, "api_base", getattr(student, "_api_base", None)),
        session_id=getattr(agent, "_session_id", None)
        or getattr(student, "_session_id", None),
        max_thinking_tokens=_agent_option(
            agent, "max_thinking_tokens", getattr(student, "_max_thinking_tokens", None),
        ),
        reasoning_effort=_agent_option(agent, "reasoning_effort"),
        model_info=resolved,
        use_responses_api=bool(_agent_option(
            agent, "use_responses_api", getattr(student, "_use_responses_api", False),
        )),
    )


def redact_agent_llm_kwargs(agent: Any) -> None:
    """Strip credentials from Harbor's trajectory dump field, not from LiteLLM.

    Must run only *after* teacher construction (see ``install_role_router``).
    Scrubbing the agent options does not remove the JWT from the live
    student LiteLLM instance; ``gateway_llm_kwargs_for_teacher`` can still
    recover it as defense in depth.
    """
    raw = _agent_option(agent, "llm_kwargs")
    if not isinstance(raw, dict):
        return
    redacted = {
        key: value
        for key, value in raw.items()
        if key != "api_key" and "loom_step_" not in str(value)
    }
    options = getattr(agent, "options", None)
    if options is not None:
        options.llm_kwargs = redacted
    else:
        agent._llm_kwargs = redacted


def install_role_router(
    agent: Any,
    *,
    teacher_model_name: str,
    first_switch_episode: int | None = None,
    return_switch_episode: int | None = None,
    mix_mode: MixMode = "student_teacher_student",
    beta: float | None = None,
    seed: str | None = None,
    trial_id: str | UUID | None = None,
    step_start: int | None = None,
    step_end: int | None = None,
    teacher_llm: Any | None = None,
    student_model_name: str | None = None,
    agent_execution_id: str | None = None,
    agent_run_attempt_id: str | None = None,
    event_sink: RoleRouterEventSink | None = None,
    starting_call_ordinal: int = 0,
) -> LoomRoleRouter:
    """Replace ``agent._llm`` with a two-delegate router; do not rename models.

    Order is load-bearing for every mix policy (STS / beta / turn schedule):

    1. Construct (or accept) the teacher LiteLLM while the step JWT is still
       available via agent LLM options and/or ``student._llm_kwargs``.
    2. Install the router as ``agent._llm``.
    3. Redact agent LLM options so Harbor trajectory dumps omit the JWT.

    Never redact before step 1.
    """
    assert_terminus2_switch_contract(agent)
    student = agent._llm
    # Construct before redact — teacher copies gateway auth from kwargs /
    # student LiteLLM. Applies to all multi-model policies.
    teacher = teacher_llm if teacher_llm is not None else construct_teacher_llm(
        agent,
        teacher_model_name=teacher_model_name,
    )
    router = LoomRoleRouter(
        agent=agent,
        student=student,
        teacher=teacher,
        first_switch_episode=first_switch_episode,
        return_switch_episode=return_switch_episode,
        mix_mode=mix_mode,
        beta=beta,
        seed=seed,
        trial_id=None if trial_id is None else str(trial_id),
        step_start=step_start,
        step_end=step_end,
        student_model_name=student_model_name,
        teacher_model_name=teacher_model_name,
        agent_execution_id=agent_execution_id,
        agent_run_attempt_id=agent_run_attempt_id,
        event_sink=event_sink,
        starting_call_ordinal=starting_call_ordinal,
    )
    agent._llm = router
    redact_agent_llm_kwargs(agent)
    return router
