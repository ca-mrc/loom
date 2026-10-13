"""Verifier framework (spec §2.4).

Submodules (added incrementally as tasks complete):
- base              : Verifier Protocol + VerifierFactory
- factory           : task-configured CLI/worker verifier construction
- audit             : shared verifier-artifact channel (#865 / #867)
- pytest_verifier   : PytestVerifier (junit XML + json-report)
- script_verifier   : ScriptVerifier
- structured        : StructuredOutputVerifier (JSON Schema)
- llm_judge         : LLMJudgeVerifier (stubbed in v1)
- composite         : CompositeVerifier + Aggregator + AggregatorFn
"""
