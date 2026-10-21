from loom_llm_gateway.execution_attempt_dispatch import admitted_service_execution_models


def test_native_trial_admits_only_its_agent_model() -> None:
    assert admitted_service_execution_models(
        {
            "agent_model": {"provider": "openai", "name": "glm-5.2", "source": "api"},
        }
    ) == (("openai", "glm-5.2"),)


def test_sts_trial_admits_student_and_teacher() -> None:
    assert admitted_service_execution_models(
        {
            "agent_model": {"provider": "openai", "name": "student", "source": "api"},
            "multi_model": {
                "enabled": True,
                "policy": "student_teacher_student",
                "secondary_model": {"provider": "openai", "name": "teacher", "source": "api"},
            },
        }
    ) == (("openai", "student"), ("openai", "teacher"))


def test_enabled_mix_policies_admit_the_teacher_and_a_disabled_plan_does_not() -> None:
    disabled = {
        "agent_model": {"provider": "openai", "name": "student", "source": "api"},
        "multi_model": {
            "enabled": False,
            "policy": "student_teacher_student",
            "secondary_model": {"provider": "openai", "name": "teacher", "source": "api"},
        },
    }
    both = (("openai", "student"), ("openai", "teacher"))
    assert admitted_service_execution_models(disabled) == (("openai", "student"),)
    assert admitted_service_execution_models(None) == ()
    for policy in ("beta_mixture", "student_to_teacher_turns"):
        assert admitted_service_execution_models({
            "agent_model": {"provider": "openai", "name": "student", "source": "api"},
            "multi_model": {
                "enabled": True,
                "policy": policy,
                "secondary_model": {"provider": "openai", "name": "teacher", "source": "api"},
            },
        }) == both
