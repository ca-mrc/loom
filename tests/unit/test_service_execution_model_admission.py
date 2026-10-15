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


def test_deferred_or_disabled_multi_model_does_not_open_teacher() -> None:
    disabled = {
        "agent_model": {"provider": "openai", "name": "student", "source": "api"},
        "multi_model": {
            "enabled": False,
            "policy": "student_teacher_student",
            "secondary_model": {"provider": "openai", "name": "teacher", "source": "api"},
        },
    }
    beta = {
        "agent_model": {"provider": "openai", "name": "student", "source": "api"},
        "multi_model": {
            "enabled": True,
            "policy": "beta_mixture",
            "secondary_model": {"provider": "openai", "name": "teacher", "source": "api"},
            "beta": 0.5,
        },
    }
    assert admitted_service_execution_models(disabled) == (("openai", "student"),)
    assert admitted_service_execution_models(beta) == (("openai", "student"),)
    assert admitted_service_execution_models(None) == ()
