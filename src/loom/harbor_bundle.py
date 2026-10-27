"""Shared Harbor bundle preparation for catalog and custom TaskSet intake."""

from pathlib import Path
from typing import Any

from loom.harbor_compose import project_single_service_compose
from loom.harbor_verifier_script import VERIFIER_SCRIPT_PATH, native_verifier_run_sh_bytes


def prepare_harbor_bundle(bundle: Path, config: dict[str, Any]) -> None:
    """Add the native reward bridge without rewriting authored task files.

    Mutate only the projected config and add one private generated script.
    Network defaults describe hosted HTTP(S), never unrestricted TCP egress.
    Explicit policies and verifier environment declarations remain authoritative.
    """
    verifier = config["verifier"]
    if verifier["name"] != "script":
        raise ValueError("unsupported Harbor requirement: verifier.name requires the native script verifier")
    if not (bundle / "instruction.md").is_file() or not (bundle / "tests/test.sh").is_file():
        raise ValueError("native Harbor package requires instruction.md and tests/test.sh")
    bridge = bundle / VERIFIER_SCRIPT_PATH
    if (bundle / "verifier").is_symlink() or bridge.exists() or bridge.is_symlink():
        raise ValueError("native package collides with reserved verifier/run.sh")

    environment = config["environment"]
    if "baseline_network_policy" not in environment and "network_policies_supported" not in environment:
        # Harbor enables internet unless the task declares otherwise. Use the
        # same hosted policy as allow_internet=true; retain offline/allowlists.
        environment["baseline_network_policy"] = {"kind": "public-web"}
        environment["network_policies_supported"] = ["public-web"]

    compose = [
        f"environment/{name}"
        for name in ("compose.yaml", "compose.yml", "docker-compose.yaml", "docker-compose.yml")
        if (bundle / "environment" / name).exists()
    ]
    if compose:
        environment["compose_files"] = compose
        project_single_service_compose(bundle, environment)
        if environment.get("single_service_compose") is None:
            from loom.guest_compose import compile_native_compose

            try:
                projected = compile_native_compose(bundle, environment)
            except ValueError:
                # Unsupported declarations remain intact and explicitly blocked.
                # Merely finding a Compose file never grants runtime support.
                pass
            else:
                from loom.models.compose import ComposeBuild

                services = []
                for service in projected.services:
                    if service.build is None:
                        context = f".loom-build/compose-{service.name}"
                        generated = bundle / context / "Dockerfile"
                        if generated.exists() or generated.is_symlink():
                            raise ValueError("native package collides with managed Compose image build")
                        generated.parent.mkdir(parents=True, exist_ok=True)
                        generated.write_text(f"FROM {service.image}\n")
                        service = service.model_copy(update={"build": ComposeBuild(
                            context=context, dockerfile=f"{context}/Dockerfile",
                        )})
                    services.append(service)
                projected = projected.model_copy(update={"services": tuple(services)})
                environment["native_compose"] = projected.model_dump(mode="json")
                main = next(service for service in projected.services if service.name == "main")
                if main.build is not None:
                    environment.update(dockerfile=main.build.dockerfile, docker_build_context=main.build.context,
                                       docker_build_args=main.build.args, docker_build_target=main.build.target)
        environment.pop("image_workdir", None)
    else:
        # Harbor runs as the image USER (root when the image omits USER) and
        # keeps ENTRYPOINT initializers. Never substitute Loom's controller UID.
        environment.setdefault("user", "image")
        environment.setdefault("image_entrypoint", "entrypoint" not in environment)
    if (bundle / "tests/Dockerfile").is_file():
        env = dict(verifier.get("environment") or environment)
        env.pop("docker_image", None)
        env.pop("compose_files", None)
        env.pop("single_service_compose", None)
        env.pop("native_compose", None)
        env.update(dockerfile="tests/Dockerfile", docker_build_context="tests")
        verifier["environment"] = env
        verifier.setdefault("env_mode", "separate")
    if verifier.get("environment") is not None:
        verifier.setdefault("env_mode", "separate")
    verifier.setdefault("env_mode", "shared")
    if environment.get("single_service_compose", {}).get("privileged"):
        verifier.setdefault("user", "root")
    bridge.parent.mkdir(exist_ok=True)
    bridge.write_bytes(native_verifier_run_sh_bytes())
    bridge.chmod(0o755)
    verifier["args"]["script_path"] = VERIFIER_SCRIPT_PATH
