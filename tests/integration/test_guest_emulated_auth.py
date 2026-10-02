"""Real PKCS#11 authentication inside software guests with confined outer containers.

The generic fixture is independently authored; no benchmark task or private
grading inputs are used. CI must build LOOM_GUEST_PAYLOAD in the Docker lane.
"""

from __future__ import annotations

import base64
import contextlib
import json
import os
import shlex
import subprocess
import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import httpx
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.docker, pytest.mark.timeout(300)]
FIXTURE = Path(__file__).resolve().parents[1] / "fixtures/emulated_auth"


def docker(
    *arguments: str, check: bool = True, timeout: float = 60
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *arguments],
        check=check,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@pytest.fixture(scope="module")
def auth_image() -> Iterator[tuple[str, Path]]:
    configured = os.environ.get("LOOM_GUEST_PAYLOAD")
    if configured is None:
        if os.environ.get("GITHUB_ACTIONS") == "true":
            pytest.fail("CI must build LOOM_GUEST_PAYLOAD before emulated-auth qualification")
        pytest.skip("requires built LOOM_GUEST_PAYLOAD")
    payload = Path(configured).resolve()
    assert (payload / "bin/loom-guest-runtime").is_file()
    assert (payload / "bin/loom-sandbox-runtime").is_file()
    image = f"loom-emulated-auth-test:{uuid4().hex}"
    try:
        built = docker("build", "-t", image, str(FIXTURE), check=False, timeout=240)
        assert built.returncode == 0, built.stdout + built.stderr
        yield image, payload
    finally:
        docker("image", "rm", "--force", image, check=False)


@dataclass
class AuthGuest:
    client: httpx.Client
    name: str
    state: Path

    def command(self, script: str, *, success: bool = True) -> tuple[int, str, str]:
        response = self.client.post(
            "http://sandbox/exec",
            json={
                "argv": ["/bin/bash", "-ec", script],
                "cwd": "/root",
                "timeout_sec": 60,
            },
        )
        response.raise_for_status()
        result = response.json()
        code = int(result["return_code"])
        stdout = base64.b64decode(result.get("stdout") or "").decode()
        stderr = base64.b64decode(result.get("stderr") or "").decode()
        if success:
            assert code == 0, (code, stdout, stderr)
        return code, stdout, stderr

    def serve(self, token: str = "trusted") -> str:
        # A daemon retaining the RPC pipe is a genuine unfinished command.
        _, output, _ = self.command(
            'eval "$(p11-kit server --provider /usr/lib/softhsm/libsofthsm2.so '
            f'pkcs11:token={token} 2>/run/auth-provider.log)"; '
            'printf "%s\\n" "$P11_KIT_SERVER_ADDRESS"',
        )
        address = output.strip()
        assert address.startswith("unix:path=/run/user/0/")
        return address.removeprefix("unix:path=")

    def authenticate(self, socket: str | None, *, wrong_pin: bool = False) -> tuple[int, str, str]:
        forward = (
            "" if socket is None else "-R /run/user/1100/p11-kit/pkcs11:" + shlex.quote(socket)
        )
        pin = "printf 'incorrect\\n'" if wrong_pin else "cat /root/auth-fixture/pin"
        # The secret is read inside the guest and never enters RPC arguments/logs.
        return self.command(
            f"{pin} | ssh -o BatchMode=yes -o ExitOnForwardFailure=yes "
            "-o StrictHostKeyChecking=yes -p 2222 "
            + forward
            + " authuser@localhost \"sudo -S -p '' /usr/bin/id -u\"",
            success=False,
        )


@contextlib.contextmanager
def auth_guest(image: str, payload: Path) -> Iterator[AuthGuest]:
    name = "loom-emulated-auth-" + uuid4().hex[:12]
    with tempfile.TemporaryDirectory(prefix="loom-auth-", dir="/tmp") as temporary:
        directory = Path(temporary)
        state, rpc = directory / "state", directory / "rpc"
        state.mkdir(mode=0o700)
        rpc.mkdir(mode=0o2770)
        rpc.chmod(0o2770)  # Runtime-created RPC socket keeps the caller's group.
        client = httpx.Client(
            transport=httpx.HTTPTransport(uds=str(rpc / "sandbox.sock")),
            timeout=125,
        )
        started = False
        try:
            docker(
                "run",
                "--detach",
                "--name",
                name,
                "--network",
                "none",
                "--read-only",
                "--cap-drop=ALL",
                "--cap-add=DAC_OVERRIDE",
                "--security-opt=no-new-privileges",
                "--cpus=1",
                "--memory=2048m",
                "--memory-swap=2048m",
                "--pids-limit=128",
                "--volume",
                f"{payload}:/payload:ro",
                "--volume",
                f"{state}:/state",
                "--volume",
                f"{rpc}:/rpc",
                "--entrypoint",
                "/payload/bin/loom-guest-runtime",
                image,
                "--payload",
                "/payload",
                "--root",
                "/",
                "--state",
                "/state/incarnation",
                "--socket",
                "/rpc/sandbox.sock",
                "--memory-mib",
                "2048",
                "--storage-mib",
                "512",
                "--cpu-millis",
                "1000",
                "--exec-timeout-seconds",
                "60",
            )
            started = True
            metadata = json.loads(docker("inspect", name).stdout)[0]
            host = metadata["HostConfig"]
            assert host["ReadonlyRootfs"] and not host["Privileged"]
            assert host["CapDrop"] == ["ALL"]
            assert host["CapAdd"] in (["DAC_OVERRIDE"], ["CAP_DAC_OVERRIDE"])
            assert "no-new-privileges" in host["SecurityOpt"]
            assert host["NetworkMode"] == "none" and not host["Devices"]
            assert host["PidMode"] == "" and not host["PortBindings"]
            assert {mount["Destination"] for mount in metadata["Mounts"]} == {
                "/payload",
                "/state",
                "/rpc",
            }
            deadline = time.monotonic() + 125
            while True:
                try:
                    if client.get("http://sandbox/health").is_success:
                        break
                except httpx.TransportError:
                    pass
                running = json.loads(docker("inspect", name).stdout)[0]["State"]["Running"]
                assert running and time.monotonic() < deadline, docker("logs", name).stderr
                time.sleep(0.1)
            guest = AuthGuest(client, name, state)
            guest.command("/usr/local/bin/prepare-auth-fixture")
            yield guest
        finally:
            client.close()
            try:
                if started:
                    stopped = docker("stop", "--time", "10", name, check=False, timeout=20)
                    assert stopped.returncode == 0, stopped.stderr
                    retired = docker(
                        "run",
                        "--rm",
                        "--network",
                        "none",
                        "--read-only",
                        "--cap-drop=ALL",
                        "--cap-add=DAC_OVERRIDE",
                        "--security-opt=no-new-privileges",
                        "--volume",
                        f"{state}:/owned",
                        "--entrypoint",
                        "/bin/sh",
                        image,
                        "-c",
                        "status=0; test ! -e /owned/incarnation/state.ext4 "
                        "&& test ! -e /owned/incarnation/channel.sock "
                        "&& test -e /owned/incarnation/retired || status=$?; "
                        'rm -rf /owned/incarnation; exit "$status"',
                    )
                    assert retired.returncode == 0
            finally:
                docker("rm", "--force", name, check=False)
                assert docker("inspect", name, check=False).returncode != 0


def assert_authenticated(result: tuple[int, str, str]) -> None:
    code, output, error = result
    assert code == 0 and "0" in output.splitlines(), (code, output, error)


def assert_rejected(result: tuple[int, str, str]) -> None:
    code, output, error = result
    assert code != 0 and "0" not in output.splitlines(), (code, output, error)
    assert "no new privileges" not in error, "must reach authentication inside the guest"


def test_guest_authentication_requires_forwarding_pin_and_trusted_certificate(
    auth_image: tuple[str, Path],
) -> None:
    """Catch direct-provider fallback, NOPASSWD/cached sudo, or unchecked certificates."""
    with auth_guest(*auth_image) as guest:
        guest.command("test ! -e /run/user/1100/p11-kit/pkcs11; visudo -c")
        socket = guest.serve()
        assert_rejected(guest.authenticate(None))  # Provider alive; forwarding missing.
        assert_rejected(guest.authenticate(socket, wrong_pin=True))
        assert_authenticated(guest.authenticate(socket))
        assert_rejected(guest.authenticate(socket, wrong_pin=True))  # No sudo -k.
        _, owner, _ = guest.command("stat -c '%u:%g:%a' /run/user/1100/p11-kit/pkcs11")
        assert owner.strip() == "1100:1100:600"
        untrusted = guest.serve("untrusted")
        # Ensure a rejected token actually has a usable signing key and correct PIN.
        guest.command("/usr/local/bin/check-untrusted-signature")
        assert_rejected(guest.authenticate(untrusted))
        assert_authenticated(guest.authenticate(socket))


def test_concurrent_guests_isolate_cards_sockets_and_service_cleanup(
    auth_image: tuple[str, Path],
) -> None:
    """Catch shared token state, shared runtime sockets or teardown of a sibling guest."""
    with auth_guest(*auth_image) as right:
        with auth_guest(*auth_image) as left:
            _, first, _ = left.command("sha256sum /root/auth-fixture/trusted.der")
            _, second, _ = right.command("sha256sum /root/auth-fixture/trusted.der")
            assert first.split()[0] != second.split()[0]
            left_socket = left.serve()
            assert_authenticated(left.authenticate(left_socket))
            right.command("test ! -e /run/user/1100/p11-kit/pkcs11")
            assert_rejected(right.authenticate(None))
            right_socket = right.serve()
            assert_authenticated(right.authenticate(right_socket))
            left.command("test -S /run/user/1100/p11-kit/pkcs11")
        # The left guest's active provider/sshd are gone; the right still authenticates.
        assert_authenticated(right.authenticate(right_socket))
