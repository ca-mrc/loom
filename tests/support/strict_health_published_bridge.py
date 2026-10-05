"""Start method from bridge 44dbda72dff90fde5c29b094227db6c5ee03389b.

Retained solely to reproduce #2337 against the current sandbox health response.
Source: src/loom/driver/service_sandbox.py at that immutable Git revision.
"""

import httpx

from loom.driver.base import StartOptions
from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.errors import DriverAlreadyStartedError, DriverError


class StrictHealthPublishedBridge(ServiceSandboxDriver):
    async def start(self, *, options: StartOptions | None = None) -> None:
        if self._started:
            raise DriverAlreadyStartedError("sandbox driver already started")
        if options is not None and options != StartOptions():
            raise DriverError("native sandbox options must be enforced by its Pod")
        client = httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=str(self._socket_path)),
            base_url="http://sandbox",
            timeout=10,
            trust_env=False,
        )
        try:
            response = await client.get("/health")
            response.raise_for_status()
            if response.json() != {"ready": True}:
                raise DriverError("sandbox readiness response invalid")
        except BaseException:
            await client.aclose()
            raise
        self._client = client
        self._started = True
