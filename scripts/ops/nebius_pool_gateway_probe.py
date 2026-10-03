"""Fixed read-only probe using the gateway's own projected Kubernetes identity.

The parent binds the Pod and challenge to the completed startup journal. This
command accepts only namespace names and a challenge, never an endpoint, token,
CA path, executable or arbitrary API path. It grants no runtime authority.
"""

BOUND_GATEWAY_KUBERNETES_COMMAND = '''import asyncio, hmac, json, re, sys

async def run():
    if len(sys.argv) != 4:
        raise ValueError()
    names = json.loads(sys.argv[1])
    if (not isinstance(names, list) or not 1 <= len(names) <= 256
            or any(not isinstance(name, str) or re.fullmatch(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?", name) is None for name in names)
            or names != sorted(set(names))
            or any(re.fullmatch("[0-9a-f]{64}", value) is None for value in sys.argv[2:])):
        raise ValueError()
    import httpx
    from uuid import UUID
    from loom_service.pool_management.__main__ import PoolGatewaySettings
    from loom_service.environment_management.kubernetes_credentials import ProjectedKubernetesCredentials
    from loom_service.environment_management.runtime import NebiusManagementAuth
    settings = PoolGatewaySettings()
    connection = settings.kubernetes
    credentials = ProjectedKubernetesCredentials(connection)
    namespaces = {}
    try:
        async with asyncio.timeout(25):
            async with httpx.AsyncClient(base_url=connection.endpoint, verify=credentials.ssl_context,
                    auth=NebiusManagementAuth(connection, credentials), trust_env=False,
                    timeout=5, follow_redirects=False, headers={"Accept-Encoding": "identity"}) as client:
                for name in names:
                    async with client.stream("GET", "/api/v1/namespaces/" + name) as response:
                        if response.status_code != 200 or response.headers.get("Content-Encoding", "identity") != "identity":
                            raise ValueError()
                        payload = bytearray()
                        async for chunk in response.aiter_raw():
                            if len(payload) + len(chunk) > 512 * 1024:
                                raise ValueError()
                            payload.extend(chunk)
                    document = json.loads(payload)
                    metadata = document["metadata"]
                    if (document.get("apiVersion") != "v1" or document.get("kind") != "Namespace"
                            or metadata.get("name") != name or metadata.get("deletionTimestamp") is not None):
                        raise ValueError()
                    uid = UUID(metadata["uid"])
                    if not uid.int:
                        raise ValueError()
                    namespaces[name] = str(uid)
        actual = {"pool_id": str(settings.pool_id), "installation_id": str(settings.installation_id),
            "machine_id": str(settings.machine_id), "admission_epoch": settings.admission_epoch,
            "kubernetes": connection.model_dump(mode="json"), "namespaces": namespaces}
        signature = hmac.new(bytes.fromhex(sys.argv[2]),
            json.dumps(actual, sort_keys=True, separators=(",", ":")).encode(), "sha256").hexdigest()
        if not hmac.compare_digest(signature, sys.argv[3]):
            raise ValueError()
        return {"status": "qualified"}
    finally:
        await credentials.close()

try:
    print(json.dumps(asyncio.run(run())))
except Exception:
    print("Pool gateway Kubernetes access unqualified", file=sys.stderr)
    raise SystemExit(1)
'''
