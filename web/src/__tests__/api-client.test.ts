import { beforeEach, describe, expect, it, vi } from "vitest";

import { api, apiFetch, loadAuthSession, setCsrfToken, setUnauthorizedHandler } from "../api";
import { setFrontendConfigForTests } from "../lib/frontendConfig";

function validAuthSessionPayload() {
  return {
    user: {
      id: "user-1",
      username: "Owner",
      email: "owner@example.com",
      display_name: "Owner Example",
      is_platform_admin: false,
    },
    teams: [{ id: "team-a", name: "Alpha", role: "owner" }],
    current_team: { id: "team-a", name: "Alpha", role: "owner" },
    role: "owner",
    scopes: ["read:own"],
    is_platform_admin: false,
    csrf_token: "csrf-safe-session-value",
  };
}

describe("apiFetch", () => {
  beforeEach(() => {
    window.localStorage.clear();
    setFrontendConfigForTests(null);
    setCsrfToken(null);
    setUnauthorizedHandler(() => undefined);
    vi.restoreAllMocks();
  });

  it("uses cookie credentials and ignores localStorage bearer tokens", async () => {
    window.localStorage.setItem("loom_token", "abc");
    const spy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    await apiFetch("/api/v1/health");
    expect(spy).toHaveBeenCalled();
    const init = spy.mock.calls[0][1] as RequestInit;
    expect("Authorization" in (init.headers as object)).toBe(false);
    expect(init.credentials).toBe("include");
  });

  it("attaches CSRF header on unsafe requests from the in-memory token", async () => {
    setCsrfToken("csrf-123");
    const spy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
    await apiFetch("/api/v1/tokens", { method: "POST", body: "{}" });
    const init = spy.mock.calls[0][1] as RequestInit;
    expect((init.headers as Record<string, string>)["X-Loom-CSRF"]).toBe(
      "csrf-123",
    );
  });

  it("calls unauthorized handler on 401", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response("", { status: 401 }),
    );
    const onAuth = vi.fn();
    setUnauthorizedHandler(onAuth);
    await expect(apiFetch("/api/v1/trials")).rejects.toMatchObject({
      status: 401,
    });
    expect(onAuth).toHaveBeenCalled();
  });

  it("keeps a request bound to the unauthorized handler active at its start", async () => {
    let resolveResponse!: (response: Response) => void;
    const response = new Promise<Response>((resolve) => {
      resolveResponse = resolve;
    });
    vi.spyOn(globalThis, "fetch").mockReturnValue(response);
    const oldHandler = vi.fn();
    const newHandler = vi.fn();
    setUnauthorizedHandler(oldHandler);

    const request = apiFetch("/api/v1/trials");
    setUnauthorizedHandler(newHandler);
    resolveResponse(new Response("", { status: 401 }));

    await expect(request).rejects.toMatchObject({
      status: 401,
      detail: "unauthorized",
    });
    expect(oldHandler).toHaveBeenCalledOnce();
    expect(newHandler).not.toHaveBeenCalled();
  });

  it("throws ApiError with detail on 403", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ detail: "nope" }), {
        status: 403,
        headers: { "Content-Type": "application/json" },
      }),
    );
    await expect(apiFetch("/api/v1/trials")).rejects.toMatchObject({
      status: 403,
      detail: "nope",
    });
  });

  it("returns undefined on 204", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(null, { status: 204 }),
    );
    const result = await apiFetch("/api/v1/tokens/abc12345");
    expect(result).toBeUndefined();
  });

  it("prefixes API calls with the runtime frontend API base", async () => {
    setFrontendConfigForTests({
      environment: "production",
      environmentLabel: "Production",
      routePath: "/prod",
      apiBase: "/prod",
      apiRouteBase: "https://yylx.world/prod/api",
    });
    const spy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await apiFetch("/api/v1/health");

    expect(spy).toHaveBeenCalledWith(
      "/prod/api/v1/health",
      expect.objectContaining({ credentials: "include" }),
    );
  });

  it("bypasses HTTP caches for live overview and monitor summaries", async () => {
    const spy = vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
      new Response(JSON.stringify({}), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await api.getOverview();
    await api.getMonitorSummary({ view: "trials" });

    expect(spy).toHaveBeenNthCalledWith(
      1,
      "/api/v1/overview",
      expect.objectContaining({ cache: "no-store", credentials: "include" }),
    );
    expect(spy).toHaveBeenNthCalledWith(
      2,
      "/api/v1/monitor/summary?view=trials",
      expect.objectContaining({ cache: "no-store", credentials: "include" }),
    );
  });

  it("uses run-scoped cursor pages for StageRuns and Artifacts", async () => {
    const spy = vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
      new Response(JSON.stringify({ items: [], next_cursor: null }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await api.listPipelineStageRuns("run/one", {
      node_key: "generate_card_00",
      state: "succeeded",
      domain_outcome: "accepted",
      cursor: "opaque",
      limit: 200,
    });
    await api.listPipelineArtifacts("run/one", { cursor: "opaque", limit: 100 });

    expect(spy).toHaveBeenNthCalledWith(
      1,
      "/api/v1/pipeline-runs/run%2Fone/stages?node_key=generate_card_00&state=succeeded&domain_outcome=accepted&cursor=opaque&limit=200",
      expect.objectContaining({ credentials: "include" }),
    );
    expect(spy).toHaveBeenNthCalledWith(
      2,
      "/api/v1/pipeline-runs/run%2Fone/artifacts?cursor=opaque&limit=100",
      expect.objectContaining({ credentials: "include" }),
    );
  });

  it("sends admin actor header when approving a team registration", async () => {
    const spy = vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify({ registration: { id: "reg-1" } }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await api.approveTeamRegistration("reg-1", "ops-owner", {
      team_id: "team-1",
      role: "member",
    });

    expect(spy).toHaveBeenCalledWith(
      "/api/v1/admin/team-registrations/reg-1/approve",
      expect.objectContaining({ method: "POST" }),
    );
    const init = spy.mock.calls[0][1] as RequestInit;
    expect((init.headers as Record<string, string>)["X-Loom-Admin-Actor"]).toBe(
      "ops-owner",
    );
    expect(JSON.parse(String(init.body))).toEqual({
      team_id: "team-1",
      role: "member",
    });
  });
});

describe("delivery downloads", () => {
  beforeEach(() => {
    setFrontendConfigForTests(null);
    setCsrfToken(null);
    setUnauthorizedHandler(() => undefined);
    vi.restoreAllMocks();
  });

  it.each([
    ["", "relative"], ["", "absolute"], ["/dev", "relative"], ["/dev", "absolute"],
  ])("downloads an authenticated %s delivery using a %s URL", async (prefix, kind) => {
    setFrontendConfigForTests({
      environment: prefix ? "development" : "local", environmentLabel: "Test",
      routePath: prefix, apiBase: prefix, apiRouteBase: `${window.location.origin}${prefix}/api`,
    });
    const endpoint = "/api/v1/batches/batch-a/delivery-export/artifact-a/download?format=tar.gz";
    const absolute = `${window.location.origin}${prefix}${endpoint}`;
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("archive"));
    vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:delivery");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => undefined);
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (this: HTMLAnchorElement) {
      expect(this.download).toBe("delivery.tar.gz");
    });

    await api.downloadBatchDeliveryExport(kind === "absolute" ? absolute : endpoint, "delivery.tar.gz");

    expect(fetchSpy).toHaveBeenCalledWith(kind === "absolute" ? absolute : `${prefix}${endpoint}`,
      expect.objectContaining({ credentials: "include" }));
  });

  it.each([
    "https://other.example/dev/api/v1/download",
    "//other.example/dev/api/v1/download",
    `${window.location.origin}/prod/api/v1/download`,
  ])("rejects a delivery URL outside the current service: %s", async (url) => {
    setFrontendConfigForTests({
      environment: "development", environmentLabel: "Development",
      routePath: "/dev", apiBase: "/dev", apiRouteBase: `${window.location.origin}/dev/api`,
    });
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("archive"));
    vi.spyOn(URL, "createObjectURL").mockReturnValue("blob:delivery");
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => undefined);
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);

    await expect(api.downloadBatchDeliveryExport(url, "delivery.tar.gz"))
      .rejects.toThrow("Download URL does not belong to this Loom service.");
    expect(fetchSpy).not.toHaveBeenCalled();
  });
});

describe("auth session loader", () => {
  beforeEach(() => {
    setFrontendConfigForTests(null);
    setUnauthorizedHandler(() => undefined);
    vi.restoreAllMocks();
  });

  it("whitelists a valid session response", async () => {
    const payload = validAuthSessionPayload();
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        JSON.stringify({
          ...payload,
          user: {
            ...payload.user,
            raw_secret: "must-not-survive",
          },
          debug_response_body: "must-not-survive",
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );

    const session = await loadAuthSession();

    expect(session).toEqual({
      user: {
        id: "user-1",
        username: "Owner",
        email: "owner@example.com",
        display_name: "Owner Example",
        is_platform_admin: false,
      },
      teams: [{ id: "team-a", name: "Alpha", role: "owner" }],
      current_team: { id: "team-a", name: "Alpha", role: "owner" },
      role: "owner",
      scopes: ["read:own"],
      is_platform_admin: false,
      csrf_token: "csrf-safe-session-value",
    });
    expect(JSON.stringify(session)).not.toContain("must-not-survive");
  });

  it.each([
    [401, "unauthorized"],
    [403, "http"],
    [503, "http"],
  ])("classifies HTTP %s without consuming its body", async (status, kind) => {
    const response = new Response("raw upstream token=secret", { status });
    vi.spyOn(globalThis, "fetch").mockResolvedValue(response);

    await expect(loadAuthSession()).rejects.toMatchObject({
      kind,
      message: expect.not.stringContaining("raw upstream"),
    });
    expect(response.bodyUsed).toBe(false);
  });

  it("replaces a raw network exception with a fixed failure", async () => {
    vi.spyOn(globalThis, "fetch").mockRejectedValue(
      new Error("raw network token=secret"),
    );

    await expect(loadAuthSession()).rejects.toMatchObject({
      kind: "network",
      message: "browser session request failed",
    });
  });

  it.each([
    ["empty", () => new Response(null, { status: 204 })],
    ["malformed", () => new Response("not-json", { status: 200 })],
    [
      "invalid shape",
      () =>
        new Response(JSON.stringify({ debug_response_body: "raw secret" }), {
          status: 200,
        }),
    ],
  ])("rejects an %s session response as invalid", async (_label, response) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(response());

    await expect(loadAuthSession()).rejects.toMatchObject({
      kind: "invalid",
      message: "browser session response is invalid",
    });
  });

  it.each([
    [
      "missing CSRF token",
      () => {
        const payload = {
          ...validAuthSessionPayload(),
        } as Record<string, unknown>;
        delete payload.csrf_token;
        return payload;
      },
    ],
    ["empty CSRF token", () => ({ ...validAuthSessionPayload(), csrf_token: "" })],
    [
      "invalid username",
      () => {
        const payload = validAuthSessionPayload();
        return { ...payload, user: { ...payload.user, username: null } };
      },
    ],
    [
      "invalid optional user field",
      () => {
        const payload = validAuthSessionPayload();
        return { ...payload, user: { ...payload.user, display_name: 42 } };
      },
    ],
    [
      "inconsistent platform-admin flag",
      () => {
        const payload = validAuthSessionPayload();
        return {
          ...payload,
          user: { ...payload.user, is_platform_admin: true },
        };
      },
    ],
    [
      "current team outside memberships",
      () => ({
        ...validAuthSessionPayload(),
        current_team: { id: "team-b", name: "Beta", role: "owner" },
      }),
    ],
  ])("rejects a successful response with %s", async (_label, payload) => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(JSON.stringify(payload()), { status: 200 }),
    );

    await expect(loadAuthSession()).rejects.toMatchObject({
      kind: "invalid",
      message: "browser session response is invalid",
    });
  });

  it("classifies a failed session mutation without consuming its body", async () => {
    setCsrfToken("csrf-before-switch");
    const response = new Response("raw upstream token=secret", { status: 503 });
    const fetchSpy = vi.spyOn(globalThis, "fetch").mockResolvedValue(response);

    await expect(api.switchTeam("team-b")).rejects.toMatchObject({
      kind: "http",
      message: "browser session returned an unsuccessful status",
    });
    expect(response.bodyUsed).toBe(false);
    expect(fetchSpy).toHaveBeenCalledWith(
      "/api/v1/auth/team",
      expect.objectContaining({
        method: "POST",
        credentials: "include",
        body: JSON.stringify({ team_id: "team-b" }),
      }),
    );
    const init = fetchSpy.mock.calls[0][1] as RequestInit;
    expect((init.headers as Record<string, string>)["X-Loom-CSRF"]).toBe(
      "csrf-before-switch",
    );
  });

  it.each([
    [
      "network failure",
      () => Promise.reject(new Error("raw network token=secret")),
      "network",
    ],
    [
      "malformed success",
      () => Promise.resolve(new Response("not-json", { status: 200 })),
      "invalid",
    ],
  ] as const)("replaces a %s from a session mutation", async (
    _label,
    reply,
    kind,
  ) => {
    vi.spyOn(globalThis, "fetch").mockImplementation(reply);

    await expect(api.loginPassword("Owner", "secret-password")).rejects.toMatchObject({
      kind,
      message: expect.not.stringContaining("secret"),
    });
  });
});

describe("API token actor transport", () => {
  beforeEach(() => {
    setCsrfToken("csrf-token");
    vi.restoreAllMocks();
  });

  it("sends the optional admin actor for create, rotate, and revoke", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ token: "synthetic", item: {} }), {
          status: 201,
        }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ token: "synthetic", item: {} }), {
          status: 201,
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    const body = {
      name: "CLI",
      type: "team",
      scopes: ["read:own"],
      expires_in_days: 30,
    };

    await api.createToken(body, "qianyi");
    await api.rotateToken("abc/123", "qianyi");
    await api.revokeToken("abc/123", "qianyi");

    expect(fetchSpy).toHaveBeenNthCalledWith(
      1,
      "/api/v1/tokens",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          "X-Loom-Admin-Actor": "qianyi",
        }),
        body: JSON.stringify(body),
      }),
    );
    expect(fetchSpy).toHaveBeenNthCalledWith(
      2,
      "/api/v1/tokens/abc%2F123/rotate",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          "X-Loom-Admin-Actor": "qianyi",
        }),
      }),
    );
    expect(fetchSpy).toHaveBeenNthCalledWith(
      3,
      "/api/v1/tokens/abc%2F123",
      expect.objectContaining({
        method: "DELETE",
        headers: expect.objectContaining({
          "X-Loom-Admin-Actor": "qianyi",
        }),
      }),
    );
  });

  it("omits the admin actor for ordinary owner token mutations", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ token: "synthetic", item: {} }), {
          status: 201,
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));

    await api.rotateToken("owner-token");
    await api.revokeToken("owner-token");

    for (const [, init] of fetchSpy.mock.calls) {
      expect(
        (init?.headers as Record<string, string>)["X-Loom-Admin-Actor"],
      ).toBeUndefined();
    }
  });
});

describe("provider connection management endpoints", () => {
  beforeEach(() => {
    setCsrfToken("csrf-xyz");
    vi.stubGlobal("fetch", vi.fn());
  });

  afterEach(() => vi.restoreAllMocks());

  it("listProviderConnections scopes the request to the selected team", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ items: [] }), { status: 200 }),
    );
    const { api } = await import("../api");

    const result = await api.listProviderConnections("team-a");

    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections?team_id=team-a",
      expect.objectContaining({}),
    );
    expect(result).toEqual({ items: [] });
  });

  it("getProviderConnection GETs /api/v1/provider-connections/:id", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ id: "abc", name: "x" }), { status: 200 }),
    );
    const { api } = await import("../api");
    const result = await api.getProviderConnection("abc");
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc",
      expect.objectContaining({}),
    );
    expect(result).toEqual({ id: "abc", name: "x" });
  });

  it("createProviderConnection POSTs to /api/v1/provider-connections", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ id: "new" }), { status: 201 }),
    );
    const { api } = await import("../api");
    const payload = {
      name: "n", type: "openai-compatible",
      base_url: "https://example", api_key: "k",
    };
    const result = await api.createProviderConnection(payload);
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections",
      expect.objectContaining({ method: "POST", body: JSON.stringify(payload) }),
    );
    expect(result).toEqual({ id: "new" });
  });

  it("updateProviderConnection PATCHes /api/v1/provider-connections/:id", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ id: "abc" }), { status: 200 }),
    );
    const { api } = await import("../api");
    const patch = { allowed_models: ["m1"] };
    await api.updateProviderConnection("abc", patch);
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc",
      expect.objectContaining({ method: "PATCH", body: JSON.stringify(patch) }),
    );
  });

  it("deleteProviderConnection DELETEs /api/v1/provider-connections/:id", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(null, { status: 204 }),
    );
    const { api } = await import("../api");
    await api.deleteProviderConnection("abc");
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc",
      expect.objectContaining({ method: "DELETE" }),
    );
  });

  it("testProviderConnection POSTs /api/v1/provider-connections/:id/test", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ status: "valid" }), { status: 200 }),
    );
    const { api } = await import("../api");
    const result = await api.testProviderConnection("abc");
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/test",
      expect.objectContaining({ method: "POST" }),
    );
    expect(result).toEqual({ status: "valid" });
  });

  it("listProviderConnectionModels GETs /api/v1/provider-connections/:id/models", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ items: [] }), { status: 200 }),
    );
    const { api } = await import("../api");
    await api.listProviderConnectionModels("abc");
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/models",
      expect.objectContaining({}),
    );
  });

  it("addProviderConnectionModel POSTs /api/v1/provider-connections/:id/models", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({}), { status: 201 }),
    );
    const { api } = await import("../api");
    const model = { model_id: "manual/x" };
    await api.addProviderConnectionModel("abc", model);
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/models",
      expect.objectContaining({ method: "POST", body: JSON.stringify(model) }),
    );
  });

  it("refreshProviderConnectionModels POSTs .../models/refresh", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(JSON.stringify({ added: 0, removed: 0 }), { status: 200 }),
    );
    const { api } = await import("../api");
    await api.refreshProviderConnectionModels("abc");
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/models/refresh",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("hideProviderConnectionModel POSTs .../models/:mid/hide (url-encoded mid)", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(null, { status: 204 }),
    );
    const { api } = await import("../api");
    await api.hideProviderConnectionModel("abc", "openai/gpt-4");
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/models/openai%2Fgpt-4/hide",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("unhideProviderConnectionModel POSTs .../models/:mid/unhide", async () => {
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValue(
      new Response(null, { status: 204 }),
    );
    const { api } = await import("../api");
    await api.unhideProviderConnectionModel("abc", "openai/gpt-4");
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc/models/openai%2Fgpt-4/unhide",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("addProviderConnectionModel is the new name (createProviderConnectionModel is gone)", async () => {
    const mod = await import("../api");
    expect(mod.api.addProviderConnectionModel).toBeDefined();
    expect((mod.api as Record<string, unknown>).createProviderConnectionModel)
      .toBeUndefined();
  });
});
