import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StrictMode, type ReactNode } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  apiFetch,
  apiUpload,
  apiDownload,
  getPipelineLivePreviewFrame,
  setCsrfToken,
  setUnauthorizedHandler,
} from "../api";
import { AuthProvider } from "../auth/AuthContext";
import { useAuth } from "../auth/useAuth";
import {
  setBrowserFailureReporter,
  type BrowserFailureReport,
} from "../lib/errorReporting";

const memberMe = {
  user: {
    id: "user-1",
    username: "Owner",
    email: "owner@example.com",
    display_name: "Owner Example",
    is_platform_admin: false,
  },
  teams: [
    { id: "team-a", name: "Alpha", role: "viewer" },
    { id: "team-b", name: "Beta", role: "owner" },
  ],
  current_team: { id: "team-a", name: "Alpha", role: "viewer" },
  role: "viewer",
  scopes: ["read:own"],
  is_platform_admin: false,
  csrf_token: "csrf-initial",
};

function Display(): JSX.Element {
  const {
    me,
    isAuthenticated,
    isLoading,
    isAdmin,
    currentTeamId,
    sessionFailure,
    sessionStatus,
    loginStart,
    loginComplete,
    loginPassword,
    switchTeam,
    logout,
    refreshMe,
  } = useAuth();
  return (
    <div>
      <span data-testid="loading">{isLoading ? "loading" : "ready"}</span>
      <span data-testid="auth">{isAuthenticated ? "in" : "out"}</span>
      <span data-testid="username">{me?.user.username ?? "none"}</span>
      <span data-testid="email">{me?.user.email ?? "none"}</span>
      <span data-testid="team">{currentTeamId ?? "none"}</span>
      <span data-testid="admin">{isAdmin ? "admin" : "not-admin"}</span>
      <span data-testid="status">{sessionStatus}</span>
      <span data-testid="failure">
        {sessionFailure
          ? `${sessionFailure.kind}:${sessionFailure.referenceId}`
          : "no-failure"}
      </span>
      <button onClick={() => void loginStart("owner@example.com")}>start</button>
      <button onClick={() => void loginComplete("login-token").catch(() => undefined)}>complete</button>
      <button onClick={() => void loginPassword("Owner", "long-passphrase-1").catch(() => undefined)}>password</button>
      <button onClick={() => void switchTeam("team-b").catch(() => undefined)}>switch</button>
      <button onClick={() => void logout()}>logout</button>
      <button onClick={() => void refreshMe()}>refresh</button>
    </div>
  );
}

function withQueryClient(ui: ReactNode, qc?: QueryClient): JSX.Element {
  const client = qc ?? new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return <QueryClientProvider client={client}>{ui}</QueryClientProvider>;
}

describe("AuthContext", () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  afterEach(() => {
    setBrowserFailureReporter(null);
    setUnauthorizedHandler(() => undefined);
    setCsrfToken(null);
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it.each([
    ["JSON", () => apiFetch("/api/v1/trials")],
    ["upload", () => apiUpload("/api/v1/tasksets", new FormData())],
    ["download", () => apiDownload("/api/v1/trials/old/atif", "old.json")],
    ["preview", () => getPipelineLivePreviewFrame("run", "stage", "attempt", 1, null)],
  ])("ignores a delayed %s 401 after signing into a new session", async (_kind, request) => {
    let releaseOld!: (response: Response) => void;
    const pending = new Promise<Response>((resolve) => { releaseOld = resolve; });
    const nextMe = { ...memberMe, csrf_token: "csrf-new", user: { ...memberMe.user, username: "New session" } };
    vi.stubGlobal("fetch", vi.fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockReturnValueOnce(pending)
      .mockResolvedValueOnce(new Response("", { status: 401 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(nextMe), { status: 200 })));
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("in"));
    const oldRequest = request().catch((error: unknown) => error);
    await act(async () => { await apiFetch("/api/v1/batches").catch(() => undefined); });
    expect(screen.getByTestId("auth").textContent).toBe("out");
    await userEvent.click(screen.getByText("password"));
    await waitFor(() => expect(screen.getByTestId("username").textContent).toBe("New session"));
    qc.setQueryData(["new-session-data"], "retained");
    await act(async () => { releaseOld(new Response("", { status: 401 })); await oldRequest; });
    expect(await oldRequest).toMatchObject({ status: 401 });
    expect(screen.getByTestId("auth").textContent).toBe("in");
    expect(qc.getQueryData(["new-session-data"])).toBe("retained");
  });

  it("loads the browser session from /auth/me on mount", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify(memberMe), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("in"));
    expect(screen.getByTestId("status").textContent).toBe("authenticated");
    expect(screen.getByTestId("failure").textContent).toBe("no-failure");
    expect(screen.getByTestId("email").textContent).toBe("owner@example.com");
    expect(screen.getByTestId("team").textContent).toBe("team-a");
    expect(window.localStorage.getItem("loom_token")).toBe(null);
  });

  it("clears cached data with unknown authorization before establishing identity", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify(memberMe), { status: 200 }),
      ),
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    qc.setQueryData(["unknown-owner"], { secret: "untrusted" });

    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));

    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("authenticated"),
    );
    expect(qc.getQueryData(["unknown-owner"])).toBeUndefined();
  });

  it("treats 401 on mount as a signed-out expired session", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response("", { status: 401 })));
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );
    await waitFor(() => expect(screen.getByTestId("loading").textContent).toBe("ready"));
    expect(screen.getByTestId("auth").textContent).toBe("out");
    expect(screen.getByTestId("status").textContent).toBe("signed-out");
    expect(screen.getByTestId("failure").textContent).toBe("no-failure");
  });

  it("clears a prior unavailable state when refresh resolves to an exact 401", async () => {
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ detail: "service unavailable" }), {
          status: 500,
          headers: { "Content-Type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(new Response("", { status: 401 }));
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );

    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("unavailable"),
    );
    const firstFailure = screen.getByTestId("failure").textContent ?? "";
    expect(firstFailure).toMatch(/^http:WEB-[0-9A-F]{8}$/);
    expect(firstFailure).not.toContain("service unavailable");
    await user.click(screen.getByRole("button", { name: "refresh" }));
    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("signed-out"),
    );
    expect(screen.getByTestId("failure").textContent).toBe("no-failure");
    expect(screen.getByTestId("auth").textContent).toBe("out");
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(reports).toEqual([
      expect.objectContaining({
        kind: "auth-session-http",
        referenceId: firstFailure.replace("http:", ""),
      }),
    ]);
  });

  it("clears cache with unknown authorization before an initial session failure", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "service unavailable" }), {
          status: 503,
          headers: { "Content-Type": "application/json" },
        }),
      ),
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    qc.setQueryData(["tasks"], { items: [{ id: "existing" }] });
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
        qc,
      ),
    );

    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("unavailable"),
    );
    expect(screen.getByTestId("auth").textContent).toBe("out");
    expect(screen.getByTestId("failure").textContent).toMatch(
      /^http:WEB-[0-9A-F]{8}$/,
    );
    expect(screen.getByTestId("failure").textContent).not.toContain(
      "service unavailable",
    );
    expect(qc.getQueryData(["tasks"])).toBeUndefined();
  });

  it.each([
    ["204", "invalid", () => Promise.resolve(new Response(null, { status: 204 }))],
    [
      "malformed 200",
      "invalid",
      () =>
        Promise.resolve(
          new Response("not-json", {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        ),
    ],
    [
      "500",
      "http",
      () =>
        Promise.resolve(
          new Response(JSON.stringify({ detail: "service unavailable" }), {
            status: 500,
            headers: { "Content-Type": "application/json" },
          }),
        ),
    ],
    [
      "network failure",
      "network",
      () => Promise.reject(new TypeError("network failed with token=secret")),
    ],
  ] as const)("marks %s auth/me outcomes as unavailable rather than anonymous", async (
    _,
    expectedKind,
    reply,
  ) => {
    vi.stubGlobal("fetch", vi.fn().mockImplementation(reply));
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
      ),
    );

    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("unavailable"),
    );
    expect(screen.getByTestId("auth").textContent).toBe("out");
    expect(screen.getByTestId("failure").textContent).toMatch(
      new RegExp(`^${expectedKind}:WEB-[0-9A-F]{8}$`),
    );
    expect(screen.getByTestId("failure").textContent).not.toContain("secret");
  });

  it("deduplicates the initial session request and report under StrictMode", async () => {
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ detail: "upstream token=secret" }), {
        status: 503,
        headers: { "Content-Type": "application/json" },
      }),
    );
    vi.stubGlobal("fetch", fetchMock);

    render(
      withQueryClient(
        <StrictMode>
          <AuthProvider>
            <Display />
          </AuthProvider>
        </StrictMode>,
      ),
    );

    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("unavailable"),
    );
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(reports).toHaveLength(1);
    expect(reports[0]).toMatchObject({ kind: "auth-session-http" });
    expect(JSON.stringify(reports[0])).not.toContain("secret");
  });

  it("retries an unavailable session without clearing same-identity cached data", async () => {
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ detail: "proxy secret" }), { status: 503 }),
      )
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));

    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("authenticated"),
    );
    qc.setQueryData(["tasks"], { items: [{ id: "existing" }] });

    await user.click(screen.getByRole("button", { name: "refresh" }));
    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("unavailable"),
    );
    const referenceId = (screen.getByTestId("failure").textContent ?? "").replace(
      "http:",
      "",
    );
    expect(qc.getQueryData(["tasks"])).toEqual({ items: [{ id: "existing" }] });

    await user.click(screen.getByRole("button", { name: "refresh" }));
    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("authenticated"),
    );
    expect(qc.getQueryData(["tasks"])).toEqual({ items: [{ id: "existing" }] });
    expect(fetchMock).toHaveBeenCalledTimes(3);
    expect(reports).toEqual([
      expect.objectContaining({ kind: "auth-session-http", referenceId }),
    ]);
  });

  it("clears cached data when a refreshed session changes identity", async () => {
    const replacementMe = {
      ...memberMe,
      user: { ...memberMe.user, id: "user-2", username: "Other" },
    };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify(replacementMe), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));

    await waitFor(() => expect(screen.getByTestId("username")).toHaveTextContent("Owner"));
    qc.setQueryData(["tasks"], { items: [{ id: "old-user" }] });
    await user.click(screen.getByRole("button", { name: "refresh" }));

    await waitFor(() => expect(screen.getByTestId("username")).toHaveTextContent("Other"));
    expect(qc.getQueryData(["tasks"])).toBeUndefined();
  });

  it("clears cached data when the same identity loses authorization", async () => {
    const privilegedMe = {
      ...memberMe,
      teams: [
        { id: "team-a", name: "Alpha", role: "owner" },
        memberMe.teams[1],
      ],
      current_team: { id: "team-a", name: "Alpha", role: "owner" },
      role: "owner",
      scopes: ["read:own", "submit", "team:manage"],
    };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify(privilegedMe), { status: 200 }),
      )
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));

    await waitFor(() => expect(screen.getByTestId("status")).toHaveTextContent("authenticated"));
    qc.setQueryData(["team-owner-data"], { secret: "old-authorization" });
    await user.click(screen.getByRole("button", { name: "refresh" }));

    await waitFor(() => expect(screen.getByTestId("status")).toHaveTextContent("authenticated"));
    expect(qc.getQueryData(["team-owner-data"])).toBeUndefined();
  });

  it("clears shared cached data when a remounted provider establishes identity", async () => {
    const replacementMe = {
      ...memberMe,
      user: { ...memberMe.user, id: "user-2", username: "Other" },
    };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify(replacementMe), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const firstMount = render(
      withQueryClient(<AuthProvider><Display /></AuthProvider>, qc),
    );
    await waitFor(() => expect(screen.getByTestId("username")).toHaveTextContent("Owner"));
    qc.setQueryData(["private", "user-1"], { value: "must-not-cross-provider" });
    firstMount.unmount();

    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));

    await waitFor(() => expect(screen.getByTestId("username")).toHaveTextContent("Other"));
    expect(qc.getQueryData(["private", "user-1"])).toBeUndefined();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("preserves cached data across a remount with the same authorization", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const firstMount = render(
      withQueryClient(<AuthProvider><Display /></AuthProvider>, qc),
    );
    await waitFor(() => expect(screen.getByTestId("username")).toHaveTextContent("Owner"));
    qc.setQueryData(["private", "user-1"], { value: "same-authorization" });
    firstMount.unmount();

    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));

    await waitFor(() => expect(screen.getByTestId("username")).toHaveTextContent("Owner"));
    expect(qc.getQueryData(["private", "user-1"])).toEqual({
      value: "same-authorization",
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("serializes authoritative session reads across provider remounts", async () => {
    let resolveOldSession!: (response: Response) => void;
    const oldSession = new Promise<Response>((resolve) => {
      resolveOldSession = resolve;
    });
    const lateSession = { ...memberMe, csrf_token: "csrf-late-old" };
    const currentSession = { ...memberMe, csrf_token: "csrf-current" };
    const finalSession = { ...memberMe, csrf_token: "csrf-final" };
    const fetchMock = vi
      .fn()
      .mockReturnValueOnce(oldSession)
      .mockResolvedValueOnce(
        new Response(JSON.stringify(currentSession), { status: 200 }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify(finalSession), { status: 200 }),
      );
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    const oldMount = render(
      withQueryClient(<AuthProvider><Display /></AuthProvider>, qc),
    );
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));
    oldMount.unmount();

    const currentMount = render(
      withQueryClient(<AuthProvider><Display /></AuthProvider>, qc),
    );
    await act(async () => {
      await Promise.resolve();
    });
    expect(fetchMock).toHaveBeenCalledTimes(1);

    await act(async () => {
      resolveOldSession(
        new Response(JSON.stringify(lateSession), { status: 200 }),
      );
      await oldSession;
    });
    await waitFor(() =>
      expect(screen.getByTestId("status")).toHaveTextContent("authenticated"),
    );
    expect(fetchMock).toHaveBeenCalledTimes(2);
    qc.setQueryData(["private", "user-1"], { value: "current-session" });

    await apiFetch("/api/v1/probe", { method: "POST", body: "{}" });
    const probe = fetchMock.mock.calls[2][1] as RequestInit;
    expect((probe.headers as Record<string, string>)["X-Loom-CSRF"]).toBe(
      "csrf-current",
    );

    currentMount.unmount();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));

    await waitFor(() =>
      expect(screen.getByTestId("status")).toHaveTextContent("authenticated"),
    );
    expect(qc.getQueryData(["private", "user-1"])).toEqual({
      value: "current-session",
    });
    expect(fetchMock).toHaveBeenCalledTimes(4);
  });

  it("reconciles a session mutation that settles after provider unmount", async () => {
    let resolveSwitch!: (response: Response) => void;
    const pendingSwitch = new Promise<Response>((resolve) => {
      resolveSwitch = resolve;
    });
    const switched = {
      ...memberMe,
      current_team: { id: "team-b", name: "Beta", role: "owner" },
      role: "owner",
      csrf_token: "csrf-switch-response",
    };
    const reconciled = { ...switched, csrf_token: "csrf-reconciled" };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify(memberMe), { status: 200 }),
      )
      .mockReturnValueOnce(pendingSwitch)
      .mockResolvedValueOnce(
        new Response(JSON.stringify(reconciled), { status: 200 }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();

    const oldMount = render(
      withQueryClient(<AuthProvider><Display /></AuthProvider>, qc),
    );
    await waitFor(() => expect(screen.getByTestId("team")).toHaveTextContent("team-a"));
    await user.click(screen.getByRole("button", { name: "switch" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    oldMount.unmount();

    render(
      withQueryClient(<AuthProvider><Display /></AuthProvider>, qc),
    );
    await act(async () => {
      await Promise.resolve();
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);

    await act(async () => {
      resolveSwitch(new Response(JSON.stringify(switched), { status: 200 }));
      await pendingSwitch;
    });
    await waitFor(() =>
      expect(screen.getByTestId("team")).toHaveTextContent("team-b"),
    );
    expect(screen.getByTestId("status")).toHaveTextContent("authenticated");
    expect(fetchMock).toHaveBeenCalledTimes(3);

    await apiFetch("/api/v1/probe", { method: "POST", body: "{}" });
    const probe = fetchMock.mock.calls[3][1] as RequestInit;
    expect((probe.headers as Record<string, string>)["X-Loom-CSRF"]).toBe(
      "csrf-reconciled",
    );
  });

  it("reconciles a logout that settles after provider unmount", async () => {
    let resolveLogout!: (response: Response) => void;
    const pendingLogout = new Promise<Response>((resolve) => {
      resolveLogout = resolve;
    });
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        new Response(JSON.stringify(memberMe), { status: 200 }),
      )
      .mockReturnValueOnce(pendingLogout)
      .mockResolvedValueOnce(new Response("", { status: 401 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    const oldMount = render(
      withQueryClient(<AuthProvider><Display /></AuthProvider>, qc),
    );
    await waitFor(() => expect(screen.getByTestId("auth")).toHaveTextContent("in"));
    await user.click(screen.getByRole("button", { name: "logout" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    oldMount.unmount();

    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));
    await act(async () => {
      await Promise.resolve();
    });
    expect(fetchMock).toHaveBeenCalledTimes(2);

    await act(async () => {
      resolveLogout(new Response(null, { status: 204 }));
      await pendingLogout;
    });
    await waitFor(() =>
      expect(screen.getByTestId("status")).toHaveTextContent("signed-out"),
    );
    expect(fetchMock).toHaveBeenCalledTimes(3);

    await apiFetch("/api/v1/probe", { method: "POST", body: "{}" });
    const probe = fetchMock.mock.calls[3][1] as RequestInit;
    expect(probe.headers).not.toHaveProperty("X-Loom-CSRF");
  });

  it("does not let a stale session response override an external 401", async () => {
    let resolveSession!: (response: Response) => void;
    const pendingSession = new Promise<Response>((resolve) => {
      resolveSession = resolve;
    });
    const fetchMock = vi
      .fn()
      .mockReturnValueOnce(pendingSession)
      .mockResolvedValueOnce(new Response("", { status: 401 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1));

    let requestError: unknown;
    await act(async () => {
      requestError = await apiFetch("/api/v1/trials").catch((error: unknown) => error);
    });
    expect(requestError).toMatchObject({ status: 401, detail: "unauthorized" });
    expect(screen.getByTestId("status")).toHaveTextContent("signed-out");

    await act(async () => {
      resolveSession(new Response(JSON.stringify(memberMe), { status: 200 }));
      await pendingSession;
    });
    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("signed-out"),
    );
    expect(screen.getByTestId("auth")).toHaveTextContent("out");
    expect(screen.getByTestId("username")).toHaveTextContent("none");

    await apiFetch("/api/v1/probe", { method: "POST", body: "{}" });
    const probe = fetchMock.mock.calls[2][1] as RequestInit;
    expect(probe.headers).not.toHaveProperty("X-Loom-CSRF");
  });

  it("clears the CSRF token while the session is unavailable", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockResolvedValueOnce(new Response("", { status: 503 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>));

    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("authenticated"),
    );
    await user.click(screen.getByRole("button", { name: "refresh" }));
    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("unavailable"),
    );
    await apiFetch("/api/v1/probe", { method: "POST", body: "{}" });

    const request = fetchMock.mock.calls[2][1] as RequestInit;
    expect(request.headers).not.toHaveProperty("X-Loom-CSRF");
  });

  it("loginComplete stores the returned session state and clears cached team data", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("", { status: 401 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify(memberMe), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
        qc,
      ),
    );
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("out"));
    qc.setQueryData(["batches"], { items: [{ id: "old" }] });
    await user.click(screen.getByText("complete"));
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("in"));
    expect(qc.getQueryData(["batches"])).toBeUndefined();
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/api/v1/auth/login/complete",
      expect.objectContaining({ method: "POST", credentials: "include" }),
    );
  });

  it("loginPassword stores the returned session state and clears cached data", async () => {
    const loginMe = { ...memberMe, csrf_token: "csrf-password" };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("", { status: 401 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify(loginMe), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(
      withQueryClient(
        <AuthProvider>
          <Display />
        </AuthProvider>,
        qc,
      ),
    );

    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("out"));
    qc.setQueryData(["batches"], { items: [{ id: "old" }] });
    await user.click(screen.getByText("password"));

    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("in"));
    expect(screen.getByTestId("username").textContent).toBe("Owner");
    expect(qc.getQueryData(["batches"])).toBeUndefined();
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/api/v1/auth/login",
      expect.objectContaining({
        method: "POST",
        credentials: "include",
        body: JSON.stringify({
          username: "Owner",
          password: "long-passphrase-1",
        }),
      }),
    );
  });

  it("loginComplete installs the returned CSRF token for the next mutation", async () => {
    const loginMe = { ...memberMe, csrf_token: "csrf-login" };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response("", { status: 401 }))
      .mockResolvedValueOnce(
        new Response(JSON.stringify(loginMe), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal("fetch", fetchMock);
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>));

    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("out"));
    await user.click(screen.getByText("complete"));
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("in"));
    await user.click(screen.getByText("logout"));

    const logoutInit = fetchMock.mock.calls[2][1] as RequestInit;
    expect((logoutInit.headers as Record<string, string>)["X-Loom-CSRF"]).toBe(
      "csrf-login",
    );
  });

  it("switchTeam updates current team and clears cached data", async () => {
    const switched = {
      ...memberMe,
      current_team: { id: "team-b", name: "Beta", role: "owner" },
      role: "owner",
      csrf_token: "csrf-switched",
    };
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(switched), { status: 200 }));
    vi.stubGlobal(
      "fetch",
      fetchMock,
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));
    await waitFor(() => expect(screen.getByTestId("team").textContent).toBe("team-a"));
    qc.setQueryData(["tokens"], { items: [{ id: "old" }] });
    await user.click(screen.getByText("switch"));
    await waitFor(() => expect(screen.getByTestId("team").textContent).toBe("team-b"));
    expect(qc.getQueryData(["tokens"])).toBeUndefined();
    const switchInit = fetchMock.mock.calls[1][1] as RequestInit;
    expect((switchInit.headers as Record<string, string>)["X-Loom-CSRF"]).toBe(
      "csrf-initial",
    );
  });

  it("fails closed and reconciles an ambiguous team-switch response", async () => {
    const switched = {
      ...memberMe,
      current_team: { id: "team-b", name: "Beta", role: "owner" },
      role: "owner",
      csrf_token: "csrf-switched",
    };
    const reports: BrowserFailureReport[] = [];
    setBrowserFailureReporter((report) => reports.push(report));
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockResolvedValueOnce(new Response("not-json", { status: 200 }))
      .mockResolvedValueOnce(new Response(JSON.stringify(switched), { status: 200 }));
    vi.stubGlobal("fetch", fetchMock);
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));
    await waitFor(() => expect(screen.getByTestId("team")).toHaveTextContent("team-a"));
    qc.setQueryData(["team", "team-a"], { secret: "old-team" });

    await user.click(screen.getByRole("button", { name: "switch" }));

    await waitFor(() =>
      expect(screen.getByTestId("status").textContent).toBe("unavailable"),
    );
    const failure = screen.getByTestId("failure").textContent ?? "";
    expect(failure).toMatch(/^invalid:WEB-[0-9A-F]{8}$/);
    expect(screen.getByTestId("team")).toHaveTextContent("none");
    expect(qc.getQueryData(["team", "team-a"])).toBeUndefined();
    expect(reports).toEqual([
      expect.objectContaining({
        kind: "auth-session-invalid",
        referenceId: failure.replace("invalid:", ""),
      }),
    ]);

    await user.click(screen.getByRole("button", { name: "refresh" }));

    await waitFor(() => expect(screen.getByTestId("team")).toHaveTextContent("team-b"));
    expect(screen.getByTestId("status")).toHaveTextContent("authenticated");
    expect(fetchMock).toHaveBeenCalledTimes(3);
  });

  it("logout clears auth state and cache", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(new Response(JSON.stringify(memberMe), { status: 200 }))
      .mockResolvedValueOnce(new Response(null, { status: 204 }));
    vi.stubGlobal(
      "fetch",
      fetchMock,
    );
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const user = userEvent.setup();
    render(withQueryClient(<AuthProvider><Display /></AuthProvider>, qc));
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("in"));
    qc.setQueryData(["batches"], { items: [{ id: "old" }] });
    await user.click(screen.getByText("logout"));
    await waitFor(() => expect(screen.getByTestId("auth").textContent).toBe("out"));
    expect(qc.getQueryData(["batches"])).toBeUndefined();
    const logoutInit = fetchMock.mock.calls[1][1] as RequestInit;
    expect((logoutInit.headers as Record<string, string>)["X-Loom-CSRF"]).toBe(
      "csrf-initial",
    );
  });
});
