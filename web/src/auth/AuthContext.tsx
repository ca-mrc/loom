/** Browser-session auth state backed by `/api/v1/auth/*`. */

import { useQueryClient, type QueryClient } from "@tanstack/react-query";
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import {
  AuthSessionLoadError,
  setCsrfToken,
  setUnauthorizedHandler,
} from "../api/core";
import {
  createBrowserFailureId,
  reportBrowserFailure,
} from "../lib/errorReporting";
import { loadAuthSession, TeamSwitchRejectedError, type AuthMe } from "../api/authSession";
import {
  AuthContext,
  type AuthCtx,
  type AuthSessionFailure,
  type AuthSessionUnavailableKind,
} from "./authContextValue";

type SessionState =
  | {
      status: "loading" | "authenticated" | "signed-out";
      failure: null;
    }
  | { status: "unavailable"; failure: AuthSessionFailure };

// Root-boundary recovery remounts AuthProvider but intentionally reuses the
// module QueryClient. Retain only the last trusted authorization fingerprint
// so a remount can reject cache from another user/team/privilege set.
const queryClientAuthorization = new WeakMap<QueryClient, string>();

// Root recovery reuses its QueryClient. Use it as the stable session-owner key
// so reads and writes remain ordered across AuthProvider remounts without
// coupling independent application roots.
const queryClientSessionOperation = new WeakMap<QueryClient, Promise<void>>();
const queryClientSessionAuthorityEpoch = new WeakMap<QueryClient, number>();

function sessionAuthorityEpoch(queryClient: QueryClient): number {
  return queryClientSessionAuthorityEpoch.get(queryClient) ?? 0;
}

function invalidateSessionAuthority(queryClient: QueryClient): void {
  queryClientSessionAuthorityEpoch.set(
    queryClient,
    sessionAuthorityEpoch(queryClient) + 1,
  );
  setCsrfToken(null);
}

function coordinateSessionOperation<T>(
  queryClient: QueryClient,
  operation: () => Promise<T>,
): Promise<T> {
  const sessionOperationTail =
    queryClientSessionOperation.get(queryClient) ?? Promise.resolve();
  const result = sessionOperationTail.then(operation, operation);
  queryClientSessionOperation.set(
    queryClient,
    result.then(
      () => undefined,
      () => undefined,
    ),
  );
  return result;
}

function loadAuthoritativeSession(queryClient: QueryClient): Promise<AuthMe> {
  const authorityEpoch = sessionAuthorityEpoch(queryClient);
  return coordinateSessionOperation(queryClient, async () => {
    setCsrfToken(null);
    try {
      const next = await loadAuthSession();
      // Keep the process-wide request layer aligned even if the Provider that
      // started this read unmounts before its React continuation runs.
      if (sessionAuthorityEpoch(queryClient) === authorityEpoch) {
        setCsrfToken(next.csrf_token);
      }
      return next;
    } catch (error) {
      setCsrfToken(null);
      throw error;
    }
  });
}

function mutateAuthoritativeSession(
  queryClient: QueryClient,
  request: () => Promise<AuthMe>,
): Promise<AuthMe> {
  const authorityEpoch = sessionAuthorityEpoch(queryClient);
  return coordinateSessionOperation(queryClient, async () => {
    try {
      const next = await request();
      if (sessionAuthorityEpoch(queryClient) === authorityEpoch) {
        setCsrfToken(next.csrf_token);
      }
      return next;
    } catch (error) {
      if (error instanceof TeamSwitchRejectedError) throw error;
      // A failed response may still follow a committed cookie/team change.
      // Withhold unsafe requests until a subsequent /me reconciles authority.
      setCsrfToken(null);
      throw error;
    }
  });
}

function clearAuthoritativeSession(
  queryClient: QueryClient,
  request: () => Promise<void>,
): Promise<void> {
  return coordinateSessionOperation(queryClient, async () => {
    try {
      await request();
    } finally {
      setCsrfToken(null);
    }
  });
}

function sessionAuthorizationFingerprint(me: AuthMe): string {
  return JSON.stringify({
    userId: me.user.id,
    currentTeamId: me.current_team?.id ?? null,
    currentTeamRole: me.current_team?.role ?? null,
    role: me.role,
    scopes: [...me.scopes].sort(),
    isPlatformAdmin: me.is_platform_admin,
    teams: [...me.teams]
      .map((team) => [team.id, team.role] as const)
      .sort(([leftId, leftRole], [rightId, rightRole]) =>
        leftId.localeCompare(rightId) || leftRole.localeCompare(rightRole),
      ),
  });
}

function unavailableKind(error: unknown): AuthSessionUnavailableKind {
  if (
    error instanceof AuthSessionLoadError &&
    error.kind !== "unauthorized"
  ) {
    return error.kind;
  }
  return "invalid";
}

export function AuthProvider({ children }: { children: ReactNode }): JSX.Element {
  const queryClient = useQueryClient();
  const [me, setMe] = useState<AuthMe | null>(null);
  const [session, setSession] = useState<SessionState>({
    status: "loading",
    failure: null,
  });
  const mountedAuthorizationRef = useRef<string | null>(
    queryClientAuthorization.get(queryClient) ?? null,
  );
  const [cacheReady, setCacheReady] = useState(
    mountedAuthorizationRef.current !== null,
  );
  const generationRef = useRef(0);
  const inFlightRef = useRef<Promise<void> | null>(null);
  const activeRef = useRef(false);
  const previousAuthorizationRef = useRef<string | null>(
    mountedAuthorizationRef.current,
  );

  useLayoutEffect(() => {
    if (cacheReady) return;
    // With no trusted fingerprint, pre-existing entries have unknown
    // provenance. Clear them before any child can start protected queries.
    queryClient.clear();
    setCacheReady(true);
  }, [cacheReady, queryClient]);

  useEffect(() => {
    activeRef.current = true;
    return () => {
      activeRef.current = false;
    };
  }, []);

  const installSession = useCallback(
    (next: AuthMe, clearCachedData: boolean): void => {
      const nextAuthorization = sessionAuthorizationFingerprint(next);
      if (
        clearCachedData ||
        (previousAuthorizationRef.current !== null &&
          previousAuthorizationRef.current !== nextAuthorization)
      ) {
        queryClient.clear();
      }
      previousAuthorizationRef.current = nextAuthorization;
      queryClientAuthorization.set(queryClient, nextAuthorization);
      setCsrfToken(next.csrf_token);
      setMe(next);
      setSession({ status: "authenticated", failure: null });
    },
    [queryClient],
  );

  const setSignedOut = useCallback(
    (invalidateRequest: boolean): void => {
      if (invalidateRequest) {
        generationRef.current += 1;
        inFlightRef.current = null;
      }
      previousAuthorizationRef.current = null;
      queryClientAuthorization.delete(queryClient);
      invalidateSessionAuthority(queryClient);
      setMe(null);
      queryClient.clear();
      setSession({ status: "signed-out", failure: null });
    },
    [queryClient],
  );

  const clearSessionState = useCallback((): void => {
    if (!activeRef.current) return;
    setSignedOut(true);
  }, [setSignedOut]);

  const enterUnavailable = useCallback(
    (error: unknown, clearAuthorization: boolean): void => {
      if (clearAuthorization) {
        previousAuthorizationRef.current = null;
        queryClientAuthorization.delete(queryClient);
        queryClient.clear();
      }
      const kind = unavailableKind(error);
      const referenceId = createBrowserFailureId();
      setCsrfToken(null);
      setMe(null);
      reportBrowserFailure(`auth-session-${kind}`, referenceId);
      setSession({
        status: "unavailable",
        failure: { kind, referenceId },
      });
    },
    [queryClient],
  );

  const refreshMe = useCallback((): Promise<void> => {
    if (!activeRef.current) return Promise.resolve();
    if (inFlightRef.current) return inFlightRef.current;

    const generation = generationRef.current + 1;
    generationRef.current = generation;
    setMe(null);
    setSession({ status: "loading", failure: null });

    const request = (async () => {
      try {
        const next = await loadAuthoritativeSession(queryClient);
        if (!activeRef.current || generation !== generationRef.current) return;
        installSession(next, false);
      } catch (error) {
        if (!activeRef.current || generation !== generationRef.current) return;
        if (
          error instanceof AuthSessionLoadError &&
          error.kind === "unauthorized"
        ) {
          setSignedOut(false);
          return;
        }

        enterUnavailable(error, false);
      } finally {
        if (activeRef.current && generation === generationRef.current) {
          inFlightRef.current = null;
        }
      }
    })();
    inFlightRef.current = request;
    return request;
  }, [enterUnavailable, installSession, queryClient, setSignedOut]);

  useEffect(() => {
    setUnauthorizedHandler(clearSessionState);
    return () => setUnauthorizedHandler(() => undefined);
  }, [clearSessionState]);

  useEffect(() => {
    void refreshMe();
  }, [refreshMe]);

  const loginStart = useCallback(
    async (email: string) => (await import("../api/authActions")).sessionActions.loginStart(email),
    [],
  );

  const runSessionMutation = useCallback(
    async (request: () => Promise<AuthMe>): Promise<AuthMe> => {
      if (!activeRef.current) {
        throw new AuthSessionLoadError("unauthorized");
      }
      const generation = generationRef.current + 1;
      generationRef.current = generation;
      inFlightRef.current = null;
      try {
        const next = await mutateAuthoritativeSession(queryClient, request);
        if (!activeRef.current || generation !== generationRef.current) {
          throw new AuthSessionLoadError("unauthorized");
        }
        installSession(next, true);
        return next;
      } catch (error) {
        if (error instanceof TeamSwitchRejectedError) throw error;
        const safeError =
          error instanceof AuthSessionLoadError
            ? error
            : new AuthSessionLoadError("invalid");
        if (activeRef.current && generation === generationRef.current) {
          if (safeError.kind === "unauthorized") {
            setSignedOut(false);
          } else {
            // The server may have committed a cookie/team/CSRF change even if
            // the browser lost or rejected the response. Reconcile via /me.
            enterUnavailable(safeError, true);
          }
        }
        throw safeError;
      }
    },
    [enterUnavailable, installSession, queryClient, setSignedOut],
  );

  const loginComplete = useCallback(
    async (token: string): Promise<void> => {
      await runSessionMutation(async () => (await import("../api/authActions")).sessionActions.loginComplete(token));
    },
    [runSessionMutation],
  );

  const loginPassword = useCallback(
    async (username: string, password: string): Promise<void> => {
      await runSessionMutation(async () => (await import("../api/authActions")).sessionActions.loginPassword(username, password));
    },
    [runSessionMutation],
  );

  const acceptInvite = useCallback(
    async (code: string, email: string): Promise<AuthMe> => {
      return runSessionMutation(async () => (await import("../api/authActions")).sessionActions.acceptInvite({ code, email }));
    },
    [runSessionMutation],
  );

  const switchTeam = useCallback(
    async (teamId: string): Promise<void> => {
      await runSessionMutation(async () => (await import("../api/authActions")).sessionActions.switchTeam(teamId));
    },
    [runSessionMutation],
  );

  const logout = useCallback(async (): Promise<void> => {
    if (!activeRef.current) return;
    const generation = generationRef.current + 1;
    generationRef.current = generation;
    inFlightRef.current = null;
    try {
      await clearAuthoritativeSession(queryClient, async () => (await import("../api/authActions")).sessionActions.logout());
    } finally {
      if (activeRef.current && generation === generationRef.current) {
        setSignedOut(false);
      }
    }
  }, [queryClient, setSignedOut]);

  const value = useMemo<AuthCtx>(
    () => ({
      me,
      teams: me?.teams ?? [],
      currentTeamId: me?.current_team?.id ?? null,
      isAuthenticated: session.status === "authenticated" && me !== null,
      isLoading: session.status === "loading",
      isAdmin: me?.is_platform_admin ?? false,
      sessionStatus: session.status,
      sessionFailure: session.failure,
      acceptInvite,
      loginStart,
      loginComplete,
      loginPassword,
      switchTeam,
      logout,
      refreshMe,
    }),
    [
      acceptInvite,
      loginComplete,
      loginPassword,
      loginStart,
      logout,
      me,
      refreshMe,
      session,
      switchTeam,
    ],
  );

  return (
    <AuthContext.Provider value={value}>
      {cacheReady ? children : null}
    </AuthContext.Provider>
  );
}
