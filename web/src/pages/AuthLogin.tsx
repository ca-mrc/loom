import { HelpButton } from "../components/HelpButton";
import { queryKeys } from "../api/queryKeys";
/** Signed-out onboarding: sign-in, account request, password-reset request (#775). */

import { useMutation, useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { Navigate, useLocation } from "react-router-dom";

import { api } from "../api";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import { getFrontendConfig } from "../lib/frontendConfig";

const SIGN_IN_FAILURE = new Error(
  "Sign-in failed. Check your username and password, then try again.",
);

const PUBLIC_TEAMS_FAILURE = new Error(
  "Could not load registration teams. Please try again.",
);

const FIELD_LABEL = "block text-sm font-medium text-slate-700";

export default function AuthLogin(): JSX.Element {
  const { environmentLabel } = getFrontendConfig();
  const location = useLocation();
  // Carry only an invite code in router state; it never becomes a login URL
  // parameter or an arbitrary return URL. Rebuild the owned route explicitly.
  const inviteCode: unknown = location.state?.inviteCode;
  const inviteReturn = typeof inviteCode === "string" && inviteCode.length >= 16 && inviteCode.length <= 256
    ? `/invites/accept?${new URLSearchParams({ code: inviteCode })}` : null;
  const { isAuthenticated, loginPassword } = useAuth();
  const [loginUsername, setLoginUsername] = useState("");
  const [loginPasswordValue, setLoginPasswordValue] = useState("");
  const [registerUsername, setRegisterUsername] = useState("");
  const [registerTeamId, setRegisterTeamId] = useState("");
  const [resetUsername, setResetUsername] = useState("");

  const publicTeams = useQuery({
    queryKey: queryKeys["public-teams"](),
    queryFn: () => api.publicTeams(),
    enabled: !isAuthenticated,
  });

  // Clear a stale Team selection when policy refetch removes it — never
  // silently replace with the first returned Team (#775).
  useEffect(() => {
    if (!registerTeamId) return;
    const ids = new Set((publicTeams.data?.items ?? []).map((team) => team.id));
    if (!ids.has(registerTeamId)) {
      setRegisterTeamId("");
    }
  }, [publicTeams.data, registerTeamId]);

  const signIn = useMutation({
    mutationFn: () => loginPassword(loginUsername.trim(), loginPasswordValue),
  });
  const requestAccess = useMutation({
    mutationFn: () =>
      api.requestRegistration({
        username: registerUsername.trim(),
        team_id: registerTeamId,
      }),
  });
  const requestReset = useMutation({
    mutationFn: () => api.requestPasswordReset(resetUsername.trim()),
  });

  if (isAuthenticated) {
    return <Navigate to={inviteReturn ?? "/settings"} replace />;
  }

  const publicTeamItems = publicTeams.data?.items ?? [];
  const hasPublicTeams = publicTeamItems.length > 0;

  return (
    <div className="stack-y-8">
      <header className="max-w-3xl">
        <p className="text-xs font-semibold uppercase tracking-wider text-slate-600">
          Loom {environmentLabel}
        </p>
        <h1 className="mt-2 text-3xl font-bold text-slate-950">
          Sign in to run and review evaluations
        </h1>
        <p className="mt-3 text-base text-slate-600">
          Account requests are reviewed by an admin. Request a
          username for a publicly available team, set your password from the
          approved setup link, then use the same account in the browser and CLI.
        </p>
      </header>

      <div className="grid items-start gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(22rem,26rem)]">
        <Card>
          <Card.Body className="stack-y-5 p-6 sm:p-7">
            <div>
              <h2 className="text-xl font-semibold text-slate-900">Sign in</h2>
              {inviteReturn ? <p className="mt-2 text-sm text-slate-600">Sign in with the account that received the invite. You will return to it after signing in.</p> : null}
              <p className="mt-2 text-sm text-slate-500">
                Use the username and password you set from an admin-approved
                account link.
              </p>
            </div>
            <form
              className="grid gap-3"
              onSubmit={(event) => {
                event.preventDefault();
                if (signIn.isPending) return;
                signIn.mutate();
              }}
            >
              <div className="grid gap-1.5">
                <label className={FIELD_LABEL} htmlFor="login-username">
                  Username
                </label>
                <Input
                  id="login-username"
                  name="username"
                  autoComplete="username"
                  value={loginUsername}
                  onChange={(e) => setLoginUsername(e.target.value)}
                />
              </div>
              <div className="grid gap-1.5">
                <label className={FIELD_LABEL} htmlFor="login-password">
                  Password
                </label>
                <Input
                  id="login-password"
                  name="password"
                  type="password"
                  autoComplete="current-password"
                  value={loginPasswordValue}
                  onChange={(e) => setLoginPasswordValue(e.target.value)}
                />
              </div>
              <Button
                type="submit"
                variant="primary"
                className="w-full"
                disabled={
                  !loginUsername.trim() ||
                  !loginPasswordValue ||
                  signIn.isPending
                }
              >
                {signIn.isPending ? "Signing in…" : "Sign in"}
              </Button>
            </form>
            {signIn.isError ? (
              <div role="alert">
                <ErrorState announce={false} error={SIGN_IN_FAILURE} />
              </div>
            ) : null}
          </Card.Body>
        </Card>

        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-1">
          <div className="stack-y-2 text-sm text-slate-600 md:col-span-2 lg:col-span-1">
            <p>New to Loom? Request a team account, set your password after approval, then choose an available provider to run your first task.</p>
            <HelpButton topic="access">Account setup guide</HelpButton>
          </div>

          <Card>
            <Card.Header
              title="Request account"
              description="Choose a team that allows public registration. Ask an admin if your team is missing."
            />
            <Card.Body className="stack-y-3">
              {publicTeams.isFetching ? (
                <p role="status" className="text-sm text-slate-600">
                  Loading registration teams…
                </p>
              ) : null}
              {publicTeams.isError ? (
                <div className="stack-y-2">
                  <ErrorState error={PUBLIC_TEAMS_FAILURE} />
                  <Button
                    type="button"
                    variant="secondary"
                    disabled={publicTeams.isFetching}
                    onClick={() => void publicTeams.refetch()}
                  >
                    Retry loading teams
                  </Button>
                </div>
              ) : publicTeams.isSuccess && !hasPublicTeams ? (
                <EmptyState label="No teams are currently open for public registration." />
              ) : null}
              <form
                className="stack-y-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  if (requestAccess.isPending || !registerTeamId) return;
                  requestAccess.mutate();
                }}
              >
                <div className="grid gap-1.5">
                  <label className={FIELD_LABEL} htmlFor="register-username">
                    Requested username
                  </label>
                  <Input
                    id="register-username"
                    name="username"
                    autoComplete="username"
                    value={registerUsername}
                    onChange={(event) => setRegisterUsername(event.target.value)}
                  />
                </div>
                <div className="grid gap-1.5">
                  <label className={FIELD_LABEL} htmlFor="register-team">
                    Registration team
                  </label>
                  <select
                    id="register-team"
                    name="team_id"
                    className="block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
                    value={registerTeamId}
                    onChange={(event) => setRegisterTeamId(event.target.value)}
                    disabled={!hasPublicTeams}
                  >
                    <option value="">Select a team…</option>
                    {publicTeamItems.map((team) => (
                      <option key={team.id} value={team.id}>
                        {team.name}
                      </option>
                    ))}
                  </select>
                </div>
                <Button
                  type="submit"
                  variant="secondary"
                  className="w-full"
                  disabled={
                    !registerUsername.trim() ||
                    !registerTeamId ||
                    requestAccess.isPending
                  }
                >
                  {requestAccess.isPending ? "Submitting…" : "Request account"}
                </Button>
              </form>
              {requestAccess.isSuccess ? (
                <p className="text-sm text-emerald-700">
                  Request submitted. An admin will review it and share a
                  password setup link manually if approved.
                </p>
              ) : null}
              {requestAccess.isError ? (
                <ErrorState announce={false} error={requestAccess.error} />
              ) : null}
            </Card.Body>
          </Card>

          <Card>
            <Card.Header
              title="Forgot password"
              description="Submit a reset request for admin approval."
            />
            <Card.Body className="stack-y-3">
              <form
                className="stack-y-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  if (requestReset.isPending) return;
                  requestReset.mutate();
                }}
              >
                <div className="grid gap-1.5">
                  <label className={FIELD_LABEL} htmlFor="reset-username">
                    Reset username
                  </label>
                  <Input
                    id="reset-username"
                    name="username"
                    autoComplete="username"
                    value={resetUsername}
                    onChange={(event) => setResetUsername(event.target.value)}
                  />
                </div>
                <Button
                  type="submit"
                  variant="secondary"
                  className="w-full"
                  disabled={!resetUsername.trim() || requestReset.isPending}
                >
                  {requestReset.isPending ? "Submitting…" : "Request reset"}
                </Button>
              </form>
              {requestReset.isSuccess ? (
                <p className="text-sm text-emerald-700">
                  Reset request submitted. An admin will share a reset link if approved.
                </p>
              ) : null}
              {requestReset.isError ? (
                <ErrorState announce={false} error={requestReset.error} />
              ) : null}
            </Card.Body>
          </Card>

          <Card className="md:col-span-2 lg:col-span-1">
            <Card.Header
              title="CLI setup"
              description="Use the same approved username and password with the CLI."
            />
            <Card.Body>
              <HelpButton topic="access">Connect through CLI or API</HelpButton>
            </Card.Body>
          </Card>
        </div>
      </div>
    </div>
  );
}
