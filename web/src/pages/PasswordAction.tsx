import { queryKeys } from "../api/queryKeys";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";
import { Link, useLocation } from "react-router-dom";

import { api } from "../api";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";

type Mode = "setup" | "reset";

function useToken(): string {
  const location = useLocation();
  return useMemo(
    () => new URLSearchParams(location.search).get("token") ?? "",
    [location.search],
  );
}

export default function PasswordAction({ mode }: { mode: Mode }): JSX.Element {
  const token = useToken();
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const isSetup = mode === "setup";
  const lookup = useQuery({
    queryKey: queryKeys["password-action"](mode, token),
    queryFn: () => (isSetup ? api.setupLookup(token) : api.resetLookup(token)),
    enabled: token.length > 0,
    retry: false,
  });
  const complete = useMutation({
    mutationFn: () => (
      isSetup
        ? api.setupComplete({
          token,
          password,
          confirm_password: confirmPassword,
        })
        : api.resetComplete({
          token,
          password,
          confirm_password: confirmPassword,
        })
    ),
  });
  const resetCompletion = complete.reset;
  useEffect(() => {
    setPassword("");
    setConfirmPassword("");
    resetCompletion();
  }, [mode, token, resetCompletion]);
  const disabled =
    token.length === 0 ||
    password.length === 0 ||
    confirmPassword.length === 0 ||
    complete.isPending;
  const lookupData = lookup.data;
  const username = lookupData?.username ?? "";
  const teamName =
    lookupData &&
    "team" in lookupData &&
    typeof lookupData.team === "object" &&
    lookupData.team !== null &&
    "name" in lookupData.team &&
    typeof lookupData.team.name === "string"
      ? lookupData.team.name
      : null;

  const linkError = lookup.error ?? (complete.isError ? complete.error : null);
  const rejectedLink = Boolean(linkError && typeof linkError === "object" && "status" in linkError
    && [400, 404, 410, 422].includes(Number(linkError.status))
    && "detail" in linkError && /token|expired|consumed|used/i.test(String(linkError.detail)));
  const recovery = !token || rejectedLink;

  return (
    <div className="mx-auto max-w-lg stack-y-5">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">
          {isSetup ? "Set Password" : "Reset Password"}
        </h1>
        <p className="mt-1 text-sm text-slate-500">
          {username ? `Account: ${username}${teamName ? ` / ${teamName}` : ""}` : "Account link"}
        </p>
      </header>
      <Card>
        <Card.Body className="stack-y-4">
          {recovery ? <div role="alert" className="stack-y-3 text-sm text-slate-700">
            <h2 className="font-semibold text-slate-900">{!token ? "Your account link is missing" : "This account link is no longer available"}</h2>
            <p>{!token ? "Open the complete link provided by your team administrator."
              : "The link is invalid, expired, or already used. For privacy, Loom cannot distinguish these states here."}</p>
            <p>{isSetup ? "Ask your team administrator for a new account setup link. If you already set your password, sign in."
              : "Go to sign in and choose Reset password to request a new link from your team administrator."}</p>
            <Link className="inline-block rounded text-accent underline" to="/auth/login">Go to sign in</Link>
          </div> : null}
          {lookup.isPending && token ? <LoadingState /> : null}
          {lookup.isError && !rejectedLink ? <ErrorState error={lookup.error} /> : null}
          {lookup.data && !complete.isSuccess && !recovery ? (
            <form
              className="stack-y-4"
              onSubmit={(event) => {
                event.preventDefault();
                if (complete.isPending || disabled) return;
                complete.mutate();
              }}
            >
              <div className="grid gap-1.5">
                <label
                  className="block text-sm font-medium text-slate-700"
                  htmlFor="password-action-password"
                >
                  Password
                </label>
                <Input
                  id="password-action-password"
                  name="password"
                  type="password"
                  autoComplete="new-password"
                  value={password}
                  onChange={(event) => setPassword(event.target.value)}
                />
              </div>
              <div className="grid gap-1.5">
                <label
                  className="block text-sm font-medium text-slate-700"
                  htmlFor="password-action-confirm"
                >
                  Confirm password
                </label>
                <Input
                  id="password-action-confirm"
                  name="confirm_password"
                  type="password"
                  autoComplete="new-password"
                  value={confirmPassword}
                  onChange={(event) => setConfirmPassword(event.target.value)}
                />
              </div>
              <Button
                type="submit"
                variant="primary"
                className="w-full"
                disabled={disabled}
              >
                {complete.isPending
                  ? (isSetup ? "Setting…" : "Resetting…")
                  : (isSetup ? "Set password" : "Reset password")}
              </Button>
            </form>
          ) : null}
          {complete.isError && !rejectedLink ? <ErrorState error={complete.error} /> : null}
          {complete.isSuccess ? (
            <div className="stack-y-3">
              <p className="text-sm font-medium text-emerald-700">
                Password updated for {complete.data.user.username}.
              </p>
              <Link className="text-sm font-medium text-accent hover:text-accent-hover" to="/auth/login">
                Sign in
              </Link>
            </div>
          ) : null}
        </Card.Body>
      </Card>
    </div>
  );
}
