import { queryKeys } from "../api/queryKeys";
/** Account onboarding and team settings. */

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, Navigate } from "react-router-dom";

import { api, type ApiTokenEntry } from "../api";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { DestructiveActionDialog } from "../components/DestructiveActionDialog";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";
import { StatusPill } from "../components/StatusPill";
import { cn } from "../lib/cn";
import { formatLocalDateTime } from "../lib/dateTime";

type TeamDetail = Awaited<ReturnType<typeof api.getTeam>>;
type TeamUserMember = NonNullable<TeamDetail["user_members"]>[number];

const LINK_BUTTON =
  "inline-flex items-center justify-center rounded-lg border px-3.5 py-2 text-sm font-medium";

const ROLE_DESCRIPTIONS: Record<string, string> = {
  owner: "Can manage team access, API tokens, provider credentials, and submissions.",
  member: "Can submit and monitor this team's evaluation work.",
  viewer: "Can read this team's completed and in-progress work.",
  platform_admin: "Can administer platform access and shared configuration.",
};

function hasScope(scopes: string[] | undefined, scope: string): boolean {
  return Boolean(scopes?.includes(scope));
}

function formatDate(value: string | null | undefined): string {
  return formatLocalDateTime(value, { fallback: "-" });
}

function roleLabel(role: string | null | undefined): string {
  if (!role) return "No team role";
  return role.replaceAll("_", " ");
}

function tokenStatus(token: ApiTokenEntry): JSX.Element {
  if (token.revoked_at) {
    return <StatusPill variant="cancelled">Revoked</StatusPill>;
  }
  if (token.expires_at && Date.parse(token.expires_at) <= Date.now()) {
    return <StatusPill variant="neutral">Expired</StatusPill>;
  }
  return <StatusPill variant="success">Active</StatusPill>;
}

function TeamMembers({
  members,
}: {
  members: TeamUserMember[] | undefined;
}): JSX.Element {
  if (!members || members.length === 0) {
    return <EmptyState label="No browser users have joined this team yet." />;
  }
  return (
    <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Team members scroll area">
      <table aria-label="Team members" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
        <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th scope="col" className="px-4 py-3 font-semibold">Member</th>
            <th scope="col" className="px-4 py-3 font-semibold">Role</th>
            <th scope="col" className="px-4 py-3 font-semibold">Joined</th>
          </tr>
        </thead>
        <tbody className="stack-divide-y stack-divide-slate-100 bg-white">
          {members.map((member) => (
            <tr key={member.user_id}>
              <td className="px-4 py-3">
                <div className="font-medium text-slate-900">
                  {member.display_name ?? member.username ?? member.email}
                </div>
                {member.email ? <div className="text-xs text-slate-500">{member.email}</div> : null}
              </td>
              <td className="px-4 py-3 text-slate-700">{roleLabel(member.role)}</td>
              <td className="px-4 py-3 text-slate-600">
                {formatDate(member.joined_at)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function Settings(): JSX.Element {
  const {
    me,
    isAuthenticated,
    isAdmin,
    currentTeamId,
    teams,
    switchTeam,
    logout,
  } = useAuth();
  const [tokenToRevoke, setTokenToRevoke] = useState<ApiTokenEntry | null>(null);

  const queryClient = useQueryClient();
  const currentTeam = teams.find((team) => team.id === currentTeamId) ?? null;
  const scopes = me?.scopes ?? [];
  const canSubmit = isAdmin || hasScope(scopes, "submit");
  const canManageProviders = isAdmin || hasScope(scopes, "providers:manage");
  const canManageTokens = isAdmin || hasScope(scopes, "tokens:manage");
  const canManageTeam = isAdmin || currentTeam?.role === "owner";

  const tokens = useQuery({
    queryKey: queryKeys["tokens"](),
    queryFn: () => api.listTokens(),
    enabled: isAuthenticated && canManageTokens,
    retry: false,
  });
  const teamDetail = useQuery({
    queryKey: queryKeys["team"](currentTeamId),
    queryFn: () => api.getTeam(currentTeamId ?? ""),
    enabled: isAuthenticated && currentTeamId !== null,
    retry: false,
  });

  const switchTeamMutation = useMutation({
    mutationFn: (teamId: string) => switchTeam(teamId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys["team"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["tokens"]() });
    },
  });

  const revoke = useMutation({
    mutationFn: (prefix: string) => api.revokeToken(prefix),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys["tokens"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["api-tokens"]() });
    },
  });

  const confirmTokenRevoke = async (): Promise<void> => {
    if (!tokenToRevoke) return;
    await revoke.mutateAsync(tokenToRevoke.token_hash_prefix);
    setTokenToRevoke(null);
  };

  // Legacy signed-out /settings → canonical /auth/login (#775).
  if (!isAuthenticated) {
    return <Navigate to="/auth/login" replace />;
  }

  return (
    <div className="stack-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Team Settings</h1>
        <p className="mt-1 text-sm text-slate-500">
          Manage your browser session, current team, members, provider setup,
          and CLI credentials.
        </p>
      </header>

      <Card>
        <Card.Header
          title="Current team"
          description={
            currentTeam?.role
              ? ROLE_DESCRIPTIONS[currentTeam.role] ?? "This role controls which team actions are available."
              : "Select a team to scope runs, credentials, and artifacts."
          }
          actions={
            <Button
              variant="secondary"
              onClick={() => void logout()}
              title="End this browser session."
            >
              Sign out
            </Button>
          }
        />
        <Card.Body className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_16rem]">
          <div className="stack-y-3">
            <div className="flex flex-wrap items-center gap-3">
              <StatusPill variant={isAdmin ? "info" : "success"}>
                {isAdmin ? "Platform admin" : roleLabel(currentTeam?.role)}
              </StatusPill>
              <span className="text-sm text-slate-700">{me?.user.username}</span>
              {currentTeam ? (
                <span className="text-sm font-medium text-slate-900">
                  {currentTeam.name}
                </span>
              ) : null}
            </div>
            <div className="flex flex-wrap gap-2">
              {canSubmit ? <StatusPill variant="success">Can submit</StatusPill> : null}
              {canManageProviders ? (
                <StatusPill variant="info">Can manage providers</StatusPill>
              ) : null}
              {canManageTokens ? (
                <StatusPill variant="info">Can manage CLI tokens</StatusPill>
              ) : null}
              {!canSubmit && !canManageProviders && !canManageTokens ? (
                <StatusPill variant="neutral">Read only</StatusPill>
              ) : null}
            </div>
          </div>

          <div>
            <label
              htmlFor="current-team"
              className="block text-sm font-medium text-slate-700"
            >
              Current team
            </label>
            <select
              id="current-team"
              aria-label="Current team"
              className="mt-2 block w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
              value={currentTeamId ?? ""}
              disabled={teams.length <= 1 || switchTeamMutation.isPending}
              onChange={(event) => switchTeamMutation.mutate(event.target.value)}
            >
              {teams.map((team) => (
                <option key={team.id} value={team.id}>
                  {team.name} ({roleLabel(team.role)})
                </option>
              ))}
            </select>
            {switchTeamMutation.isError ? (
              <p role="alert" className="mt-2 text-sm text-red-700">
                {switchTeamMutation.error.message}
              </p>
            ) : null}
          </div>
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Team members"
          description="Browser users who accepted an invite for the current team."
        />
        <Card.Body className="p-0">
          {teamDetail.isPending ? <LoadingState /> : null}
          {teamDetail.isError ? (
            <div className="p-5">
              <ErrorState error={teamDetail.error} />
            </div>
          ) : null}
          {teamDetail.data ? (
            <TeamMembers members={teamDetail.data.user_members ?? []} />
          ) : null}
        </Card.Body>
      </Card>

      <Card>
        <Card.Header
          title="Setup actions"
          description="Role-aware shortcuts for common team setup tasks."
        />
        <Card.Body className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          {canSubmit ? (
            <Link
              to="/batches/new"
              className={cn(LINK_BUTTON, "border-accent bg-accent text-white hover:bg-accent-hover")}
            >
              New batch
            </Link>
          ) : null}
          <Link
            to="/providers"
            className={cn(
              LINK_BUTTON,
              canManageProviders
                ? "border-slate-200 bg-white text-slate-700 hover:bg-slate-50"
                : "pointer-events-none border-slate-200 bg-slate-50 text-slate-600",
            )}
            aria-disabled={!canManageProviders}
            title={
              canManageProviders
                ? "Create and test provider connections."
                : "Provider connection management requires an owner role."
            }
          >
            Provider connections
          </Link>
          {canManageTeam ? (
            <Link
              to="/admin/access"
              className={cn(
                LINK_BUTTON,
                "border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
              )}
            >
              Team access
            </Link>
          ) : null}
          <Link
            to="/usage"
            className={cn(
              LINK_BUTTON,
              "border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
            )}
          >
            Usage
          </Link>
        </Card.Body>
      </Card>

      {canManageTokens ? (
        <Card>
          <Card.Header
            title="API tokens"
            description="Prefixes identify credentials without exposing secrets. Create new CLI tokens from Team access."
            actions={
              <Link
                to="/admin/access?tab=tokens"
                className={cn(
                  LINK_BUTTON,
                  "border-slate-200 bg-white text-slate-700 hover:bg-slate-50",
                )}
              >
                Create CLI token
              </Link>
            }
          />
          <Card.Body className="p-0">
            {tokens.isPending ? <LoadingState /> : null}
            {tokens.isError ? (
              <div className="p-5">
                <ErrorState error={tokens.error} />
              </div>
            ) : null}
            {tokens.data ? (
              tokens.data.items.length === 0 ? (
                <EmptyState label="No API tokens." />
              ) : (
                <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="API tokens scroll area">
                  <table aria-label="API tokens" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
                    <thead>
                      <tr className="bg-slate-50/50">
                        <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Name</th>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Prefix</th>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Scopes</th>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Issued</th>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Expires</th>
                        <th scope="col" className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">Status</th>
                        <th scope="col" className="px-4 py-3" />
                      </tr>
                    </thead>
                    <tbody className="stack-divide-y stack-divide-slate-100">
                      {tokens.data.items.map((token) => (
                        <tr key={token.token_hash_prefix} className="hover:bg-slate-50">
                          <td className="px-4 py-3 font-medium text-slate-900">
                            {token.name?.trim() || token.token_hash_prefix}
                          </td>
                          <td className="px-4 py-3 font-mono text-xs text-slate-700">{token.token_hash_prefix}</td>
                          <td className="px-4 py-3 text-slate-600">{token.scopes.join(", ")}</td>
                          <td className="px-4 py-3 text-xs text-slate-500">{formatDate(token.issued_at)}</td>
                          <td className="px-4 py-3 text-xs text-slate-500">{formatDate(token.expires_at)}</td>
                          <td className="px-4 py-3">{tokenStatus(token)}</td>
                          <td className="px-4 py-3 text-right">
                            {!token.revoked_at && !isAdmin ? (
                              <Button
                                size="sm"
                                variant="danger"
                                onClick={() => {
                                  revoke.reset();
                                  setTokenToRevoke(token);
                                }}
                                disabled={
                                  revoke.isPending &&
                                  revoke.variables === token.token_hash_prefix
                                }
                                title="Revoke this token so it can no longer call the API."
                              >
                                Revoke
                              </Button>
                            ) : null}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )
            ) : null}
          </Card.Body>
        </Card>
      ) : null}
      <DestructiveActionDialog
        open={tokenToRevoke !== null}
        onClose={() => {
          revoke.reset();
          setTokenToRevoke(null);
        }}
        title="Revoke API token"
        target={
          tokenToRevoke
            ? `${tokenToRevoke.name?.trim() || tokenToRevoke.token_hash_prefix} (${tokenToRevoke.token_hash_prefix})`
            : ""
        }
        consequence="This token will no longer be able to authenticate to the Loom API."
        confirmLabel="Revoke token"
        pendingLabel="Revoking…"
        confirmation={{ type: "simple" }}
        pending={
          revoke.isPending &&
          revoke.variables === tokenToRevoke?.token_hash_prefix
        }
        error={revoke.error}
        onConfirm={confirmTokenRevoke}
      />
    </div>
  );
}
