import { Suspense } from "react";
import { lazyRoute } from "../lib/lazyRoute";
import { type InviteRole, type InviteStatus } from "../api";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import { DestructiveActionDialog } from "../components/DestructiveActionDialog";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { Tabs } from "../components/Tabs";
import { downloadInviteLink, formatDate, statusClass } from "./adminAccessState";
import { CliSetupCommands } from "./AdminCliSetup";

import { useAdminAccess } from "./useAdminAccess";

const AdminAuditLog = lazyRoute(() => import("../components/admin/AdminAuditLog"));
const AdminApiTokens = lazyRoute(async () => ({ default: (await import("./AdminApiTokens")).AdminApiTokens }));
const AdminTeams = lazyRoute(async () => ({ default: (await import("./AdminTeams")).AdminTeams }));
const AdminLegacyRegistrations = lazyRoute(async () => ({ default: (await import("./AdminLegacyRegistrations")).AdminLegacyRegistrations }));
export default function AdminAccess(): JSX.Element {
  const state = useAdminAccess();
  const {
    isLoading,
    canManageTeam,
    userRegistrationRequests,
    accountApprovalRoles,
    setAccountApprovalRoles,
    userRegistrationBusy,
    approveUserRegistration,
    openDestructiveAction,
    isAdmin,
    actor,
    setActor,
    latestRevealedInvite,
    revealedInvites,
    revealedAccountLinks,
    revealedToken,
    visibleSections,
    activeSection,
    setSection,
    actorMissing,
    passwordResetRequests,
    passwordResetBusy,
    approvePasswordReset,
    inviteEmail,
    setInviteEmail,
    selectedInviteTeamId,
    adminTeamItems,
    setInviteTeamId,
    me,
    inviteRole,
    setInviteRole,
    inviteMaxUses,
    setInviteMaxUses,
    inviteDomain,
    setInviteDomain,
    inviteCreateDisabled,
    createInvite,
    invites,
    inviteStatus,
    setInviteStatus,
    inviteBusy,
    destructiveAction,
    closeDestructiveAction,
    destructiveDialog,
    confirmDestructiveAction,
  } = state;
  if (isLoading) return <LoadingState />;

  if (!canManageTeam) {
    return (
      <Card>
        <Card.Header title="Team access" description="Team access management requires the owner role." />
        <Card.Body>
          <p className="text-sm text-slate-600">
            Ask a team owner to manage invites, members, and CLI tokens.
          </p>
        </Card.Body>
      </Card>
    );
  }

  const accountRequestsCard = (
    <Card data-loom-query="registration-requests" data-loom-query-status={userRegistrationRequests.status}>
      <Card.Header
        title="Account requests"
        description="Approve a username into its requested team, then share the one-time password setup link manually."
      />
      <Card.Body className="space-y-3">
        {userRegistrationRequests.isPending ? <LoadingState /> : null}
        {userRegistrationRequests.isError ? <ErrorState error={userRegistrationRequests.error} /> : null}
        {userRegistrationRequests.data ? (
          userRegistrationRequests.data.items.length === 0 ? (
            <EmptyState label="No pending account requests." />
          ) : (
            <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Account requests scroll area">
              <table aria-label="Account requests" className="min-w-full divide-y divide-slate-200 text-sm">
                <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                  <tr>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Username
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Team
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Requested
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Role
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-100 bg-white">
                  {userRegistrationRequests.data.items.map((request) => {
                    const role = accountApprovalRoles[request.id] ?? request.role ?? "member";
                    return (
                      <tr key={request.id}>
                        <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-900">
                          {request.username}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-700">
                          {request.team_name ?? request.team_id}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                          {formatDate(request.requested_at)}
                        </td>
                        <td className="px-3 py-2">
                          <select
                            aria-label={`Role for ${request.username}`}
                            className="rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm"
                            value={role}
                            onChange={(event) =>
                              setAccountApprovalRoles((current) => ({
                                ...current,
                                [request.id]: event.target.value as InviteRole,
                              }))
                            }
                          >
                            {(["member", "owner", "viewer"] as InviteRole[]).map((option) => (
                              <option key={option} value={option}>
                                {option}
                              </option>
                            ))}
                          </select>
                        </td>
                        <td className="px-3 py-2">
                          <div className="flex flex-wrap gap-2">
                            <Button
                              size="sm"
                              aria-label={`Approve account ${request.username}`}
                              disabled={userRegistrationBusy(request)}
                              onClick={() => approveUserRegistration.mutate(request)}
                            >
                              Approve
                            </Button>
                            <Button
                              size="sm"
                              variant="secondary"
                              aria-label={`Reject account ${request.username}`}
                              disabled={userRegistrationBusy(request)}
                              onClick={() =>
                                openDestructiveAction({
                                  kind: "reject-user-registration",
                                  request,
                                })
                              }
                            >
                              Reject
                            </Button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )
        ) : null}
        {approveUserRegistration.isError ? <ErrorState error={approveUserRegistration.error} /> : null}
      </Card.Body>
    </Card>
  );

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Team access</h1>
        <p className="mt-1 text-sm text-slate-500">
          {isAdmin
            ? "Approve pending account requests, issue invites, and audit access decisions."
            : "Manage team invites and user-owned API tokens for the current team."}
        </p>
      </header>

      {isAdmin && !["audit", "tokens"].includes(activeSection) ? (
        <Card>
          <Card.Header
            title="Admin actor"
            description="Recorded in audit events for approve, reject, and platform-admin invite actions."
          />
          <Card.Body>
            <label className="block text-sm font-medium text-slate-700" htmlFor="admin-actor">
              Admin actor
            </label>
            <Input
              id="admin-actor"
              className="mt-2 max-w-sm"
              value={actor}
              onChange={(event) => setActor(event.target.value)}
              placeholder="qianyi"
            />
          </Card.Body>
        </Card>
      ) : null}

      {latestRevealedInvite ? (
        <Card className="border-emerald-200">
          <Card.Header
            title={revealedInvites.length === 1 ? "Invite link" : "Invite links"}
            description={
              revealedInvites.length === 1
                ? `Copy this invite link and share it manually with ${latestRevealedInvite.invite.email}. Loom will not send it by email.`
                : "Copy these invite links and share them manually. Loom will not send them by email."
            }
          />
          <Card.Body className="space-y-3">
            {revealedInvites.map((revealed) => (
              <div
                key={revealed.invite.id}
                className="space-y-2 rounded-lg border border-emerald-200 bg-emerald-50 p-3"
              >
                {revealedInvites.length > 1 ? (
                  <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-emerald-900">
                    <span className="font-medium">{revealed.invite.email}</span>
                    <span>{revealed.invite.team_name ?? revealed.invite.team_id}</span>
                  </div>
                ) : null}
                <div className="break-all rounded-lg border border-emerald-200 bg-white px-3 py-2 font-mono text-sm text-emerald-900">
                  {revealed.invite_link}
                </div>
                <div className="flex gap-2">
                  <Button
                    size="sm"
                    aria-label={`Copy invite link for ${revealed.invite.email}`}
                    onClick={() => navigator.clipboard.writeText(revealed.invite_link)}
                  >
                    Copy
                  </Button>
                  <Button
                    size="sm"
                    aria-label={`Download invite link for ${revealed.invite.email}`}
                    onClick={() => downloadInviteLink(revealed.invite_link, revealed.invite.team_name)}
                  >
                    Download
                  </Button>
                </div>
              </div>
            ))}
          </Card.Body>
        </Card>
      ) : null}

      {revealedAccountLinks.length > 0 ? (
        <Card className="border-emerald-200">
          <Card.Header
            title="Account links"
            description="Copy these one-time password links and share them manually. Loom does not send email."
          />
          <Card.Body className="space-y-3">
            {revealedAccountLinks.map((revealed) => (
              <div
                key={revealed.id}
                className="space-y-2 rounded-lg border border-emerald-200 bg-emerald-50 p-3"
              >
                <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-emerald-900">
                  <span className="font-medium">{revealed.username}</span>
                  <span>{revealed.kind === "setup" ? "Password setup" : "Password reset"}</span>
                  {revealed.teamName ? <span>{revealed.teamName}</span> : null}
                  {revealed.tokenPrefix ? <span>Prefix: {revealed.tokenPrefix}</span> : null}
                </div>
                <div className="break-all rounded-lg border border-emerald-200 bg-white px-3 py-2 font-mono text-sm text-emerald-900">
                  {revealed.link}
                </div>
                <Button
                  size="sm"
                  aria-label={`Copy ${revealed.kind} link for ${revealed.username}`}
                  onClick={() => navigator.clipboard.writeText(revealed.link)}
                >
                  Copy
                </Button>
              </div>
            ))}
          </Card.Body>
        </Card>
      ) : null}

      {revealedToken ? (
        <Card className="border-emerald-200">
          <Card.Header
            title="New API token"
            description="Shown once; store it in your password manager or CLI secret store before leaving this page."
          />
          <Card.Body className="space-y-3">
            <div className="rounded-lg border border-emerald-200 bg-emerald-50 px-3 py-2 font-mono text-sm text-emerald-900">
              {revealedToken.token}
            </div>
            <div className="flex flex-wrap gap-2 text-sm text-emerald-900">
              <span>Prefix: {revealedToken.token_hash_prefix}</span>
              <span>Expires: {formatDate(revealedToken.expires_at)}</span>
            </div>
            <Button size="sm" onClick={() => navigator.clipboard.writeText(revealedToken.token)}>
              Copy token
            </Button>
            <CliSetupCommands token={revealedToken.token} />
          </Card.Body>
        </Card>
      ) : null}

      <Tabs
        items={visibleSections}
        value={activeSection}
        onValueChange={setSection}
        ariaLabel="Team access sections"
        className="space-y-6"
        tabListClassName="flex flex-wrap gap-2 rounded-lg border border-slate-200 bg-white p-1"
        tabClassName={({ selected }) =>
          `rounded-md px-3 py-2 text-sm font-medium ${
            selected ? "bg-accent text-white" : "text-slate-600 hover:bg-slate-50 hover:text-slate-900"
          }`
        }
        panelClassName="space-y-6"
        renderPanel={() => (
          <Suspense fallback={<LoadingState label="Loading account section…" />}>
            {isAdmin && activeSection === "teams" ? <AdminTeams {...state} /> : null}

            {isAdmin && activeSection === "accounts" ? (
              <div className="grid gap-4 xl:grid-cols-2">
                <p className="text-sm text-slate-600">Accounts handles password recovery for existing accounts. Review new account and team applications under Requests.</p>

                <Card
                  data-loom-query="password-reset-requests"
                  data-loom-query-status={passwordResetRequests.status}
                >
                  <Card.Header
                    title="Password resets"
                    description="Approve reset requests only after verifying the request out of band, then share the one-time reset link manually."
                  />
                  <Card.Body className="space-y-3">
                    {passwordResetRequests.isPending ? <LoadingState /> : null}
                    {passwordResetRequests.isError ? (
                      <ErrorState error={passwordResetRequests.error} />
                    ) : null}
                    {passwordResetRequests.data ? (
                      passwordResetRequests.data.items.length === 0 ? (
                        <EmptyState label="No pending password resets." />
                      ) : (
                        <div className="overflow-x-auto">
                          <table
                            aria-label="Password reset requests"
                            className="min-w-full divide-y divide-slate-200 text-sm"
                          >
                            <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                              <tr>
                                <th scope="col" className="px-3 py-2 font-semibold">
                                  Username
                                </th>
                                <th scope="col" className="px-3 py-2 font-semibold">
                                  Requested
                                </th>
                                <th scope="col" className="px-3 py-2 font-semibold">
                                  Actions
                                </th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-slate-100 bg-white">
                              {passwordResetRequests.data.items.map((request) => (
                                <tr key={request.id}>
                                  <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-900">
                                    {request.username}
                                  </td>
                                  <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                                    {formatDate(request.requested_at)}
                                  </td>
                                  <td className="px-3 py-2">
                                    <div className="flex flex-wrap gap-2">
                                      <Button
                                        size="sm"
                                        aria-label={`Approve reset ${request.username}`}
                                        disabled={passwordResetBusy(request)}
                                        onClick={() => approvePasswordReset.mutate(request)}
                                      >
                                        Approve
                                      </Button>
                                      <Button
                                        size="sm"
                                        variant="secondary"
                                        aria-label={`Reject reset ${request.username}`}
                                        disabled={passwordResetBusy(request)}
                                        onClick={() =>
                                          openDestructiveAction({
                                            kind: "reject-password-reset",
                                            request,
                                          })
                                        }
                                      >
                                        Reject
                                      </Button>
                                    </div>
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )
                    ) : null}
                    {approvePasswordReset.isError ? <ErrorState error={approvePasswordReset.error} /> : null}
                  </Card.Body>
                </Card>
              </div>
            ) : null}

            {activeSection === "tokens" ? <AdminApiTokens {...state} /> : null}

            {isAdmin && activeSection === "requests" ? (
              <div className="grid gap-4">
                {accountRequestsCard}
                <AdminLegacyRegistrations {...state} />
              </div>
            ) : null}

            {activeSection === "invites" ? (
              <>
                <Card>
                  <Card.Header
                    title="Create invite"
                    description="Create a team invite; the raw link is shown only once."
                  />
                  <Card.Body className="grid gap-3 md:grid-cols-5">
                    <Input
                      aria-label="Invite recipient email"
                      value={inviteEmail}
                      onChange={(event) => setInviteEmail(event.target.value)}
                      placeholder="person@example.com"
                    />
                    {isAdmin ? (
                      <select
                        aria-label="Invite team"
                        className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                        value={selectedInviteTeamId}
                        disabled={adminTeamItems.length === 0}
                        onChange={(event) => setInviteTeamId(event.target.value)}
                      >
                        {adminTeamItems.length === 0 ? (
                          <option value="">No team available</option>
                        ) : (
                          adminTeamItems.map((team) => (
                            <option key={team.id} value={team.id}>
                              {team.name}
                            </option>
                          ))
                        )}
                      </select>
                    ) : (
                      <Input
                        aria-label="Invite team"
                        value={me?.current_team?.name ?? "Current team"}
                        readOnly
                        disabled
                      />
                    )}
                    <select
                      aria-label="Invite role"
                      className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-800"
                      value={inviteRole}
                      onChange={(event) => setInviteRole(event.target.value as InviteRole)}
                    >
                      <option value="viewer">viewer</option>
                      <option value="member">member</option>
                      <option value="owner">owner</option>
                    </select>
                    <Input
                      aria-label="Invite max uses"
                      type="number"
                      min={1}
                      value={inviteMaxUses}
                      onChange={(event) => setInviteMaxUses(event.target.value)}
                      placeholder="max uses"
                    />
                    <Input
                      aria-label="Allowed domain"
                      value={inviteDomain}
                      onChange={(event) => setInviteDomain(event.target.value)}
                      placeholder="allowed domain"
                    />
                    <div className="md:col-span-5">
                      <Button
                        variant="primary"
                        disabled={inviteCreateDisabled}
                        onClick={() => createInvite.mutate()}
                      >
                        Create invite
                      </Button>
                    </div>
                    {createInvite.isError ? <ErrorState error={createInvite.error} /> : null}
                  </Card.Body>
                </Card>

                <Card data-loom-query="invites" data-loom-query-status={invites.status}>
                  <Card.Header
                    title={`${inviteStatus[0].toUpperCase()}${inviteStatus.slice(1)} invites`}
                    description="Invite links are listed by status without exposing raw codes."
                    actions={
                      <select
                        aria-label="Invite status"
                        className="rounded-lg border border-slate-200 bg-white px-2 py-1 text-xs text-slate-700"
                        value={inviteStatus}
                        onChange={(event) => setInviteStatus(event.target.value as InviteStatus)}
                      >
                        <option value="pending">pending</option>
                        <option value="accepted">accepted</option>
                        <option value="expired">expired</option>
                        <option value="revoked">revoked</option>
                      </select>
                    }
                  />
                  <Card.Body>
                    {invites.isPending ? <LoadingState /> : null}
                    {invites.isError ? <ErrorState error={invites.error} /> : null}
                    {invites.data ? (
                      invites.data.items.length === 0 ? (
                        <EmptyState label={`No ${inviteStatus} invites.`} />
                      ) : (
                        <div className="overflow-x-auto">
                          <table
                            aria-label="Invitations"
                            className="min-w-full divide-y divide-slate-200 text-sm"
                          >
                            <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                              <tr>
                                <th scope="col" className="px-3 py-2 font-semibold">
                                  Team
                                </th>
                                <th scope="col" className="px-3 py-2 font-semibold">
                                  Email
                                </th>
                                <th scope="col" className="px-3 py-2 font-semibold">
                                  Role
                                </th>
                                <th scope="col" className="px-3 py-2 font-semibold">
                                  Prefix
                                </th>
                                <th scope="col" className="px-3 py-2 font-semibold">
                                  Status
                                </th>
                                <th scope="col" className="px-3 py-2 font-semibold">
                                  Actions
                                </th>
                              </tr>
                            </thead>
                            <tbody className="divide-y divide-slate-100 bg-white">
                              {invites.data.items.map((invite) => (
                                <tr key={invite.id}>
                                  <td className="whitespace-nowrap px-3 py-2 text-slate-700">
                                    {invite.team_name ?? invite.team_id}
                                  </td>
                                  <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                                    {invite.email}
                                  </td>
                                  <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                                    {invite.role}
                                  </td>
                                  <td className="whitespace-nowrap px-3 py-2 font-mono text-xs text-slate-600">
                                    {invite.code_prefix}
                                  </td>
                                  <td
                                    className={`whitespace-nowrap px-3 py-2 font-medium ${statusClass(invite.status)}`}
                                  >
                                    {invite.status}
                                  </td>
                                  <td className="px-3 py-2">
                                    <div className="flex gap-2">
                                      {invite.status === "pending" ? (
                                        <Button
                                          size="sm"
                                          variant="danger"
                                          disabled={actorMissing || inviteBusy(invite)}
                                          onClick={() =>
                                            openDestructiveAction({
                                              kind: "revoke-invite",
                                              invite,
                                            })
                                          }
                                        >
                                          Revoke
                                        </Button>
                                      ) : null}
                                      {invite.status === "pending" || invite.status === "expired" ? (
                                        <Button
                                          size="sm"
                                          disabled={actorMissing || inviteBusy(invite)}
                                          onClick={() =>
                                            openDestructiveAction({
                                              kind: "resend-invite",
                                              invite,
                                            })
                                          }
                                        >
                                          Resend
                                        </Button>
                                      ) : null}
                                    </div>
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
              </>
            ) : null}

            {isAdmin && activeSection === "audit" ? <AdminAuditLog /> : null}
          </Suspense>
        )}
      />
      <DestructiveActionDialog
        open={destructiveAction !== null}
        onClose={closeDestructiveAction}
        title={destructiveDialog.title}
        target={destructiveDialog.target}
        consequence={destructiveDialog.consequence}
        confirmLabel={destructiveDialog.confirmLabel}
        pendingLabel={destructiveDialog.pendingLabel}
        confirmation={{ type: "simple" }}
        pending={destructiveDialog.pending}
        error={destructiveDialog.error}
        onConfirm={confirmDestructiveAction}
      />
    </div>
  );
}
