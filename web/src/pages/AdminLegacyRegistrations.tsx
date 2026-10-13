import { type InviteRole } from "../api";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { formatDate } from "./adminAccessState";

import type { AdminAccessViewState } from "./useAdminAccess";
export function AdminLegacyRegistrations({
  registrations,
  approvalTeamIds,
  adminTeams,
  approvalRoles,
  setApprovalTeamIds,
  setApprovalRoles,
  actorMissing,
  teamRegistrationBusy,
  approve,
  rejectedReason,
  setRejectedReason,
  openDestructiveAction,
}: AdminAccessViewState): JSX.Element {
  return (
    <Card data-loom-query="team-registrations" data-loom-query-status={registrations.status}>
      <Card.Header
        title="Legacy team registrations"
        description="Approve older team-registration requests into an invite link. Username/password account approvals are listed above."
      />
      <Card.Body>
        {registrations.isPending ? <LoadingState /> : null}
        {registrations.isError ? <ErrorState error={registrations.error} /> : null}
        {registrations.data ? (
          registrations.data.items.length === 0 ? (
            <EmptyState label="No pending legacy team registrations." />
          ) : (
            <div className="overflow-x-auto">
              <table
                aria-label="Legacy team registrations"
                className="min-w-full stack-divide-y stack-divide-slate-200 text-sm"
              >
                <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                  <tr>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Team
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Contact
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Requested
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Assign to
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Role
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="stack-divide-y stack-divide-slate-100 bg-white">
                  {registrations.data.items.map((item) => {
                    const selectedTeamId = approvalTeamIds[item.id] ?? adminTeams.data?.items[0]?.id ?? "";
                    const selectedRole = approvalRoles[item.id] ?? "member";
                    return (
                      <tr key={item.id}>
                        <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-900">
                          {item.name}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-600">{item.contact_email}</td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                          {formatDate(item.requested_at)}
                        </td>
                        <td className="min-w-48 px-3 py-2">
                          <select
                            aria-label={`Approval team for ${item.name}`}
                            className="block w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm text-slate-800"
                            value={selectedTeamId}
                            onChange={(event) =>
                              setApprovalTeamIds((current) => ({
                                ...current,
                                [item.id]: event.target.value,
                              }))
                            }
                          >
                            {adminTeams.data?.items.map((team) => (
                              <option key={team.id} value={team.id}>
                                {team.name}
                              </option>
                            ))}
                          </select>
                        </td>
                        <td className="px-3 py-2">
                          <select
                            aria-label={`Approval role for ${item.name}`}
                            className="block w-full rounded-lg border border-slate-200 bg-white px-2 py-1 text-sm text-slate-800"
                            value={selectedRole}
                            onChange={(event) =>
                              setApprovalRoles((current) => ({
                                ...current,
                                [item.id]: event.target.value as InviteRole,
                              }))
                            }
                          >
                            <option value="member">member</option>
                            <option value="viewer">viewer</option>
                            <option value="owner">owner</option>
                          </select>
                        </td>
                        <td className="px-3 py-2">
                          <div className="flex flex-wrap items-center gap-2">
                            <Button
                              size="sm"
                              variant="primary"
                              disabled={actorMissing || !selectedTeamId || teamRegistrationBusy(item)}
                              onClick={() =>
                                approve.mutate({
                                  id: item.id,
                                  teamId: selectedTeamId,
                                  role: selectedRole,
                                })
                              }
                            >
                              Approve
                            </Button>
                            <Input
                              aria-label={`Reject reason for ${item.name}`}
                              className="w-48"
                              value={rejectedReason[item.id] ?? ""}
                              onChange={(event) =>
                                setRejectedReason((current) => ({
                                  ...current,
                                  [item.id]: event.target.value,
                                }))
                              }
                              placeholder="reason"
                            />
                            <Button
                              size="sm"
                              variant="danger"
                              disabled={actorMissing || teamRegistrationBusy(item)}
                              onClick={() =>
                                openDestructiveAction({
                                  kind: "reject-team-registration",
                                  request: item,
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
        {approve.isError ? <ErrorState error={approve.error} /> : null}
      </Card.Body>
    </Card>
  );
}
