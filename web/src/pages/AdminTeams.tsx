import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";

import type { AdminAccessViewState } from "./useAdminAccess";
export function AdminTeams({
  adminTeams,
  newTeamName,
  setNewTeamName,
  actorMissing,
  createTeam,
  teamNameEdits,
  publicRegistrationBusy,
  setTeamNameEdits,
  updateTeam,
  openDestructiveAction,
  enablePublicRegistration,
  disablePublicRegistration,
}: AdminAccessViewState): JSX.Element {
  return (
    <Card data-loom-query="admin-teams" data-loom-query-status={adminTeams.status}>
      <Card.Header
        title="Internal teams"
        description="Maintain fixed teams and which ones accept public account requests on /auth/login."
      />
      <Card.Body className="stack-y-5">
        <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_9rem]">
          <Input
            aria-label="New team name"
            value={newTeamName}
            onChange={(event) => setNewTeamName(event.target.value)}
            placeholder="Team name"
          />
          <Button
            variant="primary"
            disabled={actorMissing || !newTeamName.trim() || createTeam.isPending}
            onClick={() => createTeam.mutate()}
          >
            Create team
          </Button>
        </div>
        {adminTeams.isPending ? <LoadingState /> : null}
        {adminTeams.isError ? <ErrorState error={adminTeams.error} /> : null}
        {adminTeams.data ? (
          adminTeams.data.items.length === 0 ? (
            <EmptyState label="No teams have been created yet." />
          ) : (
            <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="Internal teams scroll area">
              <table aria-label="Internal teams" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
                <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                  <tr>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Team
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Members
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Status
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Public registration
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="stack-divide-y stack-divide-slate-100 bg-white">
                  {adminTeams.data.items.map((team) => {
                    const editName = teamNameEdits[team.id] ?? team.name;
                    const unchanged = editName.trim() === team.name;
                    const isAdminName = team.name.toLowerCase() === "admin";
                    const publicEnabled = Boolean(team.public_registration_enabled);
                    const rowBusy = publicRegistrationBusy(team);
                    return (
                      <tr key={team.id}>
                        <td className="min-w-64 px-3 py-2">
                          <Input
                            aria-label={`Team name for ${team.name}`}
                            value={editName}
                            onChange={(event) =>
                              setTeamNameEdits((current) => ({
                                ...current,
                                [team.id]: event.target.value,
                              }))
                            }
                          />
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                          {team.user_members?.length ?? 0}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                          {team.disabled_at ? "Disabled" : "Active"}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                          {publicEnabled ? "Enabled" : "Private"}
                        </td>
                        <td className="px-3 py-2">
                          <div className="flex flex-wrap gap-2">
                            <Button
                              size="sm"
                              disabled={actorMissing || !editName.trim() || unchanged || updateTeam.isPending}
                              onClick={() => updateTeam.mutate({ team, name: editName })}
                            >
                              Save
                            </Button>
                            {!isAdminName ? (
                              <Button
                                size="sm"
                                variant={publicEnabled ? "danger" : "secondary"}
                                disabled={actorMissing || rowBusy}
                                onClick={() =>
                                  openDestructiveAction({
                                    kind: publicEnabled
                                      ? "disable-public-registration"
                                      : "enable-public-registration",
                                    team,
                                  })
                                }
                              >
                                {publicEnabled ? "Disable public registration" : "Enable public registration"}
                              </Button>
                            ) : null}
                          </div>
                          {enablePublicRegistration.isError &&
                          enablePublicRegistration.variables?.id === team.id ? (
                            <div className="mt-2">
                              <ErrorState error={enablePublicRegistration.error} />
                            </div>
                          ) : null}
                          {disablePublicRegistration.isError &&
                          disablePublicRegistration.variables?.id === team.id ? (
                            <div className="mt-2">
                              <ErrorState error={disablePublicRegistration.error} />
                            </div>
                          ) : null}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )
        ) : null}
        {createTeam.isError ? <ErrorState error={createTeam.error} /> : null}
        {updateTeam.isError ? <ErrorState error={updateTeam.error} /> : null}
      </Card.Body>
    </Card>
  );
}
