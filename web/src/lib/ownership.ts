export interface SubmittedByUser {
  id: string;
  username: string;
  team_id?: string | null;
  team_name?: string | null;
}

export interface OwnershipRow {
  submitted_by_user?: SubmittedByUser | null;
  owner_team?: { id: string; name: string } | null;
  team_id?: string | null;
  team_name?: string | null;
}

export function ownershipLabel(row: OwnershipRow): string {
  const username = row.submitted_by_user?.username?.trim();
  const team =
    row.submitted_by_user?.team_name ??
    row.owner_team?.name ??
    row.team_name ??
    row.team_id;
  if (username && team) return `${username} / ${team}`;
  return username || team || "-";
}
