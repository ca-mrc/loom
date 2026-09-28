import { AuthSessionLoadError, apiBase } from "./core";

export class TeamSwitchRejectedError extends Error {
  constructor(status: number) {
    super(status === 404
      ? "That team no longer exists. Your current team is unchanged."
      : "You cannot switch to that team. It may be disabled or you may no longer have access. Your current team is unchanged.");
    this.name = "TeamSwitchRejectedError";
  }
}

export interface AuthTeam {
  id: string;
  name: string;
  role: string;
}

export interface AuthMe {
  user: {
    id: string;
    username: string;
    email?: string | null;
    display_name: string | null;
    is_platform_admin: boolean;
  };
  teams: AuthTeam[];
  current_team: AuthTeam | null;
  role: string | null;
  scopes: string[];
  is_platform_admin: boolean;
  csrf_token: string;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

export function parseAuthTeam(value: unknown): AuthTeam | null {
  if (!isRecord(value)) return null;
  if (!isNonEmptyString(value.id) || !isNonEmptyString(value.name) || !isNonEmptyString(value.role)) {
    return null;
  }
  return { id: value.id, name: value.name, role: value.role };
}

export function parseAuthMe(value: unknown): AuthMe {
  if (
    !isRecord(value) ||
    !isRecord(value.user) ||
    !Array.isArray(value.teams) ||
    !Array.isArray(value.scopes)
  ) {
    throw new AuthSessionLoadError("invalid");
  }
  const user = value.user;
  const teams = value.teams.map(parseAuthTeam);
  const currentTeam = value.current_team === null ? null : parseAuthTeam(value.current_team);
  if (
    !isNonEmptyString(user.id) ||
    !isNonEmptyString(user.username) ||
    !(typeof user.email === "string" || user.email === null) ||
    !(typeof user.display_name === "string" || user.display_name === null) ||
    typeof user.is_platform_admin !== "boolean" ||
    teams.some((team) => team === null) ||
    (value.current_team !== null && currentTeam === null) ||
    value.scopes.some((scope) => !isNonEmptyString(scope)) ||
    typeof value.is_platform_admin !== "boolean" ||
    value.is_platform_admin !== user.is_platform_admin ||
    (value.role !== null && !isNonEmptyString(value.role)) ||
    !isNonEmptyString(value.csrf_token)
  ) {
    throw new AuthSessionLoadError("invalid");
  }
  const parsedTeams = teams as AuthTeam[];
  if (
    currentTeam !== null &&
    !parsedTeams.some(
      (team) =>
        team.id === currentTeam.id && team.name === currentTeam.name && team.role === currentTeam.role,
    )
  ) {
    throw new AuthSessionLoadError("invalid");
  }

  return {
    user: {
      id: user.id,
      username: user.username,
      email: user.email,
      display_name: user.display_name,
      is_platform_admin: user.is_platform_admin,
    },
    teams: parsedTeams,
    current_team: currentTeam,
    role: value.role as string | null,
    scopes: value.scopes as string[],
    is_platform_admin: value.is_platform_admin,
    csrf_token: value.csrf_token,
  };
}

export async function parseAuthSessionResponse(response: Response): Promise<AuthMe> {
  if (response.status === 401) {
    throw new AuthSessionLoadError("unauthorized");
  }
  if (!response.ok) {
    // Session-producing responses can contain proxy diagnostics or echoed
    // request data. Classification never requires consuming their body.
    throw new AuthSessionLoadError("http");
  }
  if (response.status === 204) {
    throw new AuthSessionLoadError("invalid");
  }

  try {
    return parseAuthMe(await response.json());
  } catch (error) {
    if (error instanceof AuthSessionLoadError) throw error;
    throw new AuthSessionLoadError("invalid");
  }
}

export async function loadAuthSession(): Promise<AuthMe> {
  let response: Response;
  try {
    response = await fetch(`${apiBase()}/api/v1/auth/me`, {
      credentials: "include",
      headers: { Accept: "application/json" },
    });
  } catch {
    throw new AuthSessionLoadError("network");
  }

  return parseAuthSessionResponse(response);
}

export interface PublicTeam {
  id: string;
  name: string;
}

