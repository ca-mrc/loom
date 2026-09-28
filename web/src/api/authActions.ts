import { AuthSessionLoadError, apiBase, apiFetch, authHeaders } from "./core";
import { parseAuthSessionResponse, TeamSwitchRejectedError, type AuthMe } from "./authSession";

export async function mutateAuthSession(path: string, body: unknown): Promise<AuthMe> {
  let response: Response;
  try {
    response = await fetch(`${apiBase()}${path}`, {
      method: "POST",
      body: JSON.stringify(body),
      // Session endpoints return JSON at their exact origin. A 307/308 must
      // never forward passwords, invitation tokens or one-use managed proofs.
      redirect: "error",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        ...authHeaders(undefined, "POST"),
      },
    });
  } catch {
    throw new AuthSessionLoadError("network");
  }

  if (path === "/api/v1/auth/team" && (response.status === 403 || response.status === 404)) {
    throw new TeamSwitchRejectedError(response.status);
  }
  return parseAuthSessionResponse(response);
}

export const sessionActions = {
  loginPassword: (username: string, password: string) =>
    mutateAuthSession("/api/v1/auth/login", { username, password }),
  loginStart: (email: string) =>
    apiFetch<{ status: "sent"; login_token?: string }>("/api/v1/auth/login/start", {
      method: "POST",
      body: JSON.stringify({ email }),
    }),
  loginComplete: (token: string) => mutateAuthSession("/api/v1/auth/login/complete", { token }),
  acceptInvite: (body: { code: string; email?: string | null }) =>
    mutateAuthSession("/api/v1/invites/accept", body),
  switchTeam: (teamId: string) => mutateAuthSession("/api/v1/auth/team", { team_id: teamId }),
  logout: () => apiFetch<void>("/api/v1/auth/logout", { method: "POST" }),
};

