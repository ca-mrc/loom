import { type InviteLookup, type UserRegistrationEntry } from "./admin";
import { apiFetch, qs } from "./core";
import { type Team } from "./runs";
import { loadAuthSession, type PublicTeam } from "./authSession";
import { sessionActions } from "./authActions";

export * from "./authSession";
export { mutateAuthSession } from "./authActions";

export const authApi = {
  authMe: loadAuthSession,
  ...sessionActions,
  publicTeams: () => apiFetch<{ items: PublicTeam[] }>("/api/v1/auth/public-teams"),
  requestRegistration: (body: { username: string; team_id: string }) =>
    apiFetch<UserRegistrationEntry>("/api/v1/auth/registration-requests", {
      method: "POST",
      body: JSON.stringify({ ...body, metadata: {} }),
    }),
  setupLookup: (token: string) =>
    apiFetch<{ username: string; team: PublicTeam | null; expires_at: string }>(
      `/api/v1/auth/setup/lookup${qs({ token })}`,
    ),
  setupComplete: (body: { token: string; password: string; confirm_password: string }) =>
    apiFetch<{ status: string; user: { id: string; username: string } }>("/api/v1/auth/setup/complete", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  requestPasswordReset: (username: string) =>
    apiFetch<{ status: "pending" }>("/api/v1/auth/password-reset-requests", {
      method: "POST",
      body: JSON.stringify({ username }),
    }),
  resetLookup: (token: string) =>
    apiFetch<{ username: string; expires_at: string }>(`/api/v1/auth/reset/lookup${qs({ token })}`),
  resetComplete: (body: { token: string; password: string; confirm_password: string }) =>
    apiFetch<{ status: string; user: { id: string; username: string } }>("/api/v1/auth/reset/complete", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  lookupInvite: (code: string) => apiFetch<InviteLookup>(`/api/v1/invites/lookup${qs({ code })}`),
  getTeam: (teamId: string) => apiFetch<Team>(`/api/v1/teams/${teamId}`),
};
