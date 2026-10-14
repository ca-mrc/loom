import { apiFetch, qs } from "./core";
import { type AdminTeam } from "./runs";

export type UserRegistrationEntry = import("./schema").components["schemas"]["UserRegistrationEntry"];

export type AccountActionApproval = import("./schema").components["schemas"]["AccountActionApproval"];

export type PasswordResetRequestEntry = import("./schema").components["schemas"]["PasswordResetRequestEntry"];

export type TeamRegistrationEntry = import("./schema").components["schemas"]["TeamRegistrationEntry"];

export interface TeamRegistrationApprovalBody {
  team_id: string;
  role: InviteRole;
}

export type TeamRegistrationApproval = import("./schema").components["schemas"]["TeamRegistrationApproval"];

export interface AdminAuditEvent {
  id: string;
  created_at: string;
  actor: string;
  action: string;
  target_type: string;
  target_id: string;
  request_id: string | null;
  source_ip_hash: string | null;
  user_agent_hash: string | null;
  metadata: Record<string, unknown>;
}

export type InviteStatus = "pending" | "accepted" | "revoked" | "expired";

export type InviteRole = "owner" | "member" | "viewer";

export type InviteEntry = import("./schema").components["schemas"]["InviteEntry"];

export interface InviteCreateBody {
  email: string;
  team_id?: string;
  role: InviteRole;
  expires_in_days: number;
  max_uses?: number | null;
  allowed_domain?: string | null;
}

export type InviteReveal = import("./schema").components["schemas"]["InviteReveal"];

export type InviteLookup = import("./schema").components["schemas"]["InviteLookup"];

export interface ApiTokenEntry {
  name: string | null;
  token_hash_prefix: string;
  type: string;
  scopes: string[];
  team_id: string | null;
  issued_at: string;
  expires_at: string | null;
  revoked_at: string | null;
  last_used_at?: string | null;
  created_by_actor?: string | null;
  created_by_user_id?: string | null;
}

export interface ApiTokenList {
  items: ApiTokenEntry[];
}

export interface ApiTokenReveal {
  token: string;
  token_hash_prefix: string;
  expires_at: string | null;
  item?: ApiTokenEntry;
}

export const adminApi = {
  listTokens: () => apiFetch<ApiTokenList>("/api/v1/tokens"),
  createToken: (
    body: {
      name: string;
      type: string;
      scopes: string[];
      expires_in_days: number;
      team_id?: string;
    },
    actor?: string,
  ) =>
    apiFetch<ApiTokenReveal>("/api/v1/tokens", {
      method: "POST",
      headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
      body: JSON.stringify(body),
    }),
  rotateToken: (prefix: string, actor?: string) =>
    apiFetch<ApiTokenReveal>(`/api/v1/tokens/${encodeURIComponent(prefix)}/rotate`, {
      method: "POST",
      headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
    }),
  revokeToken: (prefix: string, actor?: string) =>
    apiFetch<void>(`/api/v1/tokens/${encodeURIComponent(prefix)}`, {
      method: "DELETE",
      headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
    }),
  listTeamRegistrations: (status: TeamRegistrationEntry["status"] = "pending") =>
    apiFetch<{ items: TeamRegistrationEntry[] }>(`/api/v1/admin/team-registrations${qs({ status })}`),
  listUserRegistrationRequests: (status = "pending") =>
    apiFetch<{ items: UserRegistrationEntry[] }>(`/api/v1/admin/registration-requests${qs({ status })}`),
  approveUserRegistrationRequest: (id: string, role: InviteRole = "member") =>
    apiFetch<AccountActionApproval>(`/api/v1/admin/registration-requests/${encodeURIComponent(id)}/approve`, {
      method: "POST",
      body: JSON.stringify({ role }),
    }),
  rejectUserRegistrationRequest: (id: string, reason?: string) =>
    apiFetch<UserRegistrationEntry>(`/api/v1/admin/registration-requests/${encodeURIComponent(id)}/reject`, {
      method: "POST",
      body: JSON.stringify({ reason: reason ?? null }),
    }),
  listPasswordResetRequests: (status = "pending") =>
    apiFetch<{ items: PasswordResetRequestEntry[] }>(
      `/api/v1/admin/password-reset-requests${qs({ status })}`,
    ),
  approvePasswordResetRequest: (id: string) =>
    apiFetch<AccountActionApproval>(
      `/api/v1/admin/password-reset-requests/${encodeURIComponent(id)}/approve`,
      { method: "POST" },
    ),
  rejectPasswordResetRequest: (id: string, reason?: string) =>
    apiFetch<PasswordResetRequestEntry>(
      `/api/v1/admin/password-reset-requests/${encodeURIComponent(id)}/reject`,
      { method: "POST", body: JSON.stringify({ reason: reason ?? null }) },
    ),
  listAdminTeams: () => apiFetch<{ items: AdminTeam[] }>("/api/v1/admin/teams"),
  createAdminTeam: (body: { name: string }, actor: string) =>
    apiFetch<AdminTeam>("/api/v1/admin/teams", {
      method: "POST",
      headers: { "X-Loom-Admin-Actor": actor },
      body: JSON.stringify(body),
    }),
  updateAdminTeam: (id: string, body: { name: string }, actor: string) =>
    apiFetch<AdminTeam>(`/api/v1/admin/teams/${encodeURIComponent(id)}`, {
      method: "PATCH",
      headers: { "X-Loom-Admin-Actor": actor },
      body: JSON.stringify(body),
    }),
  enableTeamPublicRegistration: (id: string, actor: string) =>
    apiFetch<AdminTeam>(`/api/v1/admin/teams/${encodeURIComponent(id)}/public-registration/enable`, {
      method: "POST",
      headers: { "X-Loom-Admin-Actor": actor },
    }),
  disableTeamPublicRegistration: (id: string, actor: string) =>
    apiFetch<AdminTeam>(`/api/v1/admin/teams/${encodeURIComponent(id)}/public-registration/disable`, {
      method: "POST",
      headers: { "X-Loom-Admin-Actor": actor },
    }),
  approveTeamRegistration: (id: string, actor: string, body: TeamRegistrationApprovalBody) =>
    apiFetch<TeamRegistrationApproval>(`/api/v1/admin/team-registrations/${encodeURIComponent(id)}/approve`, {
      method: "POST",
      headers: { "X-Loom-Admin-Actor": actor },
      body: JSON.stringify(body),
    }),
  rejectTeamRegistration: (id: string, actor: string, reason?: string) =>
    apiFetch<TeamRegistrationEntry>(`/api/v1/admin/team-registrations/${encodeURIComponent(id)}/reject`, {
      method: "POST",
      headers: { "X-Loom-Admin-Actor": actor },
      body: JSON.stringify({ reason: reason ?? null }),
    }),
  listInvites: (q: { team_id?: string; status?: InviteStatus } = {}) =>
    apiFetch<{ items: InviteEntry[] }>(`/api/v1/invites${qs(q)}`),
  createInvite: (body: InviteCreateBody, actor?: string) =>
    apiFetch<InviteReveal>("/api/v1/invites", {
      method: "POST",
      headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
      body: JSON.stringify(body),
    }),
  revokeInvite: (id: string, reason?: string, actor?: string) =>
    apiFetch<InviteEntry>(`/api/v1/invites/${encodeURIComponent(id)}/revoke`, {
      method: "POST",
      headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
      body: JSON.stringify({ reason: reason ?? null }),
    }),
  resendInvite: (id: string, actor?: string) =>
    apiFetch<InviteReveal>(`/api/v1/invites/${encodeURIComponent(id)}/resend`, {
      method: "POST",
      headers: actor ? { "X-Loom-Admin-Actor": actor } : undefined,
    }),
  listAdminAuditEvents: (limit = 50, cursor?: string, filters: { scope?: "access" | "all"; actor?: string; action?: string; start?: string; end?: string } = {}) =>
    apiFetch<{ items: AdminAuditEvent[]; next_cursor: string | null }>(
      `/api/v1/admin/audit-events${qs({ limit, cursor, ...filters })}`,
    ),
};
