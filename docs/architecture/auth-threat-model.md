# Authentication and Team Threat Model

This page states the security assumptions enforced by Loom's current account,
team, session, token, and operator-authentication paths. The concrete identity
and onboarding behavior is documented in [Authentication and Teams](auth-and-teams.md).

## Protected assets

- singleton operator authority and user-owned API tokens;
- one-time password setup and reset links;
- provider credentials and resolved upstream destinations;
- team-owned runs, trajectories, feedback, artifacts, and usage;
- deployment, database, object-store, and signing credentials; and
- administrative audit attribution.

## Trust boundaries

| Boundary | Required invariant |
| --- | --- |
| Public browser and API | Authentication and scope checks run before privileged handlers. Unknown reset usernames receive the same public response as known users. |
| Team data | A normal principal can act only for its current or token-bound team and allowed role. Shared completed results use explicit Run Library visibility checks. |
| Singleton operator secret | The high-entropy file-backed secret is mode-restricted, excluded from the database token table, and never implicitly displayed. |
| Browser session | Hosted cookies are host-only Secure/HttpOnly `__Host-` cookies and hash-only at rest. Unsafe requests require the matching in-memory CSRF token and reject foreign browser origins, including sibling hosts. |
| User API token | The raw token is revealed once, stored only as a hash, and bound to one user and team. |
| Provider connection | Raw upstream credentials remain in trusted service and Gateway code and are redacted from errors, logs, audit rows, and artifacts. |
| CI and deployment | Untrusted pull-request code does not receive deployment, provider, object-store, publishing, or signing credentials. |
| Administrative mutation | The authenticated authority and safe actor metadata are written to `admin_audit_events` before success is returned. |

## Expected compromise radius

| Compromise | Contained authority |
| --- | --- |
| User-owned API token | An ordinary owner's team and allowed scopes; a current platform administrator's token also carries administrative authority, including cross-team access. Expiry and revocation still apply. |
| Setup or reset link | One target account until consumption, revocation, or expiry |
| Browser session | The signed-in user's current role until logout, revocation, or expiry |
| Staging acceptance cookie | The remaining part of its fixed 900-second staging platform-admin session; no refresh or production use |
| Singleton operator secret | Platform administration until secret rotation and service reload/restart |
| Provider key | The upstream provider account until provider-side rotation |
| Database read | Hashed tokens and sessions plus application data; not the singleton plaintext secret or plaintext provider keys |

## Enforced controls

User-token administrative authority follows the owner's current role on each
request. Granting the role preserves existing credentials and elevates their
authority on subsequent requests; revoking it revokes the owner's sessions and
API tokens. See [platform administrator changes](auth-and-teams.md#platform-administrator-changes).

- Production starts only with a valid, private singleton-secret file.
- Registration targets an existing team with public registration enabled and
  requires administrative approval.
- Setup and reset tokens are one-time, hashed at rest, purpose-bound, expiring,
  and manually shared by an administrator.
- Password reset revokes active sessions, user-owned API tokens, and other
  pending reset links for that user.
- Session-authenticated mutations require `X-Loom-CSRF`; bearer-token requests
  do not use the browser CSRF mechanism.
- Team membership roles and platform-admin authority are enforced by service
  dependencies rather than by frontend visibility.
- Provider and authentication diagnostics pass through central redaction.
- Run Library sharing does not weaken owner-team checks on execution and
  control routes.
- External or ordinary CI executes without protected environment secrets.

## Residual risk

- A same-origin script compromise can use the in-memory CSRF value and act as
  the signed-in user, although it cannot read the HttpOnly session cookie.
- The singleton operator secret grants broad authority until rotated; protect
  its file, terminal reveal, backups, and any process that can read it.
- Legacy unowned team tokens cannot provide user attribution and should be
  replaced with user-owned tokens where attribution matters.
- `Public` sandbox networking does not block cloud metadata. Use the enforced
  policies and deployment controls described in
  [Sandbox Isolation](sandbox-isolation.md).
- Host root, Docker, cluster-admin, database-admin, and object-store-admin
  access remain infrastructure trust boundaries outside application tenancy.

Treat a suspected credential or cross-team data leak as a private security
report under the process in [`SECURITY.md`](../../SECURITY.md).
