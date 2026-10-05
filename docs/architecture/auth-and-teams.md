# Authentication and Teams

Loom supports username/password browser sessions, session-backed CLI access,
user-owned API tokens, legacy team tokens, worker tokens, and a singleton
operator secret. Teams are the boundary for submissions, provider
connections, usage attribution, members, and API-token administration.

## Identities

| Identity | Credential | Scope |
| --- | --- | --- |
| Browser or CLI user | Username/password followed by a host-scoped session cookie | Current team and membership role |
| User-owned API token | `loom_api_...` bearer token linked to a user | Its user and team |
| Legacy team token | Unowned `loom_team_...` bearer token | Its team, with compatibility restrictions |
| Platform administrator | User with `is_platform_admin=true` | Cross-team administrative access |
| Singleton operator | File-backed `loom_admin_...` bearer token | Operator and service administration |
| Worker | Database-backed worker bearer token | Internal worker APIs |

The singleton operator secret is not a browser account and is not stored in
the database token table. Browser and CLI users select one current team at a
time. Platform administrators still select a current team for ordinary work.

## Account onboarding and recovery

Public onboarding is closed unless an existing, enabled team has public
registration enabled.

1. The client lists eligible teams with `GET /api/v1/auth/public-teams`.
2. The user requests a globally unique, case-insensitive username and chooses
   one of those teams. Loom does not collect an email address.
3. An administrator approves or rejects the request. Approval creates the
   user and membership, then mints a one-time setup link.
4. The administrator reveals and shares the link manually. Loom stores only
   its hash and safe prefix.
5. The user sets a password and can then sign in from the web application or
   CLI.

Password recovery follows the same approval model: a user requests a reset by
username, an administrator approves it, and the administrator manually shares
the one-time reset link. Completing a reset revokes the user's active browser
sessions, user-owned API tokens, and other unconsumed password-reset links.

Invite-code routes remain available for compatibility. New onboarding should
use registration requests and setup links.

## Roles and teams

Membership roles are enforced by service authorization checks:

| Role | Capabilities |
| --- | --- |
| `viewer` | Read the current team's permitted resources |
| `member` | Viewer access plus submission |
| `owner` | Member access plus team, provider-connection, and API-token management |

Changing the current team requires a membership unless the user is a platform
administrator. A team can be disabled, have new submissions paused, or have
public registration enabled or disabled through the admin team routes.

The signed-out account-request form discovers eligible teams through the public
team list. It distinguishes loading, a successful empty result, and discovery
failure. Failure shows a fixed safe message and a retry action rather than claiming
that no teams allow registration. Retry is disabled while the list request is
pending; recovery preserves the requested username and requires an explicit team
selection. Refreshing a list that removes the selected team clears that selection.

## Platform administrator changes

Existing active platform administrators (session or user-owned token) and the
singleton operator can call:

- `POST /api/v1/admin/users/{user_id}/platform-admin/grant` with
  `{"ensure_admin_team": true}` (the default for `{}`). This also ensures `owner`
  membership in the existing, enabled reserved `admin` team. Set the option to
  `false` to change only the platform role.
- `POST /api/v1/admin/users/{user_id}/platform-admin/revoke` with the required
  `{"credential_policy": "revoke_all"}`. This removes the platform role and any
  reserved `admin` membership, and revokes every unrevoked session and user-owned
  API token, including cross-team tokens and the caller's credential on a self-revoke.

The target must be a full user UUID. Names, emails, prefixes and extra selectors
are rejected, missing users return 404, and disabled/inactive targets return 409.
Deployment-only identities cannot be promoted. Multiple case-insensitive `admin`
team matches return 409; a membership-ensuring grant also rejects a missing or
disabled reserved team. The workflow never creates teams or removes unrelated
memberships. A revoked user can sign in again with their ordinary team roles.

Grant preserves existing credentials; their authority follows the current user
flag on subsequent requests. Revoke deliberately logs the user out rather than
preserving credentials that an administrator may have issued for other teams.
Already authorized in-flight requests are not cancelled. Repeated operations on
unchanged state succeed with `changed=false`; each successful request still writes
an audit event. A later revoke also revokes any credentials created since the last
revoke, even if the user is already an ordinary user.

The user row serializes changes. Role, membership, credential revocations and the
`user.platform_admin.grant` or `user.platform_admin.revoke` audit event commit in
one transaction; an audit failure rolls all mutations back. Events contain the
authenticated actor, target UUID, prior/new role and membership, credential policy,
and revocation counts, without raw credentials. Human actors come from the login
and cannot be overridden by a header. Singleton requests require
`X-Loom-Admin-Actor`; session requests also require the normal CSRF proof.

See the [operator procedure](../runbooks/operator-runbook.md#platform-administrator-grant-and-revoke)
for CLI commands and independent Nebius integration acceptance.

## Sessions and CSRF

Successful password login creates a database-backed session and sets the raw
session secret in an HttpOnly `__Host-loom_session` cookie. Hosted sessions use
Secure, Path=/, SameSite=Lax and no Domain attribute, including development.
`auth_session_cookie_name` configures the base name; hosted mode adds `__Host-`
unless already present. The injectable legacy cookie name is not authenticated
in hosted mode. Existing hosted users must log in again after this change.
Loom stores hashes of the
session and CSRF secrets. The CSRF secret is returned in authentication JSON
and kept in application or CLI memory; it is sent as `X-Loom-CSRF` on unsafe
session-authenticated requests.

`GET /api/v1/auth/me` returns a stable CSRF proof without rotating the session's
CSRF nonce, so an identity read in another tab cannot invalidate a pending write.
The proof is HMAC-SHA256 keyed by the raw HttpOnly session cookie over
`loom-session-csrf-v1` followed by a NUL byte and the stored CSRF hash. Stored
hashes alone cannot mint it. Service and Control Plane both verify this proof;
the random token returned by login/team/refresh remains valid for compatibility.
Switching teams rotates the CSRF nonce and invalidates both previous proofs.
Refreshing a normal session rotates both the session and CSRF secrets. Logout
revokes the session and clears the authentication cookies. Bearer-token
requests do not use the browser-session CSRF check.

Hosted unsafe browser requests must also match the canonical `public_base_url`
origin (or the supported `LOOM_PUBLIC_BASE_URL` fallback when unset). Links,
cookie HTTPS policy and origin checks share that same precedence. Sibling-host,
null and malformed origins are rejected, including before
unauthenticated login. When Origin is absent, cross-site/same-site Fetch Metadata
is rejected; non-browser clients without either header retain normal auth/CSRF
requirements. This is an origin check, not a wildcard CORS allowance. Configure
the actual HTTPS public origin at the ingress; forwarded headers cannot override
that configured origin.

Local HTTP compatibility requires `LOOM_SVC_AUTH_LOCAL_HTTP=true`; the local
Compose file explicitly selects it. It retains `loom_session` and the CSRF-token
check while allowing the Vite proxy's local origins. It cannot disable Secure or
host scoping for production, an HTTPS `public_base_url`, or a `__Host-` name.

The CLI stores its current session cookie and CSRF token in the selected Loom
profile, including the issued cookie name. It understands both legacy and hosted
cookies, promotes validated rotations before the next request, rejects invalid
hosted cookie attributes and name downgrades, and bounds
session requests and redirects to the authenticated origin. Changing `server_url`
with `loom config set` clears the old environment's login credentials. These
values are credentials and must not be printed or committed. Internal trial
cancellation continues to normalize the public cookie name to the existing
Service-to-Control-Plane contract; the Control Plane independently verifies the
session and CSRF secret against that environment's database.

## CLI workflow

The `loom auth` command group exposes the current account flow:

```text
loom auth register
loom auth setup-password
loom auth login
loom auth status
loom auth whoami
loom auth teams
loom auth forgot-password
loom auth reset-password
loom auth logout
```

`loom auth teams --server URL` lists teams that currently accept public
registration requests; it does not list a signed-in user's memberships or
switch the active team. Signed-in browser users change teams with the Settings
team switcher. The service also exposes the session-authenticated
`POST /api/v1/auth/team` route for clients that implement the session and CSRF
flow. See the [user guide](../user-guide.md#web-sessions-and-teams).

## Singleton operator secret

The operator credential is a TOML file containing an `[admin]` table and a
high-entropy `loom_admin_...` token. Production services read it from the
component-specific admin-secret file settings and fail closed when it is
missing, malformed, low-entropy, or unsafe. POSIX deployments require mode
`0600`. Local `loom service up` manages a development copy
under `.loom/admin/secrets.toml`.

Manage the local secret without printing it by default:

```text
loom service init-admin
loom service reveal-admin
loom service rotate-admin
```

`reveal-admin` is the explicit secret-display operation. Protect its terminal
and logs accordingly. Rotation replaces the singleton credential used by the
service, Control Plane, and LLM Gateway.

The operator bearer grants `admin:tokens`, `admin:rate_cards`, and
`admin:worker_pools`. Hosted worker-token rotation targets the Nebius
in-cluster workers: install the replacement token, restart workers, verify
registration, then revoke the previous token by its hash prefix.

Administrative mutations write durable attribution records to
`admin_audit_events`; mutations that share the service database with their
audit record fail if that record cannot be written.

## Browser acceptance

Use a dedicated test account through normal username/password login. Admin page
checks use a platform administrator; task/provider and tenant-isolation checks
use ordinary team members. Never replace user acceptance with administrator
access. The special staging operator-to-browser exchange is retired, and its
old cookies are rejected without modifying ordinary sessions or audit history.

From `web/`, run:

```bash
npm run smoke:admin -- --url https://YOUR-LOOM-ORIGIN --username USER --password env:LOOM_SMOKE_PASSWORD
```

A `file:PATH` password source is also supported. Add `--team-id UUID` to exercise a team switch and verify it
survives page reload and API readback. The smoke visits the normal login form, checks the admin tabs,
audit and rate-card pages, then logs out and confirms session invalidation. It
does not create accounts, tokens, providers or tasks, save browser state, or
emit credentials/screenshots. Use a dedicated account; do not reuse a person's
interactive session. Migrate any external broker invocation of the removed
script/image to this normal-login command before rollout; no old alias remains.

Platform administrators can select an enabled team without a membership row;
ordinary users can select only their enabled member teams. The same policy
owns the picker, switch authorization, and subsequent session readback. A
rejected switch preserves the previous team and returns 403/404, while invalid
authentication continues to return 401.
For an already selected team, session authentication preserves the user's
identity if the team is subsequently disabled. Existing route authorization
continues to reject ordinary members with 403, including at the control-plane
cancellation boundary; disabling a team is not misreported as an expired login.

## Persistence and implementation

Authentication and team records live in Postgres. The principal tables are
`users`, `teams`, `team_memberships`, `user_sessions`,
`user_registration_requests`, `password_reset_requests`,
`account_action_tokens`, `team_invites`, `tokens`, and
`admin_audit_events`.

The current schema is defined in
[`src/loom/db/schema.py`](../../src/loom/db/schema.py). Public auth and session
routes are implemented in
[`src/loom_service/routes/auth.py`](../../src/loom_service/routes/auth.py), team
administration in
[`src/loom_service/routes/teams.py`](../../src/loom_service/routes/teams.py),
and compatibility invites in
[`src/loom_service/routes/invites.py`](../../src/loom_service/routes/invites.py).

For attack assumptions and security invariants, see the
[authentication threat model](auth-threat-model.md).

## Access navigation and audit filters

Team access tabs use `/admin/access?tab=requests|accounts|teams|invites|tokens|audit`,
with role-specific visibility unchanged. Requests reviews new account/team
applications; Accounts handles existing-account password recovery. Settings links
Create CLI token directly to `?tab=tokens`.

The Audit tab defaults to access/account/team/token events. Full system audit
remains available. Scope, actor/action substrings, UTC date bounds, and cursor
pagination are applied server-side before the page limit. Date-time API inputs
without an offset are interpreted as UTC. Filters and page history are retained
in the URL; changing a filter clears the old cursor. Ordinary users and team
owners gain no platform-audit permission from these controls.
