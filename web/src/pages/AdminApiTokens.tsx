import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";
import {
  formatDate,
  formatTokenScopes,
  TOKEN_SCOPE_OPTIONS,
  tokenName,
  tokenStatus,
} from "./adminAccessState";

import type { AdminAccessViewState } from "./useAdminAccess";
export function AdminApiTokens({
  tokens,
  tokenNameInput,
  setTokenNameInput,
  tokenExpiresDays,
  setTokenExpiresDays,
  tokenCreateDisabled,
  createToken,
  tokenScopes,
  toggleTokenScope,
  actorMissing,
  tokenBusy,
  openDestructiveAction,
}: AdminAccessViewState): JSX.Element {
  return (
    <Card data-loom-query="api-tokens" data-loom-query-status={tokens.status}>
      <Card.Header
        title="API tokens"
        description="Create scoped tokens for CLI and automation. Raw token values are shown only once after create or rotate."
      />
      <Card.Body className="stack-y-5">
        <div className="grid gap-3 lg:grid-cols-[minmax(0,1fr)_9rem_auto]">
          <div>
            <label className="block text-sm font-medium text-slate-700" htmlFor="api-token-name">
              Token name
            </label>
            <Input
              id="api-token-name"
              className="mt-2"
              value={tokenNameInput}
              onChange={(event) => setTokenNameInput(event.target.value)}
              placeholder="Nightly CLI"
            />
          </div>
          <div>
            <label className="block text-sm font-medium text-slate-700" htmlFor="api-token-expires">
              Lifetime days
            </label>
            <Input
              id="api-token-expires"
              className="mt-2"
              type="number"
              min={1}
              value={tokenExpiresDays}
              onChange={(event) => setTokenExpiresDays(event.target.value)}
            />
          </div>
          <div className="flex items-end">
            <Button variant="primary" disabled={tokenCreateDisabled} onClick={() => createToken.mutate()}>
              Create API token
            </Button>
          </div>
        </div>

        <fieldset className="grid gap-2 md:grid-cols-2">
          <legend className="mb-1 text-sm font-medium text-slate-700">Token scopes</legend>
          {TOKEN_SCOPE_OPTIONS.map((option) => (
            <label
              key={option.value}
              className="flex gap-3 rounded-lg border border-slate-200 bg-white p-3 text-sm"
            >
              <input
                aria-label={option.label}
                type="checkbox"
                className="mt-1"
                checked={tokenScopes.includes(option.value)}
                onChange={(event) => toggleTokenScope(option.value, event.currentTarget.checked)}
              />
              <span>
                <span className="block font-medium text-slate-800">{option.label}</span>
                <span className="block text-xs text-slate-500">{option.description}</span>
              </span>
            </label>
          ))}
        </fieldset>

        {tokens.isPending ? <LoadingState /> : null}
        {tokens.isError ? <ErrorState error={tokens.error} /> : null}
        {tokens.data ? (
          tokens.data.items.length === 0 ? (
            <EmptyState label="No API tokens." />
          ) : (
            <div className="overflow-x-auto" tabIndex={0} role="region" aria-label="API tokens scroll area">
              <table aria-label="API tokens" className="min-w-full stack-divide-y stack-divide-slate-200 text-sm">
                <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
                  <tr>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Name
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Prefix
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Scopes
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Last used
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Expires
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Status
                    </th>
                    <th scope="col" className="px-3 py-2 font-semibold">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="stack-divide-y stack-divide-slate-100 bg-white">
                  {tokens.data.items.map((token) => {
                    const status = tokenStatus(token);
                    const label = tokenName(token);
                    const inactive = token.revoked_at !== null;
                    return (
                      <tr key={token.token_hash_prefix}>
                        <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-900">{label}</td>
                        <td className="whitespace-nowrap px-3 py-2 font-mono text-xs text-slate-600">
                          {token.token_hash_prefix}
                        </td>
                        <td className="max-w-sm px-3 py-2 text-slate-600">
                          {formatTokenScopes(token.scopes)}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                          {token.last_used_at ? formatDate(token.last_used_at) : "Never"}
                        </td>
                        <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                          {formatDate(token.expires_at)}
                        </td>
                        <td className={`whitespace-nowrap px-3 py-2 font-medium ${status.className}`}>
                          {status.label}
                        </td>
                        <td className="px-3 py-2">
                          <div className="flex gap-2">
                            <Button
                              size="sm"
                              aria-label={`Rotate ${label}`}
                              disabled={inactive || actorMissing || tokenBusy(token)}
                              onClick={() =>
                                openDestructiveAction({
                                  kind: "rotate-token",
                                  token,
                                })
                              }
                            >
                              Rotate
                            </Button>
                            <Button
                              size="sm"
                              variant="danger"
                              aria-label={`Revoke ${label}`}
                              disabled={inactive || actorMissing || tokenBusy(token)}
                              onClick={() =>
                                openDestructiveAction({
                                  kind: "revoke-token",
                                  token,
                                })
                              }
                            >
                              Revoke
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
        {createToken.isError ? <ErrorState error={createToken.error} /> : null}
      </Card.Body>
    </Card>
  );
}
