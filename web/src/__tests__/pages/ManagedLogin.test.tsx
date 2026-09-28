import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StrictMode } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";

import App from "../../App";
import { AuthProvider } from "../../auth/AuthContext";
import { captureManagedLoginProof } from "../../lib/managedLogin";

const token = "loom_env_login_" + "a".repeat(43);
const owner = {
  user: { id: "owner", username: "owner", email: null, display_name: "Alice", is_platform_admin: false },
  teams: [{ id: "team", name: "Development alice", role: "owner" }],
  current_team: { id: "team", name: "Development alice", role: "owner" },
  role: "owner", scopes: ["read:own", "submit"], is_platform_admin: false, csrf_token: "child-csrf",
};

afterEach(() => vi.restoreAllMocks());

it.each([
  { succeeds: true, token }, { succeeds: false, token },
  { succeeds: true, token: "loom_app_login_" + "b".repeat(32) + "_" + "a".repeat(43) },
  { succeeds: false, token: "loom_app_login_" + "b".repeat(32) + "_" + "a".repeat(43) },
])("managed route consumes one proof only after an explicit click (success=$succeeds)", async ({ succeeds, token }) => {
  let scrubbed = false;
  captureManagedLoginProof(
    { pathname: "/auth/managed", protocol: "https:", hash: "#token=" + token },
    { state: null, replaceState: () => { scrubbed = true; } },
  );
  const submitted: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    expect(scrubbed).toBe(true);
    if (String(input).endsWith("/api/v1/auth/login/complete")) {
      submitted.push(JSON.parse(String(init?.body)).token);
      return new Response(JSON.stringify(succeeds ? owner : { detail: "echoed " + token }), {
        status: succeeds ? 200 : 400, headers: { "Content-Type": "application/json" },
      });
    }
    return new Response(JSON.stringify({ detail: "signed out" }), {
      status: 401, headers: { "Content-Type": "application/json" },
    });
  });
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<StrictMode><QueryClientProvider client={queryClient}><MemoryRouter initialEntries={["/auth/managed"]}
    future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
    <AuthProvider><App /></AuthProvider>
  </MemoryRouter></QueryClientProvider></StrictMode>);
  const button = await screen.findByRole("button", { name: "Sign into this environment" });
  expect(submitted).toEqual([]);
  await userEvent.click(button);
  await waitFor(() => expect(submitted).toEqual([token]));
  if (succeeds) {
    expect(await screen.findByText("Signed in to this environment.")).toBeInTheDocument();
  } else {
    expect(await screen.findByRole("alert")).toHaveTextContent("fresh browser login");
  }
  expect(screen.queryByRole("button", { name: "Sign into this environment" })).not.toBeInTheDocument();
  expect(document.body.textContent).not.toContain(token);
});
