import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";

import VersionInfo from "../../components/VersionInfo";
import { LOADED_BUILD_INFO } from "../../lib/buildInfo";
import { setFrontendConfigForTests } from "../../lib/frontendConfig";

vi.mock("../../lib/buildInfo", async () => ({
  ...await vi.importActual<typeof import("../../lib/buildInfo")>("../../lib/buildInfo"),
  LOADED_BUILD_INFO: {
    revision: null, sourceRef: "personal", buildTime: null, kind: "personal",
    sourceDigest: "sha256:" + "a".repeat(64), baseCommit: "b".repeat(40),
  },
}));

afterEach(() => {
  vi.restoreAllMocks();
  setFrontendConfigForTests(null);
});

it("keeps loaded personal source separate from served and backend builds", async () => {
  setFrontendConfigForTests({
    environment: "development", environmentLabel: "Personal", routePath: "", apiBase: "",
    apiRouteBase: "/api", servedBuildRevision: null, servedSourceDigest: "sha256:" + "c".repeat(64),
  });
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    const isBackend = String(input).includes("/version");
    return new Response(JSON.stringify(isBackend
      ? { buildRevision: null, buildTime: null, buildKind: "personal", sourceDigest: "sha256:" + "d".repeat(64), sourceBaseCommit: "e".repeat(40) }
      : { buildRevision: null, buildKind: "personal", sourceDigest: "sha256:" + "c".repeat(64) }),
    { status: 200, headers: { "Content-Type": "application/json" } });
  });
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
    <QueryClientProvider client={client}><VersionInfo environmentLabel="Personal" /></QueryClientProvider>
  </MemoryRouter>);
  expect(screen.getByRole("button", { name: "Deployed version details" })).toHaveTextContent("Personal aaaaaaaaaaaa");
  await userEvent.click(screen.getByRole("button", { name: "Deployed version details" }));
  const dialog = await screen.findByRole("dialog", { name: "Deployed version" });
  const frontend = within(dialog).getByRole("region", { name: "Frontend build" });
  expect(within(frontend).getByTitle("Copy " + LOADED_BUILD_INFO.sourceDigest)).toBeInTheDocument();
  expect(within(frontend).getByText("Personal source — not CI-approved")).toBeInTheDocument();
  expect(within(frontend).getByText("Base commit (informational)")).toBeInTheDocument();
  expect(within(frontend).queryByRole("link", { name: "View commit" })).not.toBeInTheDocument();
  const backend = within(dialog).getByRole("region", { name: "Backend build" });
  expect(await within(backend).findByTitle("Copy sha256:" + "d".repeat(64))).toBeInTheDocument();
  expect(within(dialog).getByText("A newer frontend build is available.")).toBeInTheDocument();
  expect(LOADED_BUILD_INFO.sourceDigest).toBe("sha256:" + "a".repeat(64));
});
