import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import NavBar from "../../components/NavBar";
import { setFrontendConfigForTests } from "../../lib/frontendConfig";

function renderNav(
  isAdmin: boolean,
  currentTeamRole: string | null = null,
  currentTeamName: string | null = null,
  currentUsername: string | null = null,
) {
  // #2009: the sidebar's VersionInfo entry uses react-query for its
  // focus-triggered build checks, so NavBar now needs a QueryClientProvider
  // ancestor even in isolation.
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  });
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <NavBar
          isAdmin={isAdmin}
          currentTeamRole={currentTeamRole}
          currentTeamName={currentTeamName}
          currentUsername={currentUsername}
        />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("NavBar", () => {
  afterEach(() => setFrontendConfigForTests(null));

  it("closes the menu with Escape while its trigger still has focus", async () => {
    renderNav(false);
    const user = userEvent.setup();
    const trigger = screen.getByRole("button", { name: "Menu" });
    await user.click(trigger);
    expect(trigger).toHaveAttribute("aria-expanded", "true");
    await user.keyboard("{Escape}");
    expect(trigger).toHaveAttribute("aria-expanded", "false");
    expect(trigger).toHaveFocus();
  });

  it("renders the team nav items", () => {
    renderNav(false);
    expect(screen.getByRole("link", { name: "Home" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "New batch" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Monitor" })).toBeInTheDocument();
    for (const name of ["Pipelines", "Run Library", "Providers", "Getting started"]) {
      expect(screen.getByRole("link", { name })).toBeInTheDocument();
    }
    expect(screen.getByRole("link", { name: "Settings" })).toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: "Task sets" }),
    ).not.toBeInTheDocument();
  });

  it("uses a responsive shell that becomes a sidebar only on large screens", () => {
    renderNav(false);
    expect(screen.getByRole("navigation", { name: "Primary" })).toHaveClass(
      "w-full",
      "lg:w-56",
      "lg:flex-col",
    );
  });

  it("shows the signed-in user, current team, and role in the globalThis navigation", () => {
    renderNav(false, "owner", "EAI", "Qianyi");

    const identityContext = screen.getByLabelText("Current user and team");
    expect(identityContext).toHaveTextContent("Signed in");
    expect(identityContext).toHaveTextContent("Qianyi / EAI");
    expect(identityContext).toHaveTextContent("owner");
  });

  it("shows a clear non-production environment identity in the globalThis navigation", () => {
    setFrontendConfigForTests({
      environment: "development",
      environmentLabel: "Development / staging",
      routePath: "/dev",
      apiBase: "/dev",
      apiRouteBase: "https://yylx.world/dev/api",
    });

    renderNav(false, "member", "EAI", "Dev User");

    const environment = screen.getByLabelText("Frontend environment");
    expect(environment).toHaveTextContent("Development / staging");
    expect(environment).toHaveTextContent("https://yylx.world/dev/api");
  });

  it("shows production identity without beta wording", () => {
    setFrontendConfigForTests({
      environment: "production",
      environmentLabel: "Production",
      routePath: "/prod",
      apiBase: "/prod",
      apiRouteBase: "https://yylx.world/prod/api",
    });

    renderNav(false, "member", "EAI", "Prod User");

    const environment = screen.getByLabelText("Frontend environment");
    expect(environment).toHaveTextContent("Production");
    expect(environment).not.toHaveTextContent(/beta/i);
  });


  it("hides admin-only links from a team user", () => {
    renderNav(false, "member");
    expect(
      screen.queryByRole("link", { name: "Rate cards" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: "Team access" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Admin")).not.toBeInTheDocument();
  });

  it("shows team access to team owners without exposing platform admin tools", () => {
    renderNav(false, "owner");
    expect(
      screen.getByRole("link", { name: "Team access" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: "Rate cards" }),
    ).not.toBeInTheDocument();
  });

  it("shows admin section for an admin token", () => {
    renderNav(true);
    expect(screen.getByText("Admin")).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Rate cards" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Team access" }),
    ).toBeInTheDocument();
  });
});
