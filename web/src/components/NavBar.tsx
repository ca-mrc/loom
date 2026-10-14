/**
 * Sidebar navigation. Vertical column on left of the app shell.
 * Team owners and platform admins see Team access; only platform admins
 * see Rate cards in the sidebar. Route and server authorization remain
 * authoritative, including team-aware catalog reads and separate write checks.
 *
 * The `loom` wordmark is intentionally typographic, not an image —
 * keeps the bundle small and matches the rest of the type discipline.
 */
import { useEffect, useRef, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";

import { cn } from "../lib/cn";
import { getFrontendConfig } from "../lib/frontendConfig";
import VersionInfo from "./VersionInfo";

const NAV_ITEMS = [
  { to: "/", label: "Home" },
  { to: "/getting-started", label: "Getting started" },
  { to: "/batches/new", label: "New batch" },
  { to: "/monitor", label: "Monitor" },
  { to: "/pipelines", label: "Pipelines" },
  { to: "/library", label: "Run Library" },
  { to: "/providers", label: "Providers" },
];

const TEAM_ADMIN_NAV_ITEM = { to: "/admin/access", label: "Team access" };

const PLATFORM_ADMIN_NAV_ITEMS = [
  { to: "/rate-cards", label: "Rate cards" },
];

const LINK_BASE =
  "flex shrink-0 items-center rounded-lg px-3 py-2 text-sm font-medium transition-colors";
const LINK_INACTIVE = "text-slate-600 hover:bg-slate-100 hover:text-slate-900";
const LINK_ACTIVE = "bg-accent text-white hover:bg-accent-hover";

function formatRole(role: string | null): string | null {
  return role ? role.replace(/_/g, " ") : null;
}

export interface NavBarProps {
  isAdmin: boolean;
  currentUsername?: string | null;
  currentTeamName?: string | null;
  currentTeamRole?: string | null;
}

export default function NavBar({
  isAdmin,
  currentUsername = null,
  currentTeamName = null,
  currentTeamRole = null,
}: NavBarProps): JSX.Element {
  const [menuOpen, setMenuOpen] = useState(false);
  const menuButton = useRef<HTMLButtonElement>(null);
  const location = useLocation();
  useEffect(() => { setMenuOpen(false); }, [location.pathname, location.search]);
  const canManageTeam = isAdmin || currentTeamRole === "owner";
  const displayRole = formatRole(currentTeamRole);
  const frontendConfig = getFrontendConfig();
  const identityLabel = [
    currentUsername?.trim() || null,
    currentTeamName?.trim() || null,
  ].filter(Boolean).join(" / ");
  return (
    <nav
      aria-label="Primary"
      onKeyDown={(event) => { if (event.key === "Escape" && menuOpen) { setMenuOpen(false); menuButton.current?.focus(); } }}
      className="flex w-full shrink-0 flex-col gap-3 border-b border-slate-200 bg-white px-3 py-3 lg:h-full lg:w-56 lg:flex-col lg:border-b-0 lg:border-r lg:py-5"
    >
      <div className="flex items-center justify-between gap-3 px-3 lg:mb-3 lg:block">
        <div>
        <p className="text-lg font-bold tracking-tight text-slate-900">loom</p>
        <p className="hidden text-xs uppercase tracking-wider lg:block text-slate-600">
          benchmark platform
        </p>
        <div
          aria-label="Frontend environment"
          className="lg:mt-2 lg:rounded-md lg:border lg:border-slate-200 lg:bg-slate-50 lg:px-2 lg:py-1.5"
        >
          <p className="truncate text-xs font-semibold text-slate-700">
            {frontendConfig.environmentLabel}
          </p>
          <p className="mt-0.5 hidden truncate font-mono lg:block text-[10px] text-slate-500">
            {frontendConfig.apiRouteBase}
          </p>
        </div>
        </div>
        <button ref={menuButton} type="button" className="rounded-lg border border-slate-200 px-3 py-2 text-sm font-medium lg:hidden"
          aria-expanded={menuOpen} aria-controls="primary-menu" onClick={() => setMenuOpen((open) => !open)}>
          {menuOpen ? "Close menu" : "Menu"}
        </button>
      </div>
      <div id="primary-menu" className={cn(menuOpen ? "flex" : "hidden", "min-h-0 flex-1 flex-col gap-3 lg:flex")}
      >
      <div className="flex min-w-0 flex-1 flex-col gap-1 overflow-y-auto pb-1 lg:pb-0">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            className={({ isActive }) =>
              cn(LINK_BASE, isActive ? LINK_ACTIVE : LINK_INACTIVE)
            }
          >
            {item.label}
          </NavLink>
        ))}
        {canManageTeam ? (
          <>
            <div className="hidden px-3 pt-3 text-xs font-semibold uppercase tracking-wider text-slate-600 lg:block">
              {isAdmin ? "Admin" : "Team"}
            </div>
            <NavLink
              to={TEAM_ADMIN_NAV_ITEM.to}
              className={({ isActive }) =>
                cn(LINK_BASE, isActive ? LINK_ACTIVE : LINK_INACTIVE)
              }
            >
              {TEAM_ADMIN_NAV_ITEM.label}
            </NavLink>
            {isAdmin ? PLATFORM_ADMIN_NAV_ITEMS.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  cn(LINK_BASE, isActive ? LINK_ACTIVE : LINK_INACTIVE)
                }
              >
                {item.label}
              </NavLink>
            )) : null}
          </>
        ) : null}
      </div>

      {identityLabel || displayRole ? (
        <div
          aria-label="Current user and team"
          className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm"
        >
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-600">
            Signed in
          </p>
          <p className="mt-1 truncate font-medium text-slate-900">
            {identityLabel || "No team selected"}
          </p>
          {displayRole ? (
            <p className="mt-0.5 text-xs capitalize text-slate-500">
              {displayRole}
            </p>
          ) : null}
        </div>
      ) : null}

      <NavLink
        to="/settings"
        className={({ isActive }) =>
          cn(
            LINK_BASE,
            "w-fit lg:w-auto",
            isActive ? LINK_ACTIVE : LINK_INACTIVE,
          )
        }
      >
        Settings
      </NavLink>

      <VersionInfo environmentLabel={frontendConfig.environmentLabel} />
      </div>
    </nav>
  );
}
