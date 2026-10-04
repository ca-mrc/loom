/**
 * VersionInfo — the persistent "Nebius · env" / "Build <commit>" entry at
 * the bottom of the sidebar (#2009). Clicking it opens accessible details:
 * full frontend commit (copy + GitHub link), source ref, build time, and
 * the backend's own reported build — a clearly separate identity, since
 * one backend response is evidence for that responding instance only.
 *
 * The loaded frontend identity (`buildInfo.ts`) is a frozen, build-time
 * constant: it never changes for this already-open tab. The "served" check
 * (`useServedFrontendBuild`) is a live check — every 10 visible minutes, on
 * focus/visibility (throttled), and on opening these details — that can
 * drift from it after a rollout. That is surfaced as a non-disruptive update
 * notice with an explicit refresh action, never an automatic reload or lost
 * input.
 */
import { useState } from "react";

import { LOADED_BUILD_INFO, shortRevision } from "../lib/buildInfo";
import {
  frontendUpdateStatus,
  useServedFrontendBuild,
} from "../lib/buildVersion";

import { lazyRoute } from "../lib/lazyRoute";
import { RouteRecoveryBoundary } from "./RouteRecoveryBoundary";

const VersionDetails = lazyRoute(() => import("./VersionDetails"));

export interface VersionInfoProps {
  environmentLabel: string;
}

export default function VersionInfo({
  environmentLabel,
}: VersionInfoProps): JSX.Element {
  const [open, setOpen] = useState(false);
  const served = useServedFrontendBuild();
  const { hasNewerBuild } = frontendUpdateStatus(
    LOADED_BUILD_INFO.revision,
    served.data,
    LOADED_BUILD_INFO.sourceDigest,
  );
  const revision = LOADED_BUILD_INFO.revision;

  return (
    <>
      <button
        type="button"
        onClick={() => {
          setOpen(true);
          served.checkNow();
        }}
        aria-label="Deployed version details"
        className="flex w-full flex-col gap-0.5 rounded-md border border-slate-200 bg-slate-50 px-2 py-1.5 text-left hover:bg-slate-100"
      >
        {/* The environment label may be long (e.g. "Nebius integration"),
            so it alone truncates; the revision gets its own line and is
            never clipped, keeping the build identifiable at a glance. */}
        <span className="flex w-full min-w-0 items-center justify-between gap-2">
          <span
            className="min-w-0 truncate font-mono text-[10px] text-slate-600"
            title={`Nebius · ${environmentLabel}`}
          >
            Nebius · {environmentLabel}
          </span>
          {hasNewerBuild ? (
            <span
              aria-hidden="true"
              className="h-1.5 w-1.5 shrink-0 rounded-full bg-accent"
              title="A newer build is available"
            />
          ) : null}
        </span>
        <span
          data-testid="sidebar-build-revision"
          className="whitespace-nowrap font-mono text-[10px] font-medium text-slate-700"
        >
          {LOADED_BUILD_INFO.kind === "personal"
            ? `Personal ${LOADED_BUILD_INFO.sourceDigest?.replace(/^sha256:/, "").slice(0, 12) ?? "unknown"}`
            : `Build ${shortRevision(revision)}`}
        </span>
      </button>

      {open && <RouteRecoveryBoundary><VersionDetails onClose={() => setOpen(false)} hasNewerBuild={hasNewerBuild} /></RouteRecoveryBoundary>}
    </>
  );
}
