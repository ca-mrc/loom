import { commitUrl, LOADED_BUILD_INFO } from "../lib/buildInfo";
import { useBackendVersion } from "../lib/backendVersion";
import { Button } from "./Button";
import { CopyableId } from "./CopyableId";
import { Modal } from "./Modal";

function displayValue(value: string | null): string {
  return value ?? "unknown";
}

function PersonalSource({ digest, base }: { digest: string | null; base: string | null }): JSX.Element {
  return <>
    <div className="flex items-center justify-between gap-2">
      <dt className="text-slate-500">Build kind</dt>
      <dd className="text-xs text-slate-700">Personal source — not CI-approved</dd>
    </div>
    <div className="flex items-center justify-between gap-2">
      <dt className="text-slate-500">Source digest</dt>
      <dd>{digest ? <CopyableId value={digest} chars={19} /> : "unknown"}</dd>
    </div>
    <div className="flex items-center justify-between gap-2">
      <dt className="text-slate-500">Base commit (informational)</dt>
      <dd>{base ? <CopyableId value={base} chars={12} /> : "unknown"}</dd>
    </div>
  </>;
}

export default function VersionDetails({ onClose, hasNewerBuild }: {
  onClose: () => void;
  hasNewerBuild: boolean;
}): JSX.Element {
  const backend = useBackendVersion();
  const revision = LOADED_BUILD_INFO.revision;
  const href = commitUrl(revision);
  const backendRevision = backend.data?.buildRevision ?? null;
  return (
      <Modal
        open
        onClose={onClose}
        title="Deployed version"
        size="sm"
      >
        <div className="space-y-4 text-sm">
          {hasNewerBuild ? (
            <div
              role="status"
              className="rounded-md border border-accent/30 bg-accent/5 px-3 py-2 text-xs"
            >
              <p className="font-medium text-slate-900">
                A newer frontend build is available.
              </p>
              <p className="mt-1 text-slate-600">
                This page keeps running the build it already loaded. Refresh
                to load the new one — nothing reloads automatically, so any
                unsaved input stays put until you choose to.
              </p>
              <Button
                className="mt-2"
                size="sm"
                onClick={() => window.location.reload()}
              >
                Refresh
              </Button>
            </div>
          ) : null}

          <section aria-label="Frontend build">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-600">
              Frontend (this page)
            </h3>
            <dl className="mt-1 space-y-1">
              {LOADED_BUILD_INFO.kind === "personal" ? <PersonalSource digest={LOADED_BUILD_INFO.sourceDigest} base={LOADED_BUILD_INFO.baseCommit} /> : <div className="flex items-center justify-between gap-2">
                <dt className="text-slate-500">Commit</dt>
                <dd className="flex items-center gap-2">
                  {revision ? (
                    <>
                      <CopyableId value={revision} chars={12} />
                      {href ? (
                        <a
                          href={href}
                          target="_blank"
                          rel="noreferrer"
                          className="text-xs text-accent hover:underline"
                        >
                          View commit
                        </a>
                      ) : null}
                    </>
                  ) : (
                    <span className="text-xs text-slate-500">
                      local / unknown
                    </span>
                  )}
                </dd>
              </div>}
              <div className="flex items-center justify-between gap-2">
                <dt className="text-slate-500">Source ref</dt>
                <dd className="font-mono text-xs text-slate-700">
                  {displayValue(LOADED_BUILD_INFO.sourceRef)}
                </dd>
              </div>
              <div className="flex items-center justify-between gap-2">
                <dt className="text-slate-500">Built</dt>
                <dd className="text-xs text-slate-700">
                  {displayValue(LOADED_BUILD_INFO.buildTime)}
                </dd>
              </div>
            </dl>
          </section>

          <section aria-label="Backend build">
            <h3 className="text-xs font-semibold uppercase tracking-wider text-slate-600">
              Backend (responding instance)
            </h3>
            <p className="mt-1 text-xs text-slate-500">
              One reply is evidence for that instance only, not proof every
              replica has finished rolling out.
            </p>
            <dl className="mt-1 space-y-1">
              {backend.data?.buildKind === "personal" ? <PersonalSource digest={backend.data.sourceDigest ?? null} base={backend.data.sourceBaseCommit ?? null} /> : <div className="flex items-center justify-between gap-2">
                <dt className="text-slate-500">Commit</dt>
                <dd>
                  {backendRevision ? (
                    <CopyableId value={backendRevision} chars={12} />
                  ) : (
                    <span className="text-xs text-slate-500">unknown</span>
                  )}
                </dd>
              </div>}
              <div className="flex items-center justify-between gap-2">
                <dt className="text-slate-500">Built</dt>
                <dd className="text-xs text-slate-700">
                  {displayValue(backend.data?.buildTime ?? null)}
                </dd>
              </div>
            </dl>
          </section>
        </div>
      </Modal>
  );
}
