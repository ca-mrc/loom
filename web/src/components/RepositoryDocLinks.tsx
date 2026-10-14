import { REPOSITORY_DOCS, repositoryDocUrl, repositoryDocsVersion, type RepositoryDocId } from "../lib/repositoryDocs";

export function RepositoryDocLinks({ docs }: { docs: readonly RepositoryDocId[] }): JSX.Element {
  return (
    <div className="stack-y-2 border-t border-slate-200 pt-4">
      <p className="text-xs font-semibold uppercase tracking-wide text-slate-500">Full guide in the repository</p>
      <ul className="stack-y-2">
        {docs.map((id) => <li key={id}><a className="text-sm text-accent underline underline-offset-4" href={repositoryDocUrl(id)} target="_blank" rel="noopener noreferrer">{REPOSITORY_DOCS[id].label}<span className="sr-only"> (opens in a new tab)</span></a></li>)}
      </ul>
      <p className="text-xs text-slate-500">{repositoryDocsVersion().label}</p>
    </div>
  );
}
