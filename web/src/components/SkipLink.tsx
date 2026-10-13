export interface SkipLinkProps {
  targetId?: string;
}

export function SkipLink({ targetId = "main-content" }: SkipLinkProps): JSX.Element {
  return (
    <a
      href={`#${targetId}`}
      onClick={(event) => {
        const main = document.getElementById(targetId);
        if (main) {
          event.preventDefault();
          main.focus();
          main.scrollIntoView({ block: "start" });
        }
      }}
      className="fixed left-4 top-4 z-[100] -translate-y-24 rounded-md bg-slate-900 px-4 py-2 text-sm font-semibold text-white shadow-lg focus:translate-y-0 focus:outline-hidden focus:ring-2 focus:ring-accent focus:ring-offset-2"
    >
      Skip to main content
    </a>
  );
}
