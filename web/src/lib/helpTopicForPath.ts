import type { HelpTopicId } from "./helpContent";

export function helpTopicForPath(pathname: string): HelpTopicId {
  if (pathname.startsWith("/providers")) return "providers";
  if (/^\/(task-sets|tasks|benchmarks)/.test(pathname)) return "tasks";
  if (pathname.startsWith("/library")) return "reuse";
  if (pathname.startsWith("/pipelines")) return "pipelines";
  if (pathname.startsWith("/usage")) return "usage";
  if (pathname.startsWith("/rate-cards")) return "rates";
  if (/^\/(settings|admin|auth|invites)/.test(pathname)) return "access";
  if (pathname === "/batches/new") return "quickstart";
  if (/^\/(monitor|batches|trials)/.test(pathname)) return "results";
  return "quickstart";
}
