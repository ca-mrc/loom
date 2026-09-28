/** A proof lives only in memory, never query params, storage or error details. */

let proof: string | null = null;
const FAILURE = "Sign-in unavailable. Request a fresh browser login from your management server.";

export function captureManagedLoginProof(
  location: Pick<Location, "pathname" | "protocol" | "hash"> = window.location,
  history: Pick<History, "state" | "replaceState"> = window.history,
): void {
  if (location.pathname !== "/auth/managed") return;
  const fragment = location.hash;
  proof = null;
  // Runs in the entrypoint before mounting the app or any asynchronous work.
  // Remove any query as well: query-based login proofs are never supported.
  history.replaceState(history.state, "", location.pathname);
  if (location.protocol !== "https:") return;
  const values = new URLSearchParams(fragment.replace(/^#/, ""));
  const token = values.get("token");
  if ([...values].length === 1 && token && /^loom_(?:env_login_|app_login_[0-9a-f]{32}_)[A-Za-z0-9_-]{43}$/.test(token)) {
    proof = token;
  }
}

export function hasManagedLoginProof(): boolean {
  return proof !== null;
}

export async function completeManagedLogin(loginComplete: (token: string) => Promise<void>): Promise<void> {
  const token = proof;
  proof = null;
  if (!token) throw new Error(FAILURE);
  try {
    await loginComplete(token);
  } catch {
    // Never propagate an error that may contain an echoed proof/request body.
    throw new Error(FAILURE);
  }
}
