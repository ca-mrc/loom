import { getFrontendConfig } from "./frontendConfig";

/** Convert an untrusted return URL into a path owned by the current router. */
export function internalReturnPath(value: string | null): string | null {
  if (!value || value !== value.trim() || !value.startsWith("/") || value.startsWith("//")) return null;
  const unsafe = (text: string): boolean => Array.from(text).some((character) =>
    character === "\\" || character.charCodeAt(0) < 32 || character.charCodeAt(0) === 127,
  );
  if (unsafe(value)) return null;
  try {
    const url = new URL(value, window.location.origin);
    // Browsers normalize dot segments and backslashes before navigation.
    // Check the normalized path too, including encoded path separators.
    const decodedPath = decodeURIComponent(url.pathname);
    if (url.origin !== window.location.origin || unsafe(decodedPath)
      || decodedPath.startsWith("//") || /%2f/iu.test(url.pathname)) return null;
    const base = getFrontendConfig().routePath;
    const pathname = base && (url.pathname === base || url.pathname.startsWith(`${base}/`))
      ? url.pathname.slice(base.length) || "/"
      : url.pathname;
    if (pathname.startsWith("//")) return null;
    return `${pathname}${url.search}${url.hash}`;
  } catch {
    return null;
  }
}
