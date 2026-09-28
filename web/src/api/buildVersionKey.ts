/** Shared key without importing unrelated domain cache roots at startup. */
export function buildVersionKey<const Parts extends readonly unknown[]>(...parts: Parts): readonly ["build-version", ...Parts] {
  return ["build-version", ...parts];
}
