import { useQuery, type UseQueryResult } from "@tanstack/react-query";
import { apiFetch } from "../api/core";
import { buildVersionKey } from "../api/buildVersionKey";

const STALE_TIME_MS = 60_000;

export interface BackendVersion {
  buildRevision: string | null;
  buildTime: string | null;
  buildKind?: "personal" | "commit";
  sourceDigest?: string | null;
  sourceBaseCommit?: string | null;
}

async function fetchBackendVersion(): Promise<BackendVersion | null> {
  try {
    return await apiFetch<BackendVersion>("/api/v1/version");
  } catch {
    return null;
  }
}

export function useBackendVersion(): UseQueryResult<BackendVersion | null> {
  return useQuery({
    queryKey: buildVersionKey("backend"),
    queryFn: fetchBackendVersion,
    refetchOnWindowFocus: true,
    staleTime: STALE_TIME_MS,
    retry: false,
  });
}
