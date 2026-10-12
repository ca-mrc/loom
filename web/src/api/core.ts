import { getApiBase } from "../lib/frontendConfig";

export type ApiError = { status: number; detail: string };

export type AuthSessionLoadFailureKind = "unauthorized" | "network" | "http" | "invalid";

export class AuthSessionLoadError extends Error {
  readonly kind: AuthSessionLoadFailureKind;

  constructor(kind: AuthSessionLoadFailureKind) {
    const message =
      kind === "unauthorized"
        ? "browser session is signed out"
        : kind === "network"
          ? "browser session request failed"
          : kind === "http"
            ? "browser session returned an unsuccessful status"
            : "browser session response is invalid";
    super(message);
    this.name = "AuthSessionLoadError";
    this.kind = kind;
  }
}

export let _onUnauthorized: () => void = () => {};
let authorizationGeneration = 0;

/** Retire responses belonging to the session that was just replaced. */
export function advanceAuthorizationGeneration(): void {
  authorizationGeneration += 1;
}

export function captureUnauthorizedHandler(): () => void {
  const generation = authorizationGeneration;
  const handler = _onUnauthorized;
  return () => {
    if (generation === authorizationGeneration) handler();
  };
}

export function setUnauthorizedHandler(cb: () => void): void {
  _onUnauthorized = cb;
}

export let _csrfToken: string | null = null;

export function setCsrfToken(token: string | null): void {
  _csrfToken = token;
}

export function apiBase(): string {
  return getApiBase();
}

export function isUnsafeMethod(method?: string): boolean {
  const m = (method ?? "GET").toUpperCase();
  return !["GET", "HEAD", "OPTIONS"].includes(m);
}

export function authHeaders(initHeaders?: RequestInit["headers"], method?: string): Record<string, string> {
  const headers: Record<string, string> = {
    ...(initHeaders as Record<string, string> | undefined),
  };
  if (isUnsafeMethod(method) && !("X-Loom-CSRF" in headers)) {
    if (_csrfToken) headers["X-Loom-CSRF"] = _csrfToken;
  }
  return headers;
}

export async function throwIfApiError(resp: Response, onUnauthorized: () => void): Promise<void> {
  if (resp.status === 401) {
    onUnauthorized();
    throw { status: 401, detail: "unauthorized" } satisfies ApiError;
  }
  if (!resp.ok) {
    let detail = await resp.text();
    try {
      const parsed: unknown = JSON.parse(detail);
      if (typeof parsed === "object" && parsed !== null && "detail" in parsed) {
        const d = (parsed as { detail: unknown }).detail;
        if (typeof d === "string") detail = d;
        else if (
          typeof d === "object" &&
          d !== null &&
          "message" in d &&
          typeof (d as { message: unknown }).message === "string"
        ) {
          // Structured FastAPI detail ({code, message, ...}) — prefer
          // the human message for UI/CLI surfaces (#918).
          detail = (d as { message: string }).message;
        } else {
          detail = JSON.stringify(d);
        }
      }
    } catch {
      /* keep raw text */
    }
    throw { status: resp.status, detail } satisfies ApiError;
  }
}

export async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const onUnauthorized = captureUnauthorizedHandler();
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...authHeaders(init.headers, init.method),
  };

  const resp = await fetch(`${apiBase()}${path}`, {
    ...init,
    headers,
    credentials: "include",
  });

  await throwIfApiError(resp, onUnauthorized);
  if (resp.status === 204) return undefined as T;
  return (await resp.json()) as T;
}

export async function apiUpload<T>(path: string, formData: FormData): Promise<T> {
  const onUnauthorized = captureUnauthorizedHandler();
  const headers: Record<string, string> = authHeaders(undefined, "POST");

  const resp = await fetch(`${apiBase()}${path}`, {
    method: "POST",
    headers,
    body: formData,
    credentials: "include",
  });

  await throwIfApiError(resp, onUnauthorized);
  if (resp.status === 204) return undefined as T;
  return (await resp.json()) as T;
}

export async function apiDownload(path: string, filename: string): Promise<void> {
  const onUnauthorized = captureUnauthorizedHandler();
  const resp = await fetch(`${apiBase()}${path}`, {
    headers: authHeaders(),
    credentials: "include",
  });

  await throwIfApiError(resp, onUnauthorized);

  const blob = await resp.blob();
  const objectUrl = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = objectUrl;
  link.download = filename;
  link.rel = "noreferrer";
  document.body.appendChild(link);
  try {
    link.click();
  } finally {
    link.remove();
    URL.revokeObjectURL(objectUrl);
  }
}

export function qs(params: Record<string, string | number | boolean | undefined>): string {
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(params)) {
    if (v !== undefined && v !== "") out[k] = String(v);
  }
  const s = new URLSearchParams(out).toString();
  return s ? `?${s}` : "";
}
