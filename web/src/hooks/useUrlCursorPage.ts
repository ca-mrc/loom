import { useSearchParams } from "react-router-dom";
import { initialPage, nextPage, prevPage, type PageState } from "../components/paginationState";

export interface CursorPageController {
  state: PageState;
  cursor: string | null;
  next: (cursor: string) => void;
  prev: () => void;
  reset: () => void;
}

/** Remove page state whenever a list's effective filters change. */
export function clearCursorParams(params: URLSearchParams): void {
  params.delete("cursor");
  params.delete("cursor_history");
}

/** Keep list position in the URL so reload, history and detail returns agree. */
export function useUrlCursorPage(): CursorPageController {
  const [params, setParams] = useSearchParams();
  let stack: (string | null)[] = [];
  try {
    const parsed: unknown = JSON.parse(params.get("cursor_history") ?? "[]");
    if (Array.isArray(parsed) && parsed.every((value) => value === null || typeof value === "string")) stack = parsed;
  } catch { /* A malformed history does not prevent opening a valid cursor. */ }
  const current = params.get("cursor");
  const state: PageState = current ? { current, stack: stack.length ? stack : [null] } : initialPage;
  function update(next: PageState): void {
    const values = new URLSearchParams(params);
    clearCursorParams(values);
    if (next.current) values.set("cursor", next.current);
    if (next.stack.length) values.set("cursor_history", JSON.stringify(next.stack));
    setParams(values);
  }
  return { state, cursor: state.current, next: (cursor) => update(nextPage(state, cursor)), prev: () => update(prevPage(state)), reset: () => update(initialPage) };
}
