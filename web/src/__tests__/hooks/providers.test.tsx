import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  useCreateConnection,
  useRefreshModels,
  useAddManualModel,
  useHideModel,
  useUnhideModel,
  useDeleteConnection,
  useEditConnection,
  useRotateConnectionKey,
  useTestConnection,
} from "../../hooks/providers";

function wrap(qc: QueryClient) {
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return <QueryClientProvider client={qc}>{children}</QueryClientProvider>;
  };
}

describe("providers hooks", () => {
  let qc: QueryClient;

  beforeEach(() => {
    qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    window.localStorage.setItem("loom_token", "t");
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(
      new Response(JSON.stringify({}), { status: 200 }),
    ));
  });

  afterEach(() => vi.restoreAllMocks());

  it("makes a newly created connection discoverable by submission forms", async () => {
    qc.setQueryData(["provider-connections", "team-1"], { items: [] });
    qc.setQueryData(["models", "default"], { items: [] });
    const { result } = renderHook(() => useCreateConnection(), { wrapper: wrap(qc) });
    await act(async () => {
      await result.current.mutateAsync({ name: "new", type: "custom", base_url: "https://example.test/v1", api_key: "test-key" });
    });
    expect(qc.getQueryState(["provider-connections", "team-1"])?.isInvalidated).toBe(true);
    expect(qc.getQueryState(["models", "default"])?.isInvalidated).toBe(true);
  });

  it.each([
    ["refresh", () => { const mutation = useRefreshModels("abc"); return () => mutation.mutateAsync(); }],
    ["add", () => { const mutation = useAddManualModel("abc"); return () => mutation.mutateAsync({ model_id: "new" }); }],
    ["hide", () => { const mutation = useHideModel("abc"); return () => mutation.mutateAsync("old"); }],
    ["unhide", () => { const mutation = useUnhideModel("abc"); return () => mutation.mutateAsync("old"); }],
  ])("refreshes default and raw submission catalogs after model %s", async (_name, useMutationAction) => {
    qc.setQueryData(["models", "default"], { items: [] });
    qc.setQueryData(["models", "raw"], { items: [] });
    const { result } = renderHook<() => Promise<unknown>, void>(useMutationAction, { wrapper: wrap(qc) });
    await act(async () => { await result.current(); });
    expect(qc.getQueryState(["models", "default"])?.isInvalidated).toBe(true);
    expect(qc.getQueryState(["models", "raw"])?.isInvalidated).toBe(true);
  });

  it("useEditConnection PATCHes + invalidates providers + detail", async () => {
    qc.setQueryData(["providers"], [{ id: "abc" }]);
    qc.setQueryData(["providers", "abc"], { id: "abc", name: "old" });
    const { result } = renderHook(() => useEditConnection("abc"), {
      wrapper: wrap(qc),
    });
    await act(async () => {
      await result.current.mutateAsync({ allowed_models: ["m1"] });
    });
    expect(qc.getQueryState(["providers"])?.isInvalidated).toBe(true);
    expect(qc.getQueryState(["providers", "abc"])?.isInvalidated).toBe(true);
  });

  it("useRotateConnectionKey PATCHes with api_key + invalidates", async () => {
    qc.setQueryData(["providers", "abc"], { id: "abc" });
    const { result } = renderHook(() => useRotateConnectionKey("abc"), {
      wrapper: wrap(qc),
    });
    await act(async () => {
      await result.current.mutateAsync("new-key");
    });
    const fetchMock = globalThis.fetch as ReturnType<typeof vi.fn>;
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/provider-connections/abc",
      expect.objectContaining({
        method: "PATCH",
        body: JSON.stringify({ api_key: "new-key" }),
      }),
    );
  });

  it("useDeleteConnection DELETEs + removes detail from cache", async () => {
    qc.setQueryData(["providers", "abc"], { id: "abc" });
    qc.setQueryData(["providers"], [{ id: "abc" }]);
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      new Response(null, { status: 204 }),
    );
    const { result } = renderHook(() => useDeleteConnection(), {
      wrapper: wrap(qc),
    });
    await act(async () => {
      await result.current.mutateAsync("abc");
    });
    expect(qc.getQueryData(["providers", "abc"])).toBeUndefined();
    expect(qc.getQueryState(["providers"])?.isInvalidated).toBe(true);
  });

  it("useTestConnection POSTs + invalidates list and detail (status pill on both)", async () => {
    qc.setQueryData(["providers"], [{ id: "abc", status: "untested" }]);
    qc.setQueryData(["providers", "abc"], { id: "abc", status: "untested" });
    (globalThis.fetch as ReturnType<typeof vi.fn>).mockResolvedValueOnce(
      new Response(JSON.stringify({ status: "valid" }), { status: 200 }),
    );
    qc.setQueryData(["provider-connections", "team-1"], { items: [] });
    qc.setQueryData(["models", "default"], { items: [] });
    const { result } = renderHook(() => useTestConnection("abc"), {
      wrapper: wrap(qc),
    });
    await act(async () => {
      await result.current.mutateAsync();
    });
    expect(qc.getQueryState(["providers"])?.isInvalidated).toBe(true);
    expect(qc.getQueryState(["providers", "abc"])?.isInvalidated).toBe(true);
    expect(qc.getQueryState(["provider-connections", "team-1"])?.isInvalidated).toBe(true);
    expect(qc.getQueryState(["models", "default"])?.isInvalidated).toBe(true);
  });
});
