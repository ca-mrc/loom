import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api } from "../../api";
import type { AlternateProviderRoute, ProviderWireRoute } from "../../api/providers";
import { queryKeys } from "../../api/queryKeys";
import { Button } from "../Button";
import { Card } from "../Card";

type Props = {
  id: string;
  type: string;
  baseUrl: string;
  route?: AlternateProviderRoute | null;
};

export default function ProviderRoutesCard({ id, type, baseUrl, route }: Props): JSX.Element | null {
  const qc = useQueryClient();
  const [editedUrl, setEditedUrl] = useState<string>();
  const [model, setModel] = useState("");
  const protocol: ProviderWireRoute = type === "anthropic" ? "openai_chat" : "anthropic_messages";
  const url = editedUrl ?? route?.base_url ?? baseUrl;
  const routeLabel = protocol === "openai_chat" ? "OpenAI Chat" : "Anthropic Messages";
  const invalidate = () => {
    void qc.invalidateQueries({ queryKey: queryKeys["providers"](id), exact: true });
    void qc.invalidateQueries({ queryKey: queryKeys["provider-connections"]() });
  };
  const save = useMutation({
    mutationFn: () => api.setProviderRoute(id, protocol, url.trim()),
    onSuccess: () => { setEditedUrl(undefined); invalidate(); },
  });
  const remove = useMutation({
    mutationFn: () => api.deleteProviderRoute(id, protocol),
    onSuccess: () => { test.reset(); setEditedUrl(undefined); invalidate(); },
  });
  const test = useMutation({
    mutationFn: () => api.testProviderRoute(id, protocol, model.trim()),
    onSuccess: invalidate,
  });
  if (!["anthropic", "openai-compatible", "custom"].includes(type)) return null;
  const pending = save.isPending || remove.isPending || test.isPending;
  return (
    <Card>
      <Card.Body className="stack-y-3">
        <h2 className="text-lg font-semibold">Wire routes</h2>
        <p>Primary route: {type === "anthropic" ? "Anthropic Messages" : "OpenAI Chat"}</p>
        <p className="text-sm text-slate-600">The alternate route uses the same origin and API key. Test each model before using it.</p>
        <label className="block">
          {routeLabel} base URL
          <input className="ml-2 rounded border px-2 py-1" type="url" value={url}
            onChange={(event) => setEditedUrl(event.target.value)} />
        </label>
        <Button disabled={pending || !url.trim()} onClick={() => { test.reset(); save.mutate(); }}>Save alternate route</Button>
        {route && (
          <>
            <label className="block">
              Model ID to verify
              <input className="ml-2 rounded border px-2 py-1" value={model}
                onChange={(event) => setModel(event.target.value)} />
            </label>
            <div className="flex gap-2">
              <Button disabled={pending || !model.trim() || url !== route.base_url} onClick={() => test.mutate()}>Test alternate route</Button>
              <Button disabled={pending} onClick={() => remove.mutate()}>Remove alternate route</Button>
            </div>
            {Object.entries(route.verified_models).length > 0 && (
              <ul aria-label="Alternate route model checks">
                {Object.entries(route.verified_models).map(([name, valid]) => (
                  <li key={name}>{name} · {valid ? "Valid" : "Failed"}</li>
                ))}
              </ul>
            )}
          </>
        )}
        {test.data && <p role="status">{test.data.status === "valid" ? "Route test passed" : `Route test failed (${test.data.error_code ?? "upstream error"})`}</p>}
        {(save.error || remove.error || test.error) && (
          <p role="alert">{String(((save.error || remove.error || test.error) as { detail?: string })?.detail ?? "Could not manage the alternate route")}</p>
        )}
      </Card.Body>
    </Card>
  );
}
