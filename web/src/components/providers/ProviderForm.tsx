import { useEffect, useState } from "react";

import {
  priceCatalogApi,
  type PriceCatalog,
  type ProviderPricing,
} from "../../api/providers";
import CatalogImport from "./CatalogImport";
import ModelPriceEditor from "./ModelPriceEditor";
import { parseDraft, toDraft } from "./modelPrices";

import { Button } from "../Button";
import { Input, Textarea } from "../Input";

export type ProviderFormValues = ProviderPricing & {
  name: string;
  type: string;
  base_url: string;
  api_key?: string;
  allowed_models?: string[];
};

export type ProviderFormProps = {
  mode: "create" | "edit";
  initial?: Partial<ProviderFormValues>;
  pending?: boolean;
  discoveredModels?: string[];
  onSubmit: (values: ProviderFormValues) => void;
};

const PROVIDER_TYPES = ["openai-compatible", "anthropic", "google", "custom"];

export default function ProviderForm({
  mode,
  initial,
  pending,
  discoveredModels = [],
  onSubmit,
}: ProviderFormProps): JSX.Element {
  const [name, setName] = useState(initial?.name ?? "");
  const [type, setType] = useState(initial?.type ?? "openai-compatible");
  const [baseUrl, setBaseUrl] = useState(initial?.base_url ?? "");
  const [apiKey, setApiKey] = useState("");
  const [allowedModelsText, setAllowedModelsText] = useState(
    (initial?.allowed_models ?? []).join("\n"),
  );
  const [pricingMode, setPricingMode] = useState<
    "usage_only" | "catalog" | "custom"
  >(initial?.pricing_mode ?? "usage_only");
  const [supplier, setSupplier] = useState(initial?.supplier_id ?? "");
  const [catalogId, setCatalogId] = useState(initial?.catalog_id ?? "");
  const [prices, setPrices] = useState(toDraft(initial?.custom_pricing ?? {}));
  const [pricingTouched, setPricingTouched] = useState(mode === "create");
  const [modeChosen, setModeChosen] = useState(mode === "edit");
  const [catalogs, setCatalogs] = useState<PriceCatalog[]>([]);
  const [catalogName, setCatalogName] = useState("");
  const [error, setError] = useState("");
  useEffect(() => {
    if (pricingMode !== "catalog") return;
    let active = true;
    priceCatalogApi
      .list()
      .then((data) => {
        if (!Array.isArray(data.items))
          throw new Error("Invalid catalog response");
        if (active) setCatalogs(data.items);
      })
      .catch(() => {
        if (active)
          setError(
            "Price catalogs could not be loaded. Retry before choosing catalog pricing.",
          );
      });
    return () => {
      active = false;
    };
  }, [pricingMode]);
  const chosenCatalog = catalogs.find((c) => c.id === catalogId);
  const knownModels = [
    ...new Set([
      ...discoveredModels,
      ...allowedModelsText
        .split(/[\n,]/)
        .map((s) => s.trim())
        .filter(Boolean),
      ...Object.keys(prices),
    ]),
  ];
  const unpricedModels = knownModels.filter(
    (model) => !chosenCatalog?.prices[chosenCatalog.aliases[model] ?? model],
  );
  const reportError = (e: unknown) =>
    setError(
      e instanceof Error
        ? e.message
        : String((e as { detail?: string })?.detail ?? "Unable to save prices"),
    );

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const allowed = allowedModelsText
      .split(/[\n,]/)
      .map((s) => s.trim())
      .filter(Boolean);
    setError("");
    let pricing: ProviderPricing = {};
    if (pricingTouched) {
      try {
        if (pricingMode === "catalog" && !catalogId)
          throw new Error("Select a price catalog.");
        pricing = {
          pricing_mode: pricingMode,
          supplier_id: supplier || null,
          catalog_id: pricingMode === "catalog" ? catalogId : null,
          custom_pricing: pricingMode === "custom" ? parseDraft(prices) : null,
        };
      } catch (e) {
        reportError(e);
        return;
      }
    }
    const values: ProviderFormValues = {
      name,
      type,
      base_url: baseUrl,
      ...(mode === "create" ? { api_key: apiKey } : {}),
      allowed_models: allowed,
      ...pricing,
    };
    onSubmit(values);
  };

  return (
    <form onSubmit={handleSubmit} className="stack-y-4" noValidate>
      <div className="stack-y-2">
        <label htmlFor="pf-supplier">Supplier</label>
        <select
          id="pf-supplier"
          value={supplier}
          onChange={(e) => {
            const next = e.target.value;
            setSupplier(next);
            setPricingTouched(true);
            if (!modeChosen) {
              setPricingMode(next ? "catalog" : "usage_only");
              setCatalogId(next ? `supplier:${next}` : "");
            }
          }}
        >
          <option value="">Other / unspecified supplier</option>
          <option value="yibuapi">YibuAPI — default group</option>
          <option value="az-gptplus5">AZ GPTPlus5 — default group</option>
        </select>
      </div>
      <div>
        <label
          htmlFor="pf-name"
          className="block text-sm font-medium text-slate-700"
        >
          Name
        </label>
        <Input
          id="pf-name"
          disabled={mode === "edit"}
          value={name}
          onChange={(e) => setName(e.target.value)}
          required
        />
      </div>
      <div>
        <label
          htmlFor="pf-type"
          className="block text-sm font-medium text-slate-700"
        >
          Type
        </label>
        <select
          id="pf-type"
          disabled={mode === "edit"}
          value={type}
          onChange={(e) => setType(e.target.value)}
          className="mt-1 block w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm shadow-xs"
        >
          {PROVIDER_TYPES.map((t) => (
            <option key={t} value={t}>
              {t}
            </option>
          ))}
        </select>
      </div>
      <div>
        <label
          htmlFor="pf-base-url"
          className="block text-sm font-medium text-slate-700"
        >
          Base URL
        </label>
        <Input
          id="pf-base-url"
          type="url"
          value={baseUrl}
          onChange={(e) => setBaseUrl(e.target.value)}
          required
          placeholder="https://api.example.com/v1"
        />
        <p className="mt-1 text-xs text-slate-500">
          For OpenAI-compatible servers, use the API root ending in /v1. For
          Anthropic or Google, use the provider's documented API root.
        </p>
      </div>
      {mode === "create" && (
        <div>
          <label
            htmlFor="pf-api-key"
            className="block text-sm font-medium text-slate-700"
          >
            API key
          </label>
          <Input
            id="pf-api-key"
            type="password"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            required
            autoComplete="off"
          />
        </div>
      )}
      <div>
        <label
          htmlFor="pf-allowed"
          className="block text-sm font-medium text-slate-700"
        >
          Allowed models (optional, one per line)
        </label>
        <Textarea
          id="pf-allowed"
          rows={4}
          value={allowedModelsText}
          onChange={(e) => setAllowedModelsText(e.target.value)}
          placeholder={`gpt-4o\nclaude-3-opus`}
        />
        <p className="mt-1 text-xs text-slate-500">
          Blank means all discovered models are allowed. Add one model ID per
          line to restrict the picker.
        </p>
      </div>
      <section className="stack-y-3 rounded-md border p-3">
        <h2 className="font-semibold">Model cost estimates</h2>
        {initial?.legacy_pricing && !pricingTouched && (
          <p role="status">
            Legacy pricing is retained. Select a mode to replace it with
            model-specific settings.{" "}
            {initial.legacy_pricing.default_model_price &&
              `Current uniform input/output prices: ${initial.legacy_pricing.default_model_price.input_usd_per_1m} / ${initial.legacy_pricing.default_model_price.output_usd_per_1m} USD per 1M tokens.`}
          </p>
        )}
        <label htmlFor="pf-pricing-mode">Pricing mode</label>
        <select
          id="pf-pricing-mode"
          value={pricingMode}
          onChange={(e) => {
            setPricingMode(e.target.value as typeof pricingMode);
            setPricingTouched(true);
            setModeChosen(true);
          }}
        >
          <option value="usage_only">Track usage only</option>
          <option value="catalog">Use a price catalog</option>
          <option value="custom">Enter custom model prices</option>
        </select>
        {pricingMode === "usage_only" && (
          <p>Token usage is recorded. Monetary cost is not applicable.</p>
        )}
        {pricingMode === "catalog" && (
          <>
            <label htmlFor="pf-catalog">Price catalog</label>
            <select
              id="pf-catalog"
              value={catalogId}
              onChange={(e) => {
                setCatalogId(e.target.value);
                setPricingTouched(true);
              }}
            >
              <option value="">Select a catalog</option>
              {catalogs.map((c) => (
                <option key={c.id} value={c.id}>
                  {c.name} {c.team_id ? "(team)" : "(public)"}
                </option>
              ))}
            </select>
            {chosenCatalog && (
              <div className="text-sm">
                <p>
                  USD per 1M tokens · Last successful update:{" "}
                  {chosenCatalog.updated_at ?? "Never"}
                </p>
                {chosenCatalog.source_url && (
                  <a
                    href={chosenCatalog.source_url}
                    target="_blank"
                    rel="noreferrer"
                  >
                    Published pricing source
                  </a>
                )}
                {chosenCatalog.stale && (
                  <p role="status">
                    Prices are stale. {chosenCatalog.sync_error}
                  </p>
                )}
                <p>
                  {knownModels.length - unpricedModels.length} matched /{" "}
                  {knownModels.length} listed models.{" "}
                  {unpricedModels.length > 0 &&
                    `Unpriced: ${unpricedModels.join(", ")}. Calls can run with unknown cost unless a hard budget requires prices.`}
                </p>
              </div>
            )}
            {chosenCatalog?.team_id && (
              <CatalogImport
                key={chosenCatalog.id}
                catalog={chosenCatalog}
                onUpdated={(next) =>
                  setCatalogs(
                    catalogs.map((c) => (c.id === next.id ? next : c)),
                  )
                }
              />
            )}
          </>
        )}
        {pricingMode === "custom" && (
          <>
            <ModelPriceEditor
              availableModels={knownModels}
              value={prices}
              onChange={(next) => {
                setPrices(next);
                setPricingTouched(true);
              }}
            />
            <div>
              <Input
                aria-label="Team catalog name"
                placeholder="Reusable team catalog name"
                value={catalogName}
                onChange={(e) => setCatalogName(e.target.value)}
              />
              <Button
                type="button"
                disabled={!catalogName.trim()}
                onClick={async () => {
                  try {
                    const catalog = await priceCatalogApi.create(
                      catalogName.trim(),
                      parseDraft(prices),
                    );
                    setCatalogs([...catalogs, catalog]);
                    setCatalogId(catalog.id);
                    setPricingMode("catalog");
                    setModeChosen(true);
                    setPricingTouched(true);
                    setError("");
                  } catch (e) {
                    reportError(e);
                  }
                }}
              >
                Save as team catalog and select it
              </Button>
            </div>
          </>
        )}
        {error && <p role="alert">{error}</p>}
      </section>
      <div className="flex justify-end">
        <Button type="submit" variant="primary" disabled={pending}>
          {mode === "create" ? "Create connection" : "Save changes"}
        </Button>
      </div>
    </form>
  );
}
