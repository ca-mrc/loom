import { useState } from "react";
import { priceCatalogApi, type ModelPrice } from "../../api/providers";
import { Button } from "../Button";
import { Input, Textarea } from "../Input";

import { priceKeys, toDraft, type PriceDraft } from "./modelPrices";
const errorText = (error: unknown) =>
  error instanceof Error
    ? error.message
    : String(
        (error as { detail?: string })?.detail ?? "Price operation failed",
      );

export default function ModelPriceEditor({
  value,
  onChange,
  availableModels = [],
}: {
  value: PriceDraft;
  onChange: (draft: PriceDraft) => void;
  availableModels?: string[];
}) {
  const [search, setSearch] = useState("");
  const [newModel, setNewModel] = useState("");
  const [selected, setSelected] = useState<string[]>([]);
  const [bulk, setBulk] = useState("");
  const [format, setFormat] = useState<"csv" | "json">("csv");
  const [preview, setPreview] = useState<Record<string, ModelPrice> | null>(
    null,
  );
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const add = () => {
    if (!newModel || /\s/.test(newModel) || newModel in value) {
      setError("Enter a new exact model ID.");
      return;
    }
    onChange({ ...value, [newModel]: ["", "", "", ""] });
    setNewModel("");
    setError("");
  };
  return (
    <div className="stack-y-3">
      <p className="text-sm">
        USD per 1M tokens. Input and output are required for each configured
        model. Blank cache prices are unknown; zero is an explicit free price.
        Unlisted models remain unpriced.
      </p>
      <Input
        aria-label="Search model prices"
        placeholder="Search model prices"
        value={search}
        onChange={(e) => setSearch(e.target.value)}
      />
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr>
              {[
                "Select",
                "Model",
                "Input",
                "Output",
                "Cache read",
                "Cache write",
                "Actions",
              ].map((h) => (
                <th key={h}>{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {Object.entries(value)
              .filter(([model]) => model.includes(search))
              .map(([model, cells]) => (
                <tr key={model}>
                  <td>
                    <input
                      type="checkbox"
                      aria-label={`Select ${model}`}
                      checked={selected.includes(model)}
                      onChange={(e) =>
                        setSelected(
                          e.target.checked
                            ? [...selected, model]
                            : selected.filter((m) => m !== model),
                        )
                      }
                    />
                  </td>
                  <td>{model}</td>
                  {cells.map((cell, i) => (
                    <td key={priceKeys[i]}>
                      <Input
                        type="number"
                        min="0"
                        step="any"
                        aria-label={`${model} ${priceKeys[i]}`}
                        value={cell}
                        onChange={(e) =>
                          onChange({
                            ...value,
                            [model]: cells.map((v, n) =>
                              n === i ? e.target.value : v,
                            ),
                          })
                        }
                      />
                    </td>
                  ))}
                  <td>
                    <Button
                      type="button"
                      onClick={() => {
                        const next = { ...value };
                        delete next[model];
                        onChange(next);
                        setSelected(selected.filter((m) => m !== model));
                      }}
                    >
                      Remove
                    </Button>
                    <Button
                      type="button"
                      disabled={!selected.length}
                      onClick={() =>
                        onChange({
                          ...value,
                          ...Object.fromEntries(
                            selected
                              .filter((m) => m in value)
                              .map((m) => [m, [...cells]]),
                          ),
                        })
                      }
                    >
                      Copy to selected
                    </Button>
                  </td>
                </tr>
              ))}
          </tbody>
        </table>
      </div>
      <div className="flex gap-2">
        <Input
          list="model-price-options"
          aria-label="New price model ID"
          value={newModel}
          onChange={(e) => setNewModel(e.target.value)}
          placeholder="Exact model ID"
        />
        <datalist id="model-price-options">{availableModels.filter(model => !(model in value)).map(model => <option key={model} value={model} />)}</datalist>
        <Button type="button" onClick={add}>
          Add model price
        </Button>
      </div>
      <details>
        <summary>Import model prices (CSV / JSON)</summary>
        <p className="text-sm">
          CSV columns:
          model,input_usd_per_1m,output_usd_per_1m,cache_read_usd_per_1m,cache_write_usd_per_1m.
          JSON uses an array of objects with the same fields. Applying an import
          replaces all custom rows.
        </p>
        <select
          aria-label="Price import format"
          value={format}
          onChange={(e) => {
            setFormat(e.target.value as "csv" | "json");
            setPreview(null);
          }}
        >
          <option>csv</option>
          <option>json</option>
        </select>
        <input
          type="file"
          accept=".csv,.json"
          aria-label="Price import file"
          onChange={async (e) => {
            const file = e.target.files?.[0];
            if (file) {
              setBulk(await file.text());
              setFormat(file.name.endsWith(".csv") ? "csv" : "json");
              setPreview(null);
            }
          }}
        />
        <Textarea
          aria-label="Price import content"
          value={bulk}
          onChange={(e) => {
            setBulk(e.target.value);
            setPreview(null);
          }}
        />
        <Button
          type="button"
          disabled={busy}
          onClick={async () => {
            setBusy(true);
            setError("");
            try {
              setPreview((await priceCatalogApi.preview(bulk, format)).prices);
            } catch (e) {
              setPreview(null);
              setError(errorText(e));
            } finally {
              setBusy(false);
            }
          }}
        >
          Preview import
        </Button>
        {preview && (
          <div>
            <p>
              {Object.keys(preview).length} model rows will replace{" "}
              {Object.keys(value).length} existing rows.
            </p>
            <Button
              type="button"
              onClick={() => {
                onChange(toDraft(preview));
                setPreview(null);
              }}
            >
              Apply imported rows
            </Button>
          </div>
        )}
      </details>
      {error && <p role="alert">{error}</p>}
    </div>
  );
}
