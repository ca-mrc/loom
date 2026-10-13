/**
 * Tag-filter card.
 *
 * Renders one labelled checkbox-group per distinct tag key discovered
 * across the selected benchmarks. AND across keys, OR within each
 * key's value list — matches the backend resolver added in PR-2.
 * Empty value lists are skipped at submit so a mid-edit "no values
 * checked under this key" state is a no-op rather than a filter that
 * matches zero rows.
 */
export function TagFiltersCard({
  schema,
  value,
  onChange,
  loading,
}: {
  schema: { key: string; values: string[] }[];
  value: Record<string, Set<string>>;
  onChange: (
    next:
      | Record<string, Set<string>>
      | ((prev: Record<string, Set<string>>) => Record<string, Set<string>>),
  ) => void;
  loading: boolean;
}): JSX.Element | null {
  if (loading && schema.length === 0) {
    return (
      <p className="text-xs text-slate-500">Loading available tags…</p>
    );
  }
  if (schema.length === 0) return null;

  const toggle = (key: string, val: string): void => {
    onChange((prev) => {
      const next = { ...prev };
      const bucket = new Set(next[key] ?? []);
      if (bucket.has(val)) bucket.delete(val);
      else bucket.add(val);
      next[key] = bucket;
      return next;
    });
  };
  const clearKey = (key: string): void => {
    onChange((prev) => {
      const next = { ...prev };
      next[key] = new Set();
      return next;
    });
  };
  const anyActive = Object.values(value).some((s) => s.size > 0);

  return (
    <details className="rounded-lg border border-slate-200 bg-slate-50/50" open={anyActive}>
      <summary className="flex cursor-pointer items-center gap-2 px-3 py-2 text-sm font-medium text-slate-700">
        <span>Filter by tag</span>
        <span className="text-xs font-normal text-slate-500">
          {anyActive ? "active" : "narrow the slate further"}
        </span>
      </summary>
      <div className="stack-y-3 px-3 py-2">
        {schema.map(({ key, values }) => {
          const active = value[key] ?? new Set<string>();
          return (
            <div key={key}>
              <div className="mb-1 flex items-baseline gap-2">
                <span className="text-xs font-medium uppercase tracking-wider text-slate-500">
                  {key}
                </span>
                {active.size > 0 ? (
                  <button
                    type="button"
                    onClick={() => clearKey(key)}
                    title={`Clear selected ${key} tag values.`}
                    className="text-xs text-indigo-600 hover:underline"
                  >
                    clear
                  </button>
                ) : null}
              </div>
              <div className="flex flex-wrap gap-1.5">
                {values.map((v) => {
                  const on = active.has(v);
                  return (
                    <button
                      key={v}
                      type="button"
                      onClick={() => toggle(key, v)}
                      aria-pressed={on}
                      title={
                        on
                          ? `Remove ${key}=${v} from the task filter.`
                          : `Add ${key}=${v} to the task filter.`
                      }
                      className={
                        on
                          ? "rounded-md border border-indigo-500 bg-indigo-50 px-2 py-0.5 text-xs font-medium text-indigo-700"
                          : "rounded-md border border-slate-200 bg-white px-2 py-0.5 text-xs text-slate-700 hover:border-slate-300"
                      }
                    >
                      {v}
                    </button>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </details>
  );
}
