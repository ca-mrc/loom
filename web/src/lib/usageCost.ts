export interface UsageCostLike {
  price_unknown_llm_calls_count?: number | null;
  total_cost_usd?: number | null;
  estimated_cost_usd?: number | null;
  cost_currency?: string | null;
  cost_status?: string | null;
  pricing_modes?: string[] | null;
  usage_reporting_status?: string | null;
  usage_estimate_confidence?: string | null;
  partial_usage_llm_calls_count?: number | null;
  missing_usage_llm_calls_count?: number | null;
}

function hasKey<T extends object>(item: T, key: PropertyKey): boolean {
  return Object.prototype.hasOwnProperty.call(item, key);
}

export function usageCostAmount(item: UsageCostLike): number | null {
  if (["price_unknown", "failed_upstream", "not_applicable", "no_usage"].includes(item.cost_status ?? "")) {
    return null;
  }
  if (hasKey(item, "estimated_cost_usd")) {
    return typeof item.estimated_cost_usd === "number"
      ? item.estimated_cost_usd
      : null;
  }
  return typeof item.total_cost_usd === "number" ? item.total_cost_usd : null;
}

export function formatUsageCost(item: UsageCostLike): string {
  const value = usageCostAmount(item);
  if (value == null) {
    if (item.usage_reporting_status === "missing" || item.missing_usage_llm_calls_count) {
      return "unknown (usage missing)";
    }
    if (item.cost_status === "price_unknown") return "unknown/unpriced";
    if (item.cost_status === "failed_upstream") return "unavailable";
    return "n/a";
  }
  const currency = item.cost_currency ?? "USD";
  const amount = currency === "USD" ? `$${value.toFixed(4)}` : `${value.toFixed(4)} ${currency}`;
  const coverage: string[] = [];
  if (item.price_unknown_llm_calls_count) coverage.push(`${item.price_unknown_llm_calls_count} unpriced calls`);
  if (item.missing_usage_llm_calls_count) coverage.push(`${item.missing_usage_llm_calls_count} missing usage`);
  if (item.partial_usage_llm_calls_count) coverage.push(`${item.partial_usage_llm_calls_count} partial usage`);
  if (item.cost_status === "mixed" || coverage.length) {
    return `${amount} known subtotal (${coverage.length ? coverage.join("; ") : "pricing incomplete"})`;
  }
  return amount;
}

/** Old trajectory snapshots use zero placeholders; the frozen pricing marker
 * distinguishes these from a priced call whose real amount is zero. */
export function formatTrajectoryCost(event: Record<string, unknown>): string {
  const marker = typeof event.rate_card_hash === "string" ? event.rate_card_hash : "";
  if (marker.startsWith("facade:tokens-only")) {
    return formatUsageCost({ estimated_cost_usd: null, cost_status: "not_applicable" });
  }
  if (marker === "failed-upstream") {
    return formatUsageCost({ estimated_cost_usd: null, cost_status: "failed_upstream" });
  }
  const amount = event.cost_usd_snapshot;
  if (!marker || marker === "facade:price-unknown" || marker.startsWith("facade:rate-card:missing")
      || marker === "facade:operator-supplied:invalid"
      || typeof amount !== "number" || !Number.isFinite(amount)) {
    return formatUsageCost({ estimated_cost_usd: null, cost_status: "price_unknown" });
  }
  return formatUsageCost({ estimated_cost_usd: amount });
}

export function usageCostStatus(item: UsageCostLike): string {
  if (typeof item.cost_status === "string" && item.cost_status) {
    return item.cost_status;
  }
  return usageCostAmount(item) != null ? "estimated" : "unknown";
}

export function usageEstimateConfidence(item: UsageCostLike): string {
  if (
    typeof item.usage_estimate_confidence === "string"
    && item.usage_estimate_confidence
  ) {
    return item.usage_estimate_confidence;
  }
  if (
    typeof item.usage_reporting_status === "string"
    && item.usage_reporting_status
  ) {
    return item.usage_reporting_status;
  }
  return "unknown";
}

export function summarizeUsageCost(items: UsageCostLike[]): UsageCostLike {
  let estimatedCost = 0;
  let hasEstimatedCost = false;
  const statuses = new Set<string>();
  const pricingModes = new Set<string>();
  const confidences = new Set<string>();
  let partialUsageCalls = 0;
  let missingUsageCalls = 0;
  let unpricedCalls = 0;

  for (const item of items) {
    const amount = usageCostAmount(item);
    if (amount != null) {
      estimatedCost += amount;
      hasEstimatedCost = true;
    }
    const status = item.cost_status;
    if (typeof status === "string" && status) statuses.add(status);
    for (const mode of item.pricing_modes ?? []) {
      pricingModes.add(mode);
    }
    const confidence = item.usage_estimate_confidence;
    if (typeof confidence === "string" && confidence) {
      confidences.add(confidence);
    }
    partialUsageCalls += item.partial_usage_llm_calls_count ?? 0;
    missingUsageCalls += item.missing_usage_llm_calls_count ?? 0;
    unpricedCalls += item.price_unknown_llm_calls_count ?? 0;
  }

  let costStatus = "unknown";
  if (statuses.size === 1) {
    costStatus = [...statuses][0];
  } else if (statuses.size > 1) {
    costStatus = "mixed";
  } else if (hasEstimatedCost) {
    costStatus = "estimated";
  }

  let usageConfidence = "unknown";
  if (confidences.has("missing")) {
    usageConfidence = "missing";
  } else if (confidences.has("partial") || confidences.size > 1) {
    usageConfidence = "partial";
  } else if (confidences.size === 1) {
    usageConfidence = [...confidences][0];
  }

  return {
    estimated_cost_usd: hasEstimatedCost ? estimatedCost : null,
    cost_currency: hasEstimatedCost ? "USD" : null,
    cost_status: costStatus,
    pricing_modes: [...pricingModes],
    usage_estimate_confidence: usageConfidence,
    partial_usage_llm_calls_count: partialUsageCalls,
    missing_usage_llm_calls_count: missingUsageCalls,
    price_unknown_llm_calls_count: unpricedCalls,
  };
}
