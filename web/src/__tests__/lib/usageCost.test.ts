import { describe, expect, it } from "vitest";

import { formatUsageCost, summarizeUsageCost } from "../../lib/usageCost";

describe("usage cost completeness", () => {
  it("does not turn legacy unpriced zero placeholders into free calls", () => {
    expect(formatUsageCost({ total_cost_usd: 0, cost_status: "price_unknown" }))
      .toBe("unknown/unpriced");
    expect(formatUsageCost({ total_cost_usd: 0, cost_status: "failed_upstream" }))
      .toBe("unavailable");
  });

  it("shows missing usage as unknown even with configured pricing", () => {
    expect(formatUsageCost({
      total_cost_usd: 0,
      estimated_cost_usd: null,
      cost_status: "estimated",
      usage_reporting_status: "missing",
      missing_usage_llm_calls_count: 3,
    })).toBe("unknown (usage missing)");
  });

  it("labels incomplete usage and unpriced calls as a known subtotal", () => {
    expect(formatUsageCost({
      estimated_cost_usd: 1.25,
      cost_status: "mixed",
      price_unknown_llm_calls_count: 2,
      missing_usage_llm_calls_count: 3,
      partial_usage_llm_calls_count: 1,
    })).toBe("$1.2500 known subtotal (2 unpriced calls; 3 missing usage; 1 partial usage)");
  });

  it("preserves missing usage after combining dashboard buckets", () => {
    const summary = summarizeUsageCost([
      { estimated_cost_usd: 0.5, cost_status: "estimated" },
      { estimated_cost_usd: null, cost_status: "failed_upstream", missing_usage_llm_calls_count: 1 },
    ]);
    expect(formatUsageCost(summary)).toBe("$0.5000 known subtotal (1 missing usage)");
    expect(formatUsageCost(summarizeUsageCost([
      { estimated_cost_usd: null, cost_status: "failed_upstream", missing_usage_llm_calls_count: 1 },
    ]))).toBe("unknown (usage missing)");
  });

  it("preserves a known zero price with complete usage", () => {
    expect(formatUsageCost({ estimated_cost_usd: 0, cost_status: "estimated", usage_reporting_status: "complete" }))
      .toBe("$0.0000");
  });
});
