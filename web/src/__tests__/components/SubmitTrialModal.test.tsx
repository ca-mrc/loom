/**
 * SubmitTrialModal posts the agreed-on body shape to /api/v1/trials:
 *   { task_id, config: { agent_name, agent_model: {provider,name} | null } }
 *
 * Plan 25: the agent + model values come from server-side catalogs
 * (`/agents`, `/models`) presented as dropdowns. These tests mock
 * both catalog endpoints + the trial submit POST.
 */
import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { SubmitTrialModal } from "../../components/SubmitTrialModal";
import type { FetchMock } from "../../test-utils/fetchMock";
import { renderWithProviders } from "../../test-utils/renderWithProviders";

const AGENTS_RESPONSE = {
  items: [
    {
      name: "oracle",
      needs_model: false,
      kind: "builtin",
      description: "Runs solution/solve.sh as ground truth.",
      supported_providers: ["*"],
      supported_model_sources: [],
    },
    {
      name: "claude-code",
      needs_model: true,
      kind: "adapter",
      description: "Claude Code CLI via loom-launcher.",
      supported_providers: ["anthropic"],
      supported_model_sources: ["api"],
    },
    {
      name: "openhands-sdk",
      aliases: ["openhands"],
      display_name: "openhands",
      needs_model: true,
      kind: "adapter",
      description: "OpenHands SDK runner.",
      supported_providers: ["*"],
      supported_model_sources: ["api"],
      product_support: "supported",
    },
    {
      name: "swe-agent",
      needs_model: true,
      kind: "adapter",
      description: "SWE-agent via loom-launcher.",
      supported_providers: ["*"],
      supported_model_sources: ["api"],
      product_support: "deferred",
      deferred_reason: "pending a future product decision (#2054)",
    },
  ],
};

const MODELS_RESPONSE = {
  items: [
    {
      provider: "anthropic",
      name: "claude-opus-4-7",
      provider_connection_id: "33333333-3333-4333-8333-333333333333",
      provider_connection_name: "Anthropic prod",
      provider_connection_type: "openai-compatible",
      source: "discovered",
      agent_capable: true,
      recommended: true,
      visibility: "default",
      hidden_reason: null,
      last_seen_at: "2026-06-16T00:00:00Z",
    },
  ],
};

const PROVIDER_CONNECTIONS_RESPONSE = {
  items: [
    {
      id: "33333333-3333-4333-8333-333333333333",
      name: "Anthropic prod",
      type: "openai-compatible",
      status: "valid",
      rate_card_provider: "anthropic",
    },
    {
      id: "44444444-4444-4444-8444-444444444444",
      name: "Native Anthropic",
      type: "anthropic",
      status: "valid",
      rate_card_provider: "anthropic",
    },
  ],
};

function mockEndpoints(): FetchMock {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = typeof input === "string" ? input : String(input);
      if (url.includes("/api/v1/agents")) {
        return Promise.resolve(
          new Response(JSON.stringify(AGENTS_RESPONSE), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/models")) {
        return Promise.resolve(
          new Response(JSON.stringify(MODELS_RESPONSE), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/provider-connections")) {
        return Promise.resolve(
          new Response(JSON.stringify(PROVIDER_CONNECTIONS_RESPONSE), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      }
      if (url.includes("/api/v1/trials")) {
        return Promise.resolve(
          new Response(
            JSON.stringify({
              trial_id: "00000000-0000-0000-0000-000000000abc",
              state: "submitted",
            }),
            { status: 201, headers: { "Content-Type": "application/json" } },
          ),
        );
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function trialSubmitCall(
  spy: FetchMock,
): { url: string; body: unknown } | null {
  const found = spy.mock.calls.find(([u]) =>
    String(u).includes("/api/v1/trials"),
  );
  if (!found) return null;
  return {
    url: String(found[0]),
    body: JSON.parse((found[1] as RequestInit).body as string),
  };
}

describe("SubmitTrialModal", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.localStorage.setItem("loom_token", "test-token");
    vi.restoreAllMocks();
  });

  it("defaults to the first agent in the catalog (oracle) and sends agent_model:null", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(
      <SubmitTrialModal taskId="humaneval-0" open onClose={() => undefined} />,
    );
    // Wait for the picker to populate with the first agent + show its description.
    await screen.findByText(/Runs solution\/solve.sh/i);
    await user.click(screen.getByRole("button", { name: /submit trial/i }));

    await vi.waitFor(() => {
      const call = trialSubmitCall(spy);
      expect(call).not.toBeNull();
    });
    const call = trialSubmitCall(spy)!;
    expect(call.body).toEqual({
      task_id: "humaneval-0",
      config: { agent_name: "oracle", agent_model: null },
    });
  });

  it("switches to a needs-model agent and sends {provider,name}", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(
      <SubmitTrialModal taskId="humaneval-0" open onClose={() => undefined} />,
    );
    await screen.findByText(/Runs solution\/solve.sh/i);
    const dropdowns = await screen.findAllByRole("combobox");
    // First combobox = Agent.
    await user.selectOptions(dropdowns[0], "claude-code");
    // After the agent change, a Model combobox appears. Wait for it.
    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      "33333333-3333-4333-8333-333333333333",
    );
    await user.selectOptions(
      await screen.findByLabelText(/^Model$/i),
      "anthropic|claude-opus-4-7|33333333-3333-4333-8333-333333333333",
    );
    await user.click(screen.getByRole("button", { name: /submit trial/i }));
    await vi.waitFor(() => expect(trialSubmitCall(spy)).not.toBeNull());
    const call = trialSubmitCall(spy)!;
    expect(call.body).toMatchObject({
      task_id: "humaneval-0",
      config: {
        agent_name: "claude-code",
        agent_model: {
          provider: "anthropic",
          name: "claude-opus-4-7",
          source: "api",
        },
      },
      provider_connection_id: "33333333-3333-4333-8333-333333333333",
      provider_model_id: "claude-opus-4-7",
    });
  });

  it("hides deferred agents and native provider connections (#2054)", async () => {
    mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(
      <SubmitTrialModal taskId="humaneval-0" open onClose={() => undefined} />,
    );
    await screen.findByText(/Runs solution\/solve.sh/i);
    const dropdowns = await screen.findAllByRole("combobox");
    const agentOptions = Array.from((dropdowns[0] as HTMLSelectElement).options).map((o) => o.value);
    expect(agentOptions).not.toContain("swe-agent");
    const openhands = Array.from((dropdowns[0] as HTMLSelectElement).options).find(
      (o) => o.value === "openhands-sdk",
    );
    expect(openhands?.textContent).toBe("openhands");
    await user.selectOptions(dropdowns[0], "claude-code");
    const connection = (await screen.findByLabelText(/^Provider connection$/i)) as HTMLSelectElement;
    const connectionOptions = Array.from(connection.options).map((o) => o.value);
    expect(connectionOptions).toContain("33333333-3333-4333-8333-333333333333");
    expect(connectionOptions).not.toContain("44444444-4444-4444-8444-444444444444");
  });

  it("shows an error when the selected agent needs a model and none is picked", async () => {
    const spy = mockEndpoints();
    const user = userEvent.setup();
    renderWithProviders(
      <SubmitTrialModal taskId="humaneval-0" open onClose={() => undefined} />,
    );
    await screen.findByText(/Runs solution\/solve.sh/i);
    const dropdowns = await screen.findAllByRole("combobox");
    await user.selectOptions(dropdowns[0], "claude-code");
    // Don't pick a model; click submit.
    await user.click(screen.getByRole("button", { name: /submit trial/i }));
    expect(
      await screen.findByText(/needs a model/i),
    ).toBeInTheDocument();
    expect(trialSubmitCall(spy)).toBeNull();
  });

  it("displays the task id in the body copy", async () => {
    mockEndpoints();
    renderWithProviders(
      <SubmitTrialModal
        taskId="humaneval/HumanEval/0"
        open
        onClose={() => undefined}
      />,
    );
    expect(
      await screen.findByText(/humaneval\/HumanEval\/0/),
    ).toBeInTheDocument();
  });
});
