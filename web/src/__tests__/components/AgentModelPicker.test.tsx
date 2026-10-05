import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  AgentModelPicker,
  type AgentModelValue,
} from "../../components/AgentModelPicker";
import type { FetchMock } from "../../test-utils/fetchMock";

const INITIAL_VALUE: AgentModelValue = {
  agentName: "direct-completion",
  source: "api",
  modelProvider: "",
  modelName: "",
};

function mockPickerEndpoints(
  modelSources: string[] = ["api", "local-server", "hf"],
): FetchMock {
  return vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url =
        input instanceof Request
          ? input.url
          : typeof input === "string"
            ? input
            : String(input);
      const json = (body: unknown) =>
        Promise.resolve(
          new Response(JSON.stringify(body), {
            status: 200,
            headers: { "Content-Type": "application/json" },
          }),
        );
      if (url.includes("/api/v1/agents")) {
        return json({
          items: [
            {
              name: "direct-completion",
              needs_model: true,
              kind: "builtin",
              description: "Direct completion without workspace tools.",
              supported_providers: ["*"],
              supported_model_sources: modelSources,
            },
            {
              name: "terminus-2",
              versions: [
                { agent_version: "harbor-v1", harbor_version: "0.1.0", loom_bridge_revision: "1" },
                { agent_version: "harbor-v2", harbor_version: "0.2.0", loom_bridge_revision: "2" },
                {
                  agent_version: "harbor-0.18.0-nebius-44dbda72-b",
                  harbor_version: "0.18.0", loom_bridge_revision: "1.0",
                  readiness_status: "unavailable",
                  readiness_message: "This published bridge rejects sandbox instance_id; choose a compatible published version.",
                },
              ],
              needs_model: true,
              kind: "adapter",
              description: "Terminal-Bench Terminus adapter.",
              supported_providers: ["*"],
              supported_model_sources: ["api"],
            },
          ],
        });
      }
      if (url.includes("/api/v1/provider-connections")) {
        return json({
          items: [
            {
              id: "conn-1",
              name: "Lab vLLM",
              type: "openai-compatible",
              status: "valid",
              rate_card_provider: "openai",
            },
            {
              id: "conn-mz",
              name: "mz_tn_canada_qianyi",
              type: "openai-compatible",
              status: "valid",
              rate_card_provider: "openai",
            },
          ],
        });
      }
      if (url.includes("/api/v1/models")) {
        return json({
          items: [
            {
              provider: "openai",
              name: "deepseek-chat",
              provider_connection_id: "conn-1",
              provider_connection_name: "Lab vLLM",
              provider_connection_type: "openai-compatible",
              source: "discovered",
              agent_capable: true,
              recommended: true,
              visibility: "default",
              hidden_reason: null,
              last_preflight_status: "failed",
              last_preflight_http_status: 403,
              last_preflight_error_code: "access-denied",
              last_preflight_error_message: "HTTP 403 from upstream: [REDACTED]",
              last_preflight_failure_kind: "rejected",
            },
            {
              provider: "openai",
              name: "slow-reasoner",
              provider_connection_id: "conn-1",
              provider_connection_name: "Lab vLLM",
              provider_connection_type: "openai-compatible",
              source: "discovered",
              agent_capable: true,
              recommended: true,
              visibility: "default",
              hidden_reason: null,
              last_preflight_status: "failed",
              last_preflight_at: "2026-09-20T12:00:00Z",
              last_preflight_http_status: null,
              last_preflight_error_code: "timeout",
              last_preflight_error_message: "timeout after 20.0s: read timeout",
              last_preflight_failure_kind: "inconclusive",
            },
            {
              provider: "openai",
              name: "gpt-4o-mini",
              provider_connection_id: "conn-mz",
              provider_connection_name: "mz_tn_canada_qianyi",
              provider_connection_type: "openai-compatible",
              source: "discovered",
              agent_capable: true,
              recommended: true,
              visibility: "default",
              hidden_reason: null,
              last_preflight_status: null,
              last_preflight_http_status: null,
              last_preflight_error_code: null,
              last_preflight_error_message: null,
            },
          ],
        });
      }
      return Promise.reject(new Error(`unexpected fetch ${url}`));
    });
}

function Harness({ initial = INITIAL_VALUE, initialAllow = false }: { initial?: AgentModelValue; initialAllow?: boolean }): JSX.Element {
  const [value, setValue] = useState<AgentModelValue>(initial);
  const [allow, setAllow] = useState(initialAllow);
  return <>
    <button onClick={() => setAllow(false)}>Disable agent versions</button>
    <button onClick={() => setValue(JSON.parse(JSON.stringify(value)) as AgentModelValue)}>Copy picker state</button>
    <AgentModelPicker value={value} onChange={setValue} allowAgentVersion={allow} />
    <output data-testid="picker-state">{JSON.stringify(value)}</output>
  </>;
}

function renderPicker(initial?: AgentModelValue, initialAllow?: boolean): void {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={qc}>
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
        <Harness initial={initial} initialAllow={initialAllow} />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe("AgentModelPicker copy", () => {
  beforeEach(() => {
    window.localStorage.setItem("loom_token", "t");
    mockPickerEndpoints();
  });
  afterEach(() => vi.restoreAllMocks());

  it("uses operator-readable raw and manual model labels", async () => {
    const user = userEvent.setup();
    renderPicker();

    expect(
      await screen.findByRole("checkbox", {
        name: /Include hidden\/discovered models/i,
      }),
    ).toBeInTheDocument();

    await user.selectOptions(
      screen.getByLabelText(/^Provider connection$/i),
      await screen.findByRole("option", { name: /Lab vLLM/i }),
    );
    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      screen.getByRole("option", { name: /Ad-hoc model ID/i }),
    );

    expect(
      screen.getByText(/Use this for a model ID that exists on the selected provider/i),
    ).toBeInTheDocument();
  });

  it("offers only Provider Connections as the model source (#2054)", async () => {
    renderPicker();

    expect(await screen.findByLabelText(/^Provider connection$/i)).toBeInTheDocument();
    expect(screen.queryByRole("tablist", { name: "Model source" })).not.toBeInTheDocument();
    expect(screen.queryByRole("tab", { name: "Local server" })).not.toBeInTheDocument();
    expect(screen.queryByRole("tab", { name: "HuggingFace" })).not.toBeInTheDocument();
    expect(screen.getByLabelText(/^Model$/i)).toBeDisabled();
    expect(screen.queryByLabelText("Local model")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("HuggingFace model")).not.toBeInTheDocument();
    expect(vi.mocked(fetch).mock.calls.some(([input]) =>
      String(input).includes("/api/v1/local-servers"),
    )).toBe(false);
  });

  it("warns before submit when a selected provider model failed preflight", async () => {
    const user = userEvent.setup();
    renderPicker();

    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      await screen.findByRole("option", { name: /Lab vLLM/i }),
    );
    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      await screen.findByRole("option", { name: /deepseek-chat/i }),
    );

    expect(
      screen.getByText(/This model failed its last preflight/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/access-denied/i)).toBeInTheDocument();
  });

  it("keeps the selected model and its preflight warning while searching (#2054)", async () => {
    const user = userEvent.setup();
    renderPicker();

    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      await screen.findByRole("option", { name: /Lab vLLM/i }),
    );
    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      await screen.findByRole("option", { name: /deepseek-chat/i }),
    );
    await user.type(screen.getByPlaceholderText(/deepseek, qwen, llama/i), "slow");

    const model = screen.getByLabelText(/^Model$/i) as HTMLSelectElement;
    expect(model.value).toBe("openai|deepseek-chat|conn-1");
    expect(screen.getByRole("option", { name: /slow-reasoner/i })).toBeInTheDocument();
    expect(screen.getByText(/This model failed its last preflight/i)).toBeInTheDocument();
    expect(screen.queryByText(/Ad-hoc model ID$/i, { selector: "span" })).not.toBeInTheDocument();
  });

  it("shows a non-blocking notice when the last preflight was only inconclusive", async () => {
    // #948: a timed-out probe is not a rejection and must not read as one.
    const user = userEvent.setup();
    renderPicker();

    await user.selectOptions(
      await screen.findByLabelText(/^Provider connection$/i),
      await screen.findByRole("option", { name: /Lab vLLM/i }),
    );
    await user.selectOptions(
      screen.getByLabelText(/^Model$/i),
      await screen.findByRole("option", { name: /slow-reasoner \(preflight inconclusive\)/i }),
    );

    expect(
      screen.getByText(/last preflight for this model was inconclusive/i),
    ).toBeInTheDocument();
    expect(screen.getByText(/You can still submit/i)).toBeInTheDocument();
    expect(screen.queryByText(/This model failed its last preflight/i)).toBeNull();
  });

  it("keeps mz_tn_canada_qianyi models visible for terminus-2", async () => {
    const user = userEvent.setup();
    renderPicker();

    await user.selectOptions(
      await screen.findByLabelText(/^Agent$/i),
      await screen.findByRole("option", { name: "terminus-2" }),
    );
    await user.selectOptions(
      screen.getByLabelText(/^Provider connection$/i),
      await screen.findByRole("option", { name: /mz_tn_canada_qianyi/i }),
    );

    expect(
      await screen.findByRole("option", { name: /gpt-4o-mini/i }),
    ).toBeInTheDocument();
    expect(
      screen.queryByText(/No discovered models match this agent and search/i),
    ).not.toBeInTheDocument();
  });
});


describe("Harbor agent versions", () => {
  beforeEach(() => { window.localStorage.setItem("loom_token", "t"); mockPickerEndpoints(); });
  afterEach(() => vi.restoreAllMocks());

  it("disables an incompatible bridge and preserves a restored selection with its reason", async () => {
    const version = "harbor-0.18.0-nebius-44dbda72-b";
    renderPicker({ ...INITIAL_VALUE, agentName: "terminus-2", agentVersion: version }, true);
    const option = await screen.findByRole("option", { name: new RegExp(version) });
    expect(option).toBeDisabled();
    expect(screen.getByLabelText("Agent version")).toHaveValue(version);
    expect(await screen.findByRole("alert")).toHaveTextContent("rejects sandbox instance_id");
    const user = userEvent.setup();
    await user.selectOptions(screen.getByLabelText("Agent version"), "harbor-v2");
    expect(screen.getByLabelText("Agent version")).toHaveValue("harbor-v2");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("defaults to deployment version, preserves exact selection when copying, and clears on agent change", async () => {
    const user = userEvent.setup();
    renderPicker({ ...INITIAL_VALUE, agentName: "terminus-2" }, true);
    const picker = await screen.findByLabelText("Agent version");
    await screen.findByRole("option", { name: /harbor-v1 · Harbor 0.1.0 · bridge 1/ });
    expect(picker).toHaveValue("");
    expect(JSON.parse(screen.getByTestId("picker-state").textContent!)).not.toHaveProperty("agentVersion");
    await user.selectOptions(picker, "harbor-v2");
    await user.click(screen.getByText("Copy picker state"));
    expect(picker).toHaveValue("harbor-v2");
    await user.selectOptions(screen.getByLabelText("Agent"), "direct-completion");
    expect(screen.queryByLabelText("Agent version")).not.toBeInTheDocument();
    expect(JSON.parse(screen.getByTestId("picker-state").textContent!)).not.toHaveProperty("agentVersion");
  });

  it("preserves a restored catalog selection and clears it when agent versions are disabled", async () => {
    const user = userEvent.setup();
    renderPicker({ ...INITIAL_VALUE, agentName: "terminus-2", agentVersion: "harbor-v1" }, true);
    await screen.findByRole("option", { name: /harbor-v1 · Harbor/ });
    expect(screen.getByLabelText("Agent version")).toHaveValue("harbor-v1");
    await user.selectOptions(screen.getByLabelText("Agent version"), "");
    expect(JSON.parse(screen.getByTestId("picker-state").textContent!)).not.toHaveProperty("agentVersion");
    await user.selectOptions(screen.getByLabelText("Agent version"), "harbor-v2");
    await user.click(screen.getByText("Disable agent versions"));
    expect(screen.queryByLabelText("Agent version")).not.toBeInTheDocument();
    expect(JSON.parse(screen.getByTestId("picker-state").textContent!)).not.toHaveProperty("agentVersion");
  });

  it.each([
    [false, "terminus-2"], [true, "direct-completion"],
  ])("clears restored versions unsupported when allowAgentVersion=%s and agent=%s", async (allow, agentName) => {
    renderPicker({ ...INITIAL_VALUE, agentName, agentVersion: "harbor-v1" }, allow);
    await screen.findByRole("option", { name: "terminus-2" });
    expect(screen.queryByLabelText("Agent version")).not.toBeInTheDocument();
    expect(JSON.parse(screen.getByTestId("picker-state").textContent!)).not.toHaveProperty("agentVersion");
  });

  it("does not silently replace an unavailable restored exact version with deployment default", async () => {
    renderPicker({ ...INITIAL_VALUE, agentName: "terminus-2", agentVersion: "removed-version" }, true);
    expect(await screen.findByRole("option", { name: "removed-version (unavailable)" })).toBeDisabled();
    expect(screen.getByLabelText("Agent version")).toHaveValue("removed-version");
  });
});
