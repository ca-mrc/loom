import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import ProviderForm from "../../../components/providers/ProviderForm";

describe("ProviderForm", () => {
  it("create mode renders api_key field", () => {
    render(<ProviderForm mode="create" onSubmit={vi.fn()} />);
    expect(screen.getByLabelText(/api key/i)).toBeInTheDocument();
  });

  it("edit mode does NOT render api_key field (rotation has its own flow)", () => {
    render(
      <ProviderForm
        mode="edit"
        initial={{ name: "x", type: "openai-compatible", base_url: "https://" }}
        onSubmit={vi.fn()}
      />,
    );
    expect(screen.queryByLabelText(/api key/i)).not.toBeInTheDocument();
  });

  it("shows human-readable pricing choices without handwritten JSON", () => {
    render(<ProviderForm mode="create" onSubmit={vi.fn()} />);
    expect(screen.getByLabelText(/pricing mode/i)).toHaveValue("usage_only");
    expect(
      screen.getByRole("option", { name: "Enter custom model prices" }),
    ).toBeInTheDocument();
  });

  it("calls onSubmit with the form values on submit", async () => {
    const onSubmit = vi.fn();
    const user = userEvent.setup();
    render(<ProviderForm mode="create" onSubmit={onSubmit} />);
    await user.type(screen.getByLabelText(/name/i), "test-conn");
    await user.type(screen.getByLabelText(/base url/i), "https://api.x");
    await user.type(screen.getByLabelText(/api key/i), "sk-x");
    await user.click(screen.getByRole("button", { name: /create/i }));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "test-conn",
        base_url: "https://api.x",
        api_key: "sk-x",
      }),
    );
  });

  it("labels native types as unusable for agent runs (#2054)", async () => {
    const user = userEvent.setup();
    render(<ProviderForm mode="create" onSubmit={vi.fn()} />);
    expect(screen.queryByRole("note")).not.toBeInTheDocument();
    expect(
      screen.getByRole("option", { name: "anthropic (not for agent runs)" }),
    ).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText(/type/i), "google");
    expect(screen.getByRole("note")).toHaveTextContent(
      /agent batches and trials can't use this connection/i,
    );
    await user.selectOptions(screen.getByLabelText(/type/i), "custom");
    expect(screen.queryByRole("note")).not.toBeInTheDocument();
  });

  it("submit button is disabled while pending=true is passed", () => {
    render(<ProviderForm mode="create" onSubmit={vi.fn()} pending />);
    expect(screen.getByRole("button", { name: /create/i })).toBeDisabled();
  });
});

it("initializes per-model prices and submits edits with the active mode", async () => {
  const onSubmit = vi.fn();
  const user = userEvent.setup();
  render(
    <ProviderForm
      mode="edit"
      initial={{
        name: "x",
        type: "custom",
        base_url: "https://x",
        pricing_mode: "custom",
        custom_pricing: { a: { input_usd_per_1m: 1, output_usd_per_1m: 2 } },
      }}
      onSubmit={onSubmit}
    />,
  );
  const input = screen.getByLabelText("a input_usd_per_1m");
  expect(input).toHaveValue(1);
  await user.clear(input);
  await user.type(input, "0");
  await user.click(screen.getByRole("button", { name: "Save changes" }));
  expect(onSubmit).toHaveBeenLastCalledWith(
    expect.objectContaining({
      pricing_mode: "custom",
      catalog_id: null,
      custom_pricing: {
        a: {
          input_usd_per_1m: 0,
          output_usd_per_1m: 2,
          cache_read_usd_per_1m: null,
          cache_write_usd_per_1m: null,
        },
      },
    }),
  );
  await user.selectOptions(screen.getByLabelText("Pricing mode"), "usage_only");
  await user.click(screen.getByRole("button", { name: "Save changes" }));
  expect(onSubmit).toHaveBeenLastCalledWith(
    expect.objectContaining({
      pricing_mode: "usage_only",
      catalog_id: null,
      custom_pricing: null,
    }),
  );
});

it("blocks incomplete prices instead of silently dropping malformed values", async () => {
  const onSubmit = vi.fn();
  const user = userEvent.setup();
  render(
    <ProviderForm
      mode="edit"
      initial={{
        name: "x",
        type: "custom",
        base_url: "https://x",
        pricing_mode: "custom",
        custom_pricing: { a: { input_usd_per_1m: 1, output_usd_per_1m: 2 } },
      }}
      onSubmit={onSubmit}
    />,
  );
  await user.clear(screen.getByLabelText("a output_usd_per_1m"));
  await user.click(screen.getByRole("button", { name: "Save changes" }));
  expect(screen.getByRole("alert")).toHaveTextContent(
    "input and output prices are required",
  );
  expect(onSubmit).not.toHaveBeenCalled();
});
