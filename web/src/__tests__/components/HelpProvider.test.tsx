import { useState } from "react";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";
import { HelpButton } from "../../components/HelpButton";
import { HelpProvider } from "../../components/HelpProvider";

function DraftForm() {
  const [draft, setDraft] = useState("");
  return <><input aria-label="Draft batch name" value={draft} onChange={(event) => setDraft(event.target.value)} /><HelpButton topic="tasks">Task help</HelpButton></>;
}
describe("contextual help", () => {
  it("keeps the form mounted, restores trigger focus, and offers repository documentation", async () => {
    const user = userEvent.setup();
    render(<MemoryRouter><HelpProvider><DraftForm /></HelpProvider></MemoryRouter>);
    const input = screen.getByRole("textbox", { name: "Draft batch name" });
    await user.type(input, "keep my draft");
    const trigger = screen.getByRole("button", { name: "Task help" });
    await user.click(trigger);
    const dialog = await screen.findByRole("dialog", { name: "Tasks and batch purpose" });
    expect(within(dialog).getByRole("link", { name: /Task sets and manifest schema/ })).toHaveAttribute("href", expect.stringContaining("#manifest"));
    expect(input).toHaveValue("keep my draft");
    await user.keyboard("{Escape}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(input).toHaveValue("keep my draft");
    expect(trigger).toHaveFocus();
  });
  it("has a navigable guide fallback outside the app shell", () => {
    render(<MemoryRouter><HelpButton topic="providers" /></MemoryRouter>);
    expect(screen.getByRole("link", { name: "Help" })).toHaveAttribute("href", "/getting-started?topic=providers");
  });
  it("does not expose rate-card management help to a non-admin", async () => {
    render(<MemoryRouter><HelpProvider><HelpButton topic="rates" /></HelpProvider></MemoryRouter>);
    await userEvent.setup().click(screen.getByRole("button", { name: "Help" }));
    expect(await screen.findByRole("dialog", { name: "Start using Loom" })).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: /Rate-card format/ })).not.toBeInTheDocument();
  });
});
