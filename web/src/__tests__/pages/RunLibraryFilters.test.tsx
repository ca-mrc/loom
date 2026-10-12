import { act, fireEvent, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";
import { render } from "@testing-library/react";
import { RunLibraryFilters } from "../../pages/RunLibraryFilters";

function LocationProbe(): JSX.Element {
  const location = useLocation();
  const navigate = useNavigate();
  return <><output aria-label="Current URL">{location.search}</output><button onClick={() => navigate(-1)}>Back</button></>;
}
function mount(route = "/library?scope=all&team_id=team-1&agent=legacy") {
  render(<MemoryRouter initialEntries={[route]}><RunLibraryFilters teamOptions={[{ id: "team-1", name: "Research" }]} /><LocationProbe /></MemoryRouter>);
}

describe("Run Library filters", () => {
  afterEach(() => vi.useRealTimers());
  it("commits one debounced search, and does not replay a draft over back navigation", async () => {
    vi.useFakeTimers();
    mount();
    const search = screen.getByRole("textbox", { name: "Search" });
    fireEvent.change(search, { target: { value: "experiment" } });
    expect(screen.getByLabelText("Current URL")).not.toHaveTextContent("q=");
    await act(async () => { vi.advanceTimersByTime(300); });
    expect(screen.getByLabelText("Current URL")).toHaveTextContent("q=experiment");
    fireEvent.click(screen.getByRole("button", { name: "Back" }));
    expect(search).toHaveValue("");
    await act(async () => { vi.advanceTimersByTime(1000); });
    expect(screen.getByLabelText("Current URL")).not.toHaveTextContent("q=");
  });
  it("removes aliases and clears filters while preserving team and scope", async () => {
    const user = userEvent.setup(); mount();
    await user.click(screen.getByRole("button", { name: "Remove Agent: legacy" }));
    expect(screen.getByLabelText("Current URL")).toHaveTextContent("?scope=all&team_id=team-1");
    await user.click(screen.getByText("Advanced filters"));
    await user.selectOptions(screen.getByRole("combobox", { name: "State" }), "finished");
    await user.click(screen.getByRole("button", { name: "Clear filters" }));
    expect(screen.getByLabelText("Current URL")).toHaveTextContent("?scope=all&team_id=team-1");
  });

  it("only enables filters supported by the current result mode", async () => {
    const user = userEvent.setup(); mount("/library?producer_kind=pipeline&q=retained&pipeline_recipe=research");
    expect(screen.getByRole("textbox", { name: "Search" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: "Remove Search: retained" })).not.toBeInTheDocument();
    await user.click(screen.getByText("Advanced filters"));
    expect(screen.getByRole("textbox", { name: "Pipeline Recipe" })).toBeEnabled();
    expect(screen.getByRole("textbox", { name: "Agent" })).toBeDisabled();
  });
  it("retains run filters from the URL outside Pipeline mode", async () => {
    const user = userEvent.setup(); mount("/library?q=retained&pipeline_recipe=research");
    expect(screen.getByRole("textbox", { name: "Search" })).toBeEnabled();
    expect(screen.getByRole("textbox", { name: "Search" })).toHaveValue("retained");
    await user.click(screen.getByText("Advanced filters"));
    expect(screen.getByRole("textbox", { name: "Pipeline Recipe" })).toBeDisabled();
  });
});
