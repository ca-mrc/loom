import { act, fireEvent, render, screen } from "@testing-library/react";
import { useLayoutEffect } from "react";
import { Link, MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { afterEach, expect, it, vi } from "vitest";
import { useRouteScroll } from "../../hooks/useRouteScroll";

afterEach(() => vi.restoreAllMocks());

it("does not save a shorter destination's clamped scroll against the departed route", () => {
  let scrollY = 0;
  vi.spyOn(window, "scrollY", "get").mockImplementation(() => scrollY);
  vi.spyOn(window, "scrollTo").mockImplementation((x: number | ScrollToOptions, y?: number) => {
    scrollY = typeof x === "object" ? x.top ?? 0 : y ?? 0;
  });
  function Routes(): JSX.Element {
    useRouteScroll();
    const { pathname } = useLocation();
    const navigate = useNavigate();
    useLayoutEffect(() => {
      if (pathname === "/monitor") {
        // A shorter page clamps the window during layout, before passive effects.
        scrollY = 525;
        window.dispatchEvent(new Event("scroll"));
      }
    }, [pathname]);
    return <main id="main-content">
      <Link to="/monitor">Open Monitor</Link>
      <button onClick={() => navigate(-1)}>Back</button>
    </main>;
  }
  render(<MemoryRouter initialEntries={["/guide"]}><Routes /></MemoryRouter>);
  act(() => {
    scrollY = 1178;
    window.dispatchEvent(new Event("scroll"));
  });
  fireEvent.click(screen.getByRole("link", { name: "Open Monitor" }));
  fireEvent.click(screen.getByRole("button", { name: "Back" }));
  expect(scrollY).toBe(1178);
});
