import { readFile } from "node:fs/promises";
import { expect, test } from "@playwright/test";

// HTTPS is browser-visible but fully intercepted: no DNS/TLS/service contact.
// Serve the actual built entrypoint so fragment scrubbing is exercised before
// the first frontend-config/auth request, not only in a component harness.
test("built managed login scrubs the proof before startup and submits it only on click", { tag: "@protocol" }, async ({ page }) => {
  const origin = "https://managed-login.example.test";
  const token = "loom_env_login_" + "a".repeat(43);
  const failures: string[] = [];
  const submitted: string[] = [];
  let startupReads = 0;
  const owner = {
    user: { id: "owner", username: "owner", email: null, display_name: "Alice", is_platform_admin: false },
    teams: [{ id: "team", name: "Development alice", role: "owner" }],
    current_team: { id: "team", name: "Development alice", role: "owner" },
    role: "owner", scopes: ["read:own", "submit"], is_platform_admin: false, csrf_token: "child-csrf",
  };
  page.on("pageerror", (error) => failures.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error" || message.type() === "warning" || message.text().includes(token)) {
      failures.push(message.text());
    }
  });
  await page.route(origin + "/**", async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    expect(request.url()).not.toContain(token);
    if (path === "/auth/managed") {
      await route.fulfill({ contentType: "text/html", body: await readFile("dist/index.html") });
      return;
    }
    const asset = path.match(/^\/(?:dev\/|prod\/)?assets\/([A-Za-z0-9_-]+\.(js|css))$/);
    if (asset) {
      await route.fulfill({ contentType: asset[2] === "js" ? "text/javascript" : "text/css",
                            body: await readFile("dist/assets/" + asset[1]) });
      return;
    }
    expect(await page.evaluate(() => window.location.hash + window.location.search)).toBe("");
    if (path === "/loom-frontend-config.json") {
      startupReads += 1;
      await route.fulfill({ json: { environment: "development", environmentLabel: "Personal dev", routePath: "",
                                  apiBase: "", apiRouteBase: origin + "/api" } });
    } else if (path === "/api/v1/auth/me") {
      await route.fulfill({ json: owner });
    } else if (path === "/api/v1/auth/login/complete") {
      submitted.push(request.postDataJSON().token);
      await route.fulfill({ json: owner });
    } else {
      failures.push("unexpected request " + path);
      await route.fulfill({ status: 404, body: "not found" });
    }
  });
  await page.goto(origin + "/auth/managed#token=" + token);
  const button = page.getByRole("button", { name: "Sign into this environment" });
  await expect(button).toBeVisible();
  expect(startupReads).toBeGreaterThan(0);
  expect(submitted).toEqual([]);
  expect(page.url()).toBe(origin + "/auth/managed");
  await button.click();
  await expect(page.getByText("Signed in to this environment.")).toBeVisible();
  expect(submitted).toEqual([token]);
  expect(failures).toEqual([]);
});

for (const status of [307, 308]) {
  test(`managed login refuses ${status} redirects before forwarding its proof`, { tag: "@protocol" }, async ({ page }) => {
    const origin = "https://managed-login.example.test";
    const foreign = "https://foreign.example.test";
    const token = "loom_env_login_" + "b".repeat(43);
    const foreignRequests: string[] = [];
    const submitted: string[] = [];
    // CDP intercepts redirected requests too, before DNS or network access.
    // Playwright route handlers alone do not intercept every redirect hop.
    const cdp = await page.context().newCDPSession(page);
    await cdp.send("Fetch.enable", { patterns: [{ urlPattern: "*" }] });
    cdp.on("Fetch.requestPaused", async ({ requestId, request }) => {
      const url = new URL(request.url);
      const headers = [{ name: "Content-Type", value: "application/json" }];
      let responseCode = 200;
      let body: string | Buffer = "{}";
      if (url.origin === foreign) {
        foreignRequests.push(request.method + ":" + (request.postData ?? ""));
        headers.push(
          { name: "Access-Control-Allow-Origin", value: origin },
          { name: "Access-Control-Allow-Credentials", value: "true" },
          { name: "Access-Control-Allow-Methods", value: "POST, OPTIONS" },
          { name: "Access-Control-Allow-Headers", value: "content-type, x-loom-csrf" },
        );
        responseCode = 400;
        if (request.method === "OPTIONS") responseCode = 204;
      } else if (url.origin !== origin) {
        responseCode = 404;
      } else if (url.pathname === "/auth/managed") {
        headers[0].value = "text/html";
        body = await readFile("dist/index.html");
      } else if (/^\/(?:dev\/|prod\/)?assets\/[A-Za-z0-9_-]+\.(js|css)$/.test(url.pathname)) {
        const asset = url.pathname.split("/").at(-1)!;
        headers[0].value = asset.endsWith(".js") ? "text/javascript" : "text/css";
        body = await readFile("dist/assets/" + asset);
      } else if (url.pathname === "/loom-frontend-config.json") {
        body = JSON.stringify({ environment: "development", environmentLabel: "Personal dev", routePath: "", apiBase: "", apiRouteBase: origin + "/api" });
      } else if (url.pathname === "/api/v1/auth/me") {
        responseCode = 401;
      } else if (url.pathname === "/api/v1/auth/login/complete") {
        submitted.push(JSON.parse(request.postData!).token);
        responseCode = status;
        headers.push({ name: "Location", value: foreign + "/collect" });
      } else {
        responseCode = 404;
      }
      await cdp.send("Fetch.fulfillRequest", {
        requestId, responseCode, responseHeaders: headers,
        body: Buffer.from(body).toString("base64"),
      });
    });
    await page.goto(origin + "/auth/managed#token=" + token);
    await page.getByRole("button", { name: "Sign into this environment" }).click();
    await expect(page.getByRole("alert")).toContainText("Request a fresh browser login");
    expect(submitted).toEqual([token]);
    expect(foreignRequests).toEqual([]);
    expect(page.url()).toBe(origin + "/auth/managed");
    await expect(page.getByRole("button", { name: "Sign into this environment" })).toHaveCount(0);
    await cdp.detach();
  });
}
