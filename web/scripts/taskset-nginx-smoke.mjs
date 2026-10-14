#!/usr/bin/env node
// Local/disposable smoke: real production bundle, nginx config and entrypoint.
// Only the API is stubbed; no cloud resources or model calls are used.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { chmodSync, cpSync, mkdtempSync, copyFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { resolve } from "node:path";
import { chromium } from "@playwright/test";

const root = resolve(import.meta.dirname, "../..");
const temp = mkdtempSync(resolve(tmpdir(), "loom-taskset-routes-"));
const docker = (...args) => execFileSync("docker", args, { encoding: "utf8" }).trim();
const id = "ts/team/a & b?x=1#100%+雪'()*!";
const task = {
  task_set_id: id, display_name: "Routing fixture", status: "ready",
  intents: ["evaluation"], manifest_intents: ["evaluation"], inferred_intents: [],
  capabilities: ["evaluation"], warnings: [], evaluation_ready: true,
  task_count: 1, error_summary: [], materialization_job_state: null,
};
const team = { id: "team", name: "Routing team", role: "member" };
let container;
let browser;
try {
  cpSync(resolve(root, "web/dist"), temp, { recursive: true });
  // mkdtemp creates mode 0700; nginx runs as uid 101 and needs to read this public build.
  chmodSync(temp, 0o755);
  copyFileSync(resolve(temp, "index.html"), resolve(temp, "index.html.template"));
  container = docker("run", "--rm", "-d", "-p", "127.0.0.1::8080",
    "-v", `${temp}:/usr/share/nginx/html:ro`,
    "-v", `${process.env.LOOM_NGINX_CONFIG ?? resolve(root, "deploy/nginx-spa.conf")}:/etc/nginx/conf.d/default.conf:ro`,
    "-v", `${resolve(root, "deploy/nginx-spa-security-headers.conf")}:/etc/nginx/loom-spa-security-headers.conf:ro`,
    "nginxinc/nginx-unprivileged:1.27-alpine");
  const origin = `http://${docker("port", container, "8080/tcp")}`;
  browser = await chromium.launch();
  for (const [prefix, environment] of [["", "local"], ["/dev", "development"], ["/prod", "production"], ["/staging", "staging"]]) {
    execFileSync("sh", [resolve(root, "deploy/web-runtime-config.sh")], { env: {
      ...process.env, LOOM_FRONTEND_ENVIRONMENT: environment, LOOM_FRONTEND_ROUTE_PATH: prefix,
      LOOM_FRONTEND_CONFIG_PATH: resolve(temp, "loom-frontend-config.json"),
      LOOM_FRONTEND_INDEX_TEMPLATE_PATH: resolve(temp, "index.html.template"),
      LOOM_FRONTEND_INDEX_PATH: resolve(temp, "index.html"),
    } });
    const context = await browser.newContext();
    const page = await context.newPage();
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));
    const requests = [];
    await page.route("**/api/**", async route => {
      const url = new URL(route.request().url());
      const path = url.pathname.slice(prefix.length);
      let body;
      let status = 200;
      if (path === "/api/v1/auth/me") body = {
        user: { id: "member", username: "Member", email: "member@example.test", display_name: "Member", is_platform_admin: false },
        teams: [team], current_team: team, role: "member", scopes: ["read:own", "submit"], is_platform_admin: false, csrf_token: "fixture",
      };
      else if (path === "/api/v1/tasksets") body = { items: [task] };
      else if (path === `/api/v1/tasksets/${id.split("/").map(encodeURIComponent).join("/")}` && !url.search) {
        requests.push(path); body = task;
      } else if (path === "/api/v1/tasksets/ts/other/private") { status = 404; body = { detail: "not found" }; }
      else throw new Error(`Unexpected API request: ${path}${url.search}`);
      await route.fulfill({ status, contentType: "application/json", body: JSON.stringify(body) });
    });
    const canonical = `${origin}${prefix}/task-sets/detail?${new URLSearchParams({ id })}`;
    const legacy = `${origin}${prefix}/task-sets/${encodeURIComponent(id)}?tab=errors`;
    const redirect = await fetch(legacy, { redirect: "manual" });
    assert.equal(redirect.status, 308, "old encoded link must migrate instead of nginx 404");
    for (const url of [canonical, legacy]) {
      assert.equal((await page.goto(url)).status(), 200);
      await page.getByRole("heading", { name: task.display_name, exact: true }).waitFor();
      assert.equal(await page.locator("#root").getAttribute("data-loom-mounted"), "true");
      assert.equal(new URL(page.url()).searchParams.get("id"), id);
      await page.reload();
      await page.getByRole("heading", { name: task.display_name, exact: true }).waitFor();
    }
    assert.equal(new URL(page.url()).searchParams.get("tab"), "errors");
    await page.goto(`${origin}${prefix}/task-sets`);
    await page.getByRole("link", { name: "Routing fixture" }).click();
    await page.getByRole("heading", { name: task.display_name, exact: true }).waitFor();
    assert.equal(page.url(), canonical);
    const beforeInvalid = requests.length;
    await page.goto(`${origin}${prefix}/task-sets/detail?id=ts%2F..%2Fx`);
    await page.getByRole("alert").filter({ hasText: "Invalid task set link" }).waitFor();
    assert.equal(requests.length, beforeInvalid);
    await page.goto(`${origin}${prefix}/task-sets/detail?id=ts%2Fother%2Fprivate`);
    await page.getByRole("alert").filter({ hasText: "unavailable to your team" }).waitFor();
    assert.equal(await page.getByRole("heading", { name: task.display_name, exact: true }).count(), 0);
    assert.deepEqual(errors, []);
    await context.close();
    console.log(`${prefix || "/"}: direct, refresh, legacy redirect, list, reserved IDs and unavailable-team UI passed`);
  }
  for (const path of ["/DEV/task-sets/ts%2Fteam%2Fx", "/%64ev/task-sets/ts%2Fteam%2Fx", "/dev%2Ftask-sets/ts%2Fteam%2Fx", "/dev//task-sets/ts%2Fteam%2Fx", "/providers/a%2Fb", "/task-sets/ts%2Fteam%5Cx", "/task-sets/ts%2Fteam%2Fx%0A", "/task-sets/ts%2Fteam%2Fx/extra", "/task-sets/ts%2Fteam%ZZ", "/task-sets/ts%2Fteam\\x"]) {
    // curl preserves raw path forms which fetch/Chromium normalize client-side.
    const status = execFileSync("curl", ["--path-as-is", "-s", "-o", "/dev/null", "-w", "%{http_code}", `${origin}${path}`], { encoding: "utf8" });
    assert.ok(["400", "404"].includes(status), `${path}: ${status}`);
  }
  console.log("Malformed paths remain rejected");
} finally {
  await browser?.close();
  if (container) docker("rm", "-f", container);
  rmSync(temp, { recursive: true, force: true });
}
