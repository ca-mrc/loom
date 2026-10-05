import { defineConfig, devices } from "@playwright/test";

import {
  browserWebServerConfig,
  readBrowserHarnessConfig,
} from "./scripts/browser-harness-config.mjs";

const harness = readBrowserHarnessConfig();

export default defineConfig({
  testDir: "./e2e",
  timeout: 30_000,
  expect: { timeout: 10_000 },
  fullyParallel: true,
  forbidOnly: true,
  retries: 0,
  reporter: [["line"]],
  use: {
    baseURL: harness.baseURL,
    screenshot: "only-on-failure",
    trace: "retain-on-failure",
  },
  projects: [
    // Protocol cases exercise the built client once on desktop. Responsive,
    // keyboard, layout, and accessibility cases still run in every viewport.
    { name: "chromium-tablet", grepInvert: /@protocol/, use: { ...devices["Desktop Chrome"], viewport: { width: 768, height: 1024 } } },
    { name: "chromium-laptop", grepInvert: /@protocol/, use: { ...devices["Desktop Chrome"], viewport: { width: 1280, height: 800 } } },
    {
      name: "chromium-desktop",
      use: { ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 } },
    },
    {
      name: "chromium-mobile",
      grepInvert: /@protocol/,
      use: { ...devices["Desktop Chrome"], viewport: { width: 390, height: 844 } },
    },
  ],
  webServer: browserWebServerConfig(harness),
});
