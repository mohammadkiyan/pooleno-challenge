import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  testMatch: "**/*.spec.ts",
  workers: 1,
  use: {
    baseURL: process.env.VIEWER_URL ?? "http://127.0.0.1:5173",
    viewport: { width: 1440, height: 1060 },
  },
  webServer: process.env.VIEWER_URL ? undefined : {
    command: "npm run dev",
    url: "http://127.0.0.1:5173",
    reuseExistingServer: !process.env.CI,
  },
});
