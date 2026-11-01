import { expect, test } from "@playwright/test";

test("Run executes the backend and shows real order and trade identifiers", async ({ page }, testInfo) => {
  test.setTimeout(120_000);
  await page.goto("/");
  const request = page.waitForRequest((r) => r.method() === "POST" && r.url().endsWith("/live/runs"));
  await page.getByRole("button", { name: "Price, time & partial fills", exact: false }).click();
  await request;
  await expect(page.getByText("RUN VERIFIED", { exact: true })).toBeVisible({ timeout: 100_000 });
  await expect(page.locator(".trades > div")).toHaveCount(3);
  await expect(page.locator(".book-row")).toHaveCount(1);
  await expect(page.locator(".book-row")).toContainText("S2");
  await expect(page.locator(".book-row")).toContainText("2.0000");
  await expect(page.getByTestId("live-evidence")).toContainText(/"orderId": "[0-9a-f-]{36}"/);
  await expect(page.getByTestId("live-evidence")).toContainText(/"tradeId": "[0-9a-f-]{36}"/);
  await page.screenshot({ path: testInfo.outputPath("live-matching.png"), fullPage: true });
  await page.getByLabel("View 2 · Show internals").check();
  await expect(page.getByText("View 2 · Detailed", { exact: true })).toBeVisible();
  await page.getByLabel("View 2 · Show internals").uncheck();
  const runCount: string[] = [];
  page.on("request", r => { if (r.method() === "POST" && r.url().endsWith("/live/runs")) runCount.push(r.url()); });
  await page.getByRole("button", { name: "Play recorded observations" }).click();
  await expect(page.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "1");
  await page.getByRole("button", { name: "Pause recorded observations" }).click();
  expect(runCount).toEqual([]);
});

test("an unavailable runner shows an error without illustrated success", async ({ page }) => {
  await page.route("**/live/active", route => route.fulfill({ json: null }));
  await page.route("**/live/runs", route => route.fulfill({ status: 503, json: { error: "Backend unavailable" } }));
  await page.goto("/");
  await page.getByRole("button", { name: "Run scenario", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("Backend unavailable");
  await expect(page.getByText("RUN VERIFIED", { exact: true })).toHaveCount(0);
  await expect(page.locator(".trades > div")).toHaveCount(0);
});

const cases = [
  ["non-crossing", ""], ["duplicate", ""], ["conflict", ""], ["lost-response", ""], ["kafka-outage", ""],
  ["matching", "sell-taker"], ["matching", "partial-taker"], ["matching", "crash-replay"],
  ["duplicate", "concurrent-retries"], ["conflict", "invalid-precision"], ["conflict", "invalid-quantity"],
  ["lost-response", "temporary-503"], ["lost-response", "permanent-redrive"], ["lost-response", "retry-exhaustion"],
];
for (const [scenarioId, variantId] of cases) {
  test(`real backend: ${scenarioId} ${variantId || "primary"}`, async ({ request }) => {
    test.setTimeout(180_000);
    const accepted = await request.post("/live/runs", { data: { scenarioId, variantId } });
    expect(accepted.status()).toBe(202);
    const initial = await accepted.json();
    let result = initial;
    await expect.poll(async () => {
      const response = await request.get(`/live/runs/${initial.id}`);
      result = await response.json();
      return result.status;
    }, { timeout: 170_000, intervals: [400, 500, 1000] }).not.toBe("running");
    expect(result.error, JSON.stringify(result.logs.slice(-10))).toBeUndefined();
    expect(result.cleanupError).toBeUndefined();
    expect(result.status).toBe("passed");
    expect(result.checks.length).toBeGreaterThan(0);
    expect(result.checks.every((c: { passed: boolean }) => c.passed)).toBe(true);
    expect(result.frames.length).toBeGreaterThan(1);
    expect(result.frames.at(-1).snapshot.counts.orders).toBe(scenarioId === "conflict" && variantId ? 0 : scenarioId === "matching" ? variantId === "partial-taker" ? 3 : 4 : scenarioId === "non-crossing" ? 2 : 1);
  });
}

test("rejects concurrent runs, invalid cases and cross-origin control", async ({ request }) => {
  test.setTimeout(120_000);
  const foreign = await request.post("/live/runs", { headers: { origin: "https://foreign.example" }, data: { scenarioId: "matching" } });
  expect(foreign.status()).toBe(403);
  expect((await request.post("/live/runs", { data: { scenarioId: "matching", variantId: "invented-case" } })).status()).toBe(400);
  const first = await request.post("/live/runs", { data: { scenarioId: "duplicate" } });
  expect(first.status()).toBe(202);
  expect((await request.post("/live/runs", { data: { scenarioId: "matching" } })).status()).toBe(409);
  const { id } = await first.json();
  await expect.poll(async () => (await (await request.get(`/live/runs/${id}`)).json()).status, { timeout: 100_000 }).toBe("passed");
});


test("a lost run capture unlocks the UI after a runner restart", async ({ page }) => {
  await page.route("**/live/active", route => route.fulfill({ json: null }));
  await page.route("**/live/runs", route => route.fulfill({ status: 202, json: { id: "lost-run", scenarioId: "matching", variantId: "", status: "running", startedAt: new Date().toISOString(), frames: [], checks: [], logs: [] } }));
  await page.route("**/live/runs/lost-run", route => route.fulfill({ status: 404, json: { error: "Run not retained" } }));
  await page.goto("/");
  await page.getByRole("button", { name: "Run scenario", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("runner may have restarted");
  await expect(page.getByRole("button", { name: "Run again", exact: true })).toBeEnabled();
  await expect(page.getByText("RUN FAILED", { exact: true })).toBeVisible();
  await expect(page.locator(".trades > div")).toHaveCount(0);
});


test("live evidence remains usable on a narrow screen with reduced motion", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  const errors: string[] = [];
  page.on("pageerror", error => errors.push(error.message));
  await page.goto("/");
  await expect(page.getByText("Live backend", { exact: false })).toBeVisible();
  await expect(page.getByRole("button", { name: "Price, time & partial fills", exact: false })).toBeVisible();
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth > innerWidth + 1);
  expect(overflow).toBe(false);
  await page.screenshot({ path: testInfo.outputPath("live-mobile.png"), fullPage: true });
  expect(errors).toEqual([]);
});
