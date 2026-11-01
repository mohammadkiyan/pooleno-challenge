import { expect, test } from "@playwright/test";

test("one click plays the matching illustration to the expected final book", async ({
  page,
}) => {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/");
  await expect(
    page.getByText("This preview does not connect to the backend.", {
      exact: false,
    }),
  ).toBeVisible();
  await page.getByLabel("Playback speed").selectOption("2");
  await page.getByRole("button", { name: "Run scenario", exact: true }).click();
  await expect(
    page.getByText("ILLUSTRATION COMPLETE", { exact: true }),
  ).toBeVisible({ timeout: 22000 });
  await expect(page.locator(".trades > div")).toHaveCount(3);
  await expect(page.locator(".book-row")).toHaveCount(1);
  await expect(page.locator(".book-row")).toContainText("S2");
  await expect(page.locator(".book-row")).toContainText("2.0000");
  await page
    .getByRole("button", { name: "Replay scenario", exact: true })
    .click();
  await expect(page.getByRole("progressbar")).toHaveAttribute(
    "aria-valuenow",
    "1",
  );
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Resume", exact: true }),
  ).toBeVisible();
  expect(errors).toEqual([]);
});

test("all scenario cards start on click and can be inspected without autoplay", async ({
  page,
}) => {
  await page.goto("/");
  const titles = [
    "Price, time & partial fills",
    "Prices that never cross",
    "One order, two requests",
    "Same key, different order",
    "Delivered. Response lost.",
    "Kafka pauses. Orders don’t.",
  ];
  for (const title of titles) {
    await page.getByRole("button", { name: title, exact: false }).click();
    await expect(
      page.getByRole("heading", { name: title, exact: true }),
    ).toBeVisible();
    await page.getByRole("button", { name: "Pause", exact: true }).click();
    await expect(page.getByRole("progressbar")).toHaveAttribute(
      "aria-valuenow",
      "1",
    );
    const next = page.getByRole("button", { name: "Next step", exact: true });
    while (await next.isEnabled()) await next.click();
    await expect(
      page.getByText("ILLUSTRATION COMPLETE", { exact: true }),
    ).toBeVisible();
    await page
      .getByRole("button", { name: "Previous step", exact: true })
      .click();
    await expect(
      page.getByText("ILLUSTRATION COMPLETE", { exact: true }),
    ).not.toBeVisible();
  }
});

test("a destination retry animates only the worker-to-destination connection", async ({
  page,
}) => {
  await page.goto("/");
  await page
    .getByRole("button", { name: "Delivered. Response lost.", exact: false })
    .click();
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  for (
    let index = 0;
    index < 25 &&
    !(await page
      .getByRole("heading", { name: "Retry with the original key" })
      .isVisible());
    index += 1
  )
    await page.getByRole("button", { name: "Next step" }).click();
  await expect(
    page.getByRole("heading", { name: "Retry with the original key" }),
  ).toBeVisible();
  await expect(page.locator(".edge-active")).toHaveCount(1);
  await expect(page.locator(".edge-active path")).toHaveAttribute(
    "d",
    "M420 205H220",
  );
});

test("detail tabs support keyboard navigation", async ({ page }) => {
  await page.goto("/");
  const explanation = page.getByRole("tab", { name: "What’s happening" });
  const data = page.getByRole("tab", { name: "Example data" });
  await explanation.focus();
  await page.keyboard.press("ArrowRight");
  await expect(data).toBeFocused();
  await expect(data).toHaveAttribute("aria-selected", "true");
  await expect(
    page.getByText("ILLUSTRATIVE REQUEST", { exact: true }),
  ).toBeVisible();
  await page.keyboard.press("Home");
  await expect(explanation).toBeFocused();
  await expect(explanation).toHaveAttribute("aria-selected", "true");
});

test("mobile layout fits the viewport and respects reduced motion", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.goto("/");
  await page.getByRole("button", { name: "Run scenario", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "Pause", exact: true }),
  ).toBeVisible();
  await expect(page.locator(".connections circle")).toHaveCount(0);
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
  for (const width of [320, 768, 1024, 1440]) {
    await page.setViewportSize({ width, height: 900 });
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
  }
});

test("detail view exposes API and matching internals without resetting playback", async ({
  page,
}) => {
  await page.goto("/");
  const toggle = page.getByRole("checkbox", {
    name: "View 2 · Show internals",
  });
  await expect(toggle).not.toBeChecked();
  await expect(
    page.getByRole("region", { name: "Internal processing flows" }),
  ).not.toBeVisible();
  await page.getByRole("button", { name: "Next step" }).click();
  await toggle.check();
  await expect(
    page.getByRole("region", { name: "Internal processing flows" }),
  ).toBeVisible();
  await expect(page.getByRole("progressbar")).toHaveAttribute(
    "aria-valuenow",
    "2",
  );
  await expect(
    page.getByText("Validate & normalize", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("Commit before changing the book", { exact: true }),
  ).toBeVisible();
  for (let i = 0; i < 2; i += 1)
    await page.getByRole("button", { name: "Next step" }).click();
  await expect(page.locator('[data-phase="api-save"]')).toHaveAttribute(
    "aria-current",
    "step",
  );
  await toggle.uncheck();
  await expect(page.getByRole("progressbar")).toHaveAttribute(
    "aria-valuenow",
    "4",
  );
  await toggle.check();
  await page.setViewportSize({ width: 320, height: 844 });
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);
});

test("view 2 expands diagram components and reveals detail on hover", async ({
  page,
}) => {
  await page.goto("/");
  await page.getByRole("checkbox", { name: "View 2 · Show internals" }).check();
  const api = page.getByRole("button", { name: "Order API internals" });
  await expect(api).toHaveAttribute("aria-expanded", "true");
  await api.click();
  await expect(api).toHaveAttribute("aria-expanded", "false");
  await api.click();
  await page
    .getByRole("button", { name: "Validate & normalize", exact: true })
    .hover();
  await expect(page.getByRole("tooltip")).toContainText("exact integer units");
  await page.getByRole("tooltip").hover();
  await expect(page.getByRole("tooltip")).toBeVisible();
  await page
    .getByRole("button", {
      name: "Commit before changing the book",
      exact: true,
    })
    .focus();
  await expect(page.getByRole("tooltip")).toContainText("without mutating");
  await page.keyboard.press("Escape");
  await expect(page.getByRole("tooltip")).not.toBeVisible();
});

test("detailed API steps animate in order while the scenario plays", async ({
  page,
}) => {
  await page.goto("/");
  await page
    .getByRole("button", { name: "One order, two requests", exact: false })
    .click();
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await page.getByRole("checkbox", { name: "View 2 · Show internals" }).check();
  await page.getByRole("button", { name: "Resume", exact: true }).click();
  for (const phase of ["api-validate", "api-key", "api-save", "api-reply"]) {
    await expect(page.locator(`[data-phase="${phase}"]`)).toHaveAttribute(
      "aria-current",
      "step",
      { timeout: 4000 },
    );
  }
});

test("both views share the request sequence and do not invent internal steps while paused", async ({
  page,
}) => {
  await page.clock.install();
  await page.goto("/");
  await expect(page.locator(".node-active")).toContainText("Client");
  await expect(page.locator(".request-context")).toContainText("R1 · B1");
  await page.getByRole("button", { name: "Next step" }).click();
  await expect(
    page.getByRole("heading", { name: "Validate the incoming request" }),
  ).toBeVisible();
  await page.getByRole("checkbox", { name: "View 2 · Show internals" }).check();
  await expect(page.locator('[data-phase="api-validate"]')).toHaveAttribute(
    "aria-current",
    "step",
  );
  await page.clock.runFor(10000);
  await expect(page.locator('[data-phase="api-validate"]')).toHaveAttribute(
    "aria-current",
    "step",
  );
  await expect(page.getByRole("progressbar")).toHaveAttribute(
    "aria-valuenow",
    "2",
  );
  await page.getByRole("button", { name: "Next step" }).click();
  await expect(page.locator('[data-phase="api-key"]')).toHaveAttribute(
    "aria-current",
    "step",
  );
});

test("canvas zoom and flow states make directions and parallel branches explicit", async ({
  page,
}) => {
  await page.goto("/");
  const canvas = page.getByRole("region", { name: "Flow canvas" });
  await expect(canvas).toBeVisible();
  const zoom = page.getByLabel("Zoom level");
  const initial = await zoom.textContent();
  await canvas.hover();
  await page.mouse.wheel(0, -350);
  await expect(zoom).not.toHaveText(initial!);
  await page.getByRole("button", { name: "Fit diagram" }).click();
  await expect(zoom).toHaveText(initial!);
  const bounds = await canvas.boundingBox();
  const before = await page.locator(".flow-world").boundingBox();
  if (!bounds || !before) throw new Error("Canvas must be measurable");
  await page.mouse.move(bounds.x + 24, bounds.y + 24);
  await page.mouse.down();
  await page.mouse.move(bounds.x + 94, bounds.y + 44, { steps: 5 });
  await page.mouse.up();
  const after = await page.locator(".flow-world").boundingBox();
  expect(after!.x).toBeGreaterThan(before.x + 40);
  await page.getByRole("button", { name: "Fit diagram" }).click();

  for (let i = 0; i < 4; i++)
    await page.getByRole("button", { name: "Next step", exact: true }).click();
  await expect(page.locator(".edge-response.edge-active")).toHaveCount(1);
  await expect(page.getByText("Response path", { exact: true })).toBeVisible();
  for (let i = 0; i < 3; i++)
    await page.getByRole("button", { name: "Next step", exact: true }).click();
  await expect(page.locator(".node-parallel")).toHaveCount(2);
  await expect(page.getByText("Next:", { exact: false }).first()).toBeVisible();
});

test("detailed outage retains a visible unavailable status", async ({
  page,
}) => {
  await page.goto("/");
  await page
    .getByRole("button", { name: "Kafka pauses. Orders don’t.", exact: false })
    .click();
  await page.getByRole("button", { name: "Pause", exact: true }).click();
  await page.getByRole("checkbox", { name: "View 2 · Show internals" }).check();
  await expect(page.locator(".state-blocked")).toContainText("Kafka");
  await expect(page.locator(".state-blocked")).toContainText(
    "Unavailable / uncertain",
  );
});
