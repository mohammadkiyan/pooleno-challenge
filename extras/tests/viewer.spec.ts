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
  ).toBeVisible({ timeout: 15000 });
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
  for (let index = 0; index < 4; index += 1)
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
