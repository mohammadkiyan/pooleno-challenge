import { describe, expect, it } from "vitest";
import { scenarios } from "../frontend/scenarios";

describe("request-led walkthroughs", () => {
  it.each(scenarios)(
    "$id starts with an identified client request",
    (scenario) => {
      expect(scenario.frames[0]?.active).toEqual(["client"]);
      expect(scenario.frames[0]?.request).toContain("POST /v1/orders");
    },
  );
  it.each(scenarios)(
    "$id explains each decision and links it to source code",
    (scenario) => {
      for (const frame of scenario.frames) {
        expect(frame.requestLabel.length).toBeGreaterThan(0);
        expect(frame.decision.length).toBeGreaterThan(0);
        expect(frame.source).toMatch(/^src\//);
      }
      const phases = scenario.frames.map((f) => f.phase);
      expect(phases.indexOf("api-validate")).toBeLessThan(
        phases.indexOf("api-save"),
      );
      expect(phases.indexOf("api-save")).toBeLessThan(
        phases.indexOf("api-reply"),
      );
    },
  );
  it("uses only the saved receipt path for an identical retry", () => {
    const frames = scenarios.find((s) => s.id === "duplicate")!.frames;
    const retry = frames.filter(
      (f) => f.requestLabel === "R2 · retry same order",
    );
    expect(retry[0]?.active).toEqual(["client"]);
    expect(retry.map((f) => f.phase)).toContain("api-key");
    expect(retry.map((f) => f.phase)).not.toContain("api-save");
    expect(retry.some((f) => f.active.includes("kafka"))).toBe(false);
  });
  it("keeps the committed book unchanged throughout match computation", () => {
    const frames = scenarios[0].frames;
    const input = frames[0]!.book;
    for (const frame of frames.filter(
      (f) =>
        f.phase === "match-price" ||
        f.phase === "match-fill" ||
        f.phase === "match-save",
    ))
      expect(frame.book).toEqual(input);
    expect(frames.find((f) => f.phase === "match-book")?.book).toEqual([
      { id: "S2", side: "SELL", price: "100.00", quantity: "2.0000" },
    ]);
  });
});
