import { expect, it } from "vitest";
import { scenarios } from "../frontend/scenarios";
import { nodeState, isResponse } from "../frontend/flow-state";
it("separates the current client from the next API", () => {
  expect(
    nodeState("client", scenarios[0].frames[0]!, scenarios[0].frames[1]),
  ).toBe("current");
  expect(
    nodeState("api", scenarios[0].frames[0]!, scenarios[0].frames[1]),
  ).toBe("next");
});
it("only marks the independent fanout as parallel, not an API database transaction", () => {
  const fork = scenarios[0].frames.find((f) => f.parallel.length > 0)!;
  expect(nodeState("matching", fork)).toBe("parallel");
  expect(nodeState("worker", fork)).toBe("parallel");
  const commit = scenarios[0].frames.find((f) => f.phase === "api-save")!;
  expect(nodeState("api", commit)).toBe("current");
  expect(nodeState("database", commit)).toBe("current");
});
it("distinguishes an HTTP response from an incoming request", () => {
  expect(
    isResponse(scenarios[0].frames.find((f) => f.phase === "api-reply")!),
  ).toBe(true);
  expect(
    isResponse(scenarios[0].frames.find((f) => f.phase === "api-validate")!),
  ).toBe(false);
});
