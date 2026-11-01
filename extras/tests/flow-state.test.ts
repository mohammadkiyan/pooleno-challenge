import { expect, it } from "vitest";
import { isResponse, nodeState } from "../frontend/flow-state";
import type { Frame } from "../frontend/flow-types";
const frame: Frame = { title: "Observed API receipt", detail: "", active: ["api", "client"], blocked: ["kafka"], parallel: [], book: [], trades: [], facts: [], request: "", response: "202", requestLabel: "", decision: "", source: "", phase: "api-reply", transfers: [["api", "client"]] };
it("highlights observed, blocked and next components", () => {
  expect(nodeState("api", frame)).toBe("current");
  expect(nodeState("kafka", frame)).toBe("blocked");
  expect(nodeState("matching", frame)).toBe("idle");
  expect(nodeState("matching", frame, { ...frame, active: ["matching"] })).toBe("next");
});
it("recognizes actual response transfers", () => {
  expect(isResponse(frame)).toBe(true);
  expect(isResponse({ ...frame, transfers: [] })).toBe(false);
});
