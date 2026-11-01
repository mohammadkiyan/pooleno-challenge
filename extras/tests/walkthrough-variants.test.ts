import { expect, it } from "vitest";
import { walkthroughVariants } from "../frontend/walkthrough-variants";
const all = Object.values(walkthroughVariants).flat();
function variant(id: string) {
  const found = all.find((item) => item.id === id);
  if (!found) throw new Error(id);
  return found;
}
it.each(all)(
  "$id starts at the client and explains every decision with source evidence",
  (item) => {
    expect(item.guarantee.length).toBeGreaterThan(0);
    expect(item.frames[0]?.active).toEqual(["client"]);
    expect(item.frames[0]?.request).toContain("POST /v1/orders");
    for (const frame of item.frames) {
      expect(frame.decision.length).toBeGreaterThan(0);
      expect(frame.source).toMatch(/^src\//);
    }
    expect(item.coverage.evidence.length).toBeGreaterThan(0);
  },
);
it.each(["invalid-precision", "invalid-quantity"])(
  "%s rejects without an acceptance phase",
  (id) => {
    const frames = variant(id).frames;
    expect(
      frames.some((f) => f.phase === "api-save" || f.phase === "publish-send"),
    ).toBe(false);
    expect(frames.at(-1)?.response).toContain("400");
    expect(frames.at(-1)?.facts).toContainEqual(["Orders / events", "0 / 0"]);
  },
);
it("restores committed quantities and treats event replay as a no-op", () => {
  const frames = variant("crash-replay").frames;
  expect(
    frames.find((f) => f.title === "Crash after the matching commit")
      ?.bookState,
  ).toBe("discarded");
  const restored = frames.find((f) => f.phase === "match-restore")!;
  const replay = frames.filter((f) => f.phase === "match-replay");
  expect(restored.trades).toHaveLength(3);
  for (const frame of replay) {
    expect(frame.trades).toEqual(restored.trades);
    expect(frame.book).toEqual(restored.book);
  }
  expect(frames.at(-1)?.facts).toContainEqual(["New trades on replay", "0"]);
});
it("SELL takers consume the highest bid first and identify the right taker", () => {
  const last = variant("sell-taker").frames.at(-1)!;
  expect(
    last.trades.map((t) => [t.maker, t.price, t.quantity, t.taker]),
  ).toEqual([
    ["B3", "101.00", "1.0000", "S1"],
    ["B1", "100.00", "2.0000", "S1"],
    ["B2", "100.00", "1.0000", "S1"],
  ]);
  expect(last.book[0]?.quantity).toBe("2.0000");
});
it("rests only the partially filled taker remainder at its limit", () => {
  const last = variant("partial-taker").frames.at(-1)!;
  expect(last.trades).toHaveLength(1);
  expect(last.book).toContainEqual({
    id: "B1",
    side: "BUY",
    price: "100.00",
    quantity: "2.0000",
  });
  expect(last.book).toContainEqual({
    id: "S2",
    side: "SELL",
    price: "105.00",
    quantity: "1.0000",
  });
});
it("shows a second request and response before exhausting its illustrated retry budget", () => {
  const frames = variant("retry-exhaustion").frames;
  expect(frames.filter((f) => f.phase === "notify-send")).toHaveLength(2);
  expect(frames.filter((f) => f.phase === "notify-response")).toHaveLength(2);
  expect(frames.at(-1)?.facts).toContainEqual(["Job status", "FAILED"]);
});
it("resets a redriven failed job budget while preserving its delivery key", () => {
  const frames = variant("permanent-redrive").frames;
  expect(
    frames.find((f) => f.title === "Operator redrives the failed job")?.facts,
  ).toContainEqual(["Attempts after redrive", "0"]);
  const sends = frames.filter((f) => f.phase === "notify-send");
  for (const frame of sends)
    expect(frame.request).toContain("external-notify:event-A");
  expect(frames.at(-1)?.facts).toContainEqual(["Job status", "SUCCEEDED"]);
});
