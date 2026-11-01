import type { Frame, NodeId } from "./scenarios";
export function nodeState(id: NodeId, frame: Frame, next?: Frame) {
  if (frame.blocked.includes(id)) return "blocked";
  if (frame.parallel.includes(id)) return "parallel";
  if (frame.active.includes(id)) return "current";
  if (next?.active.includes(id)) return "next";
  return "idle";
}
export function isResponse(frame: Frame) {
  return frame.transfers.some(
    ([from, to]) =>
      (from === "api" && to === "client") ||
      (from === "external" && to === "worker"),
  );
}
