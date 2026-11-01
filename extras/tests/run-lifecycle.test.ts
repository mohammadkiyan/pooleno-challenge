import { afterEach, expect, it, vi } from "vitest";
import { execute } from "../server/execute.ts";
import { Sandbox } from "../server/sandbox.ts";
import type { LiveRun } from "../shared/live.ts";
afterEach(() => vi.restoreAllMocks());
it("keeps a failed run busy until sandbox cleanup settles", async () => {
  const run: LiveRun = { id: "00000000-0000-4000-8000-000000000001", scenarioId: "matching", variantId: "", status: "running", startedAt: new Date().toISOString(), frames: [], checks: [], logs: [] };
  let finishCleanup!: () => void;
  let cleanupStarted!: () => void;
  const started = new Promise<void>(resolve => { cleanupStarted = resolve; });
  // Control only the process/database boundary to reproduce an in-flight cleanup.
  vi.spyOn(Sandbox.prototype, "setup").mockRejectedValue(new Error("Database unavailable"));
  vi.spyOn(Sandbox.prototype, "cleanup").mockImplementation(() => {
    cleanupStarted();
    return new Promise<void>(resolve => { finishCleanup = resolve; });
  });
  const completion = execute(run, new AbortController().signal);
  await started;
  try {
    expect(run.error).toBe("Database unavailable");
    expect(run.status).toBe("running");
  } finally { finishCleanup(); await completion; }
  expect(run.status).toBe("failed");
  expect(run.finishedAt).toBeDefined();
});
