import { describe, expect, it } from "vitest";
import { initialPlayback, playbackReducer } from "../frontend/playback";

describe("scenario playback", () => {
  it("starts a fresh run from the first frame, including after a completed run", () => {
    expect(
      playbackReducer({ frame: 6, status: "complete" }, { type: "run" }),
    ).toEqual({ frame: 0, status: "playing" });
  });

  it("does not advance after the viewer pauses", () => {
    const paused = playbackReducer(
      { frame: 2, status: "playing" },
      { type: "pause" },
    );
    expect(paused).toEqual({ frame: 2, status: "paused" });
    expect(playbackReducer(paused, { type: "advance", total: 6 })).toEqual(
      paused,
    );
  });

  it("resumes at the paused step rather than starting over", () => {
    const resumed = playbackReducer(
      { frame: 2, status: "paused" },
      { type: "resume" },
    );
    expect(resumed).toEqual({ frame: 2, status: "playing" });
    expect(playbackReducer(resumed, { type: "advance", total: 6 })).toEqual({
      frame: 3,
      status: "playing",
    });
  });

  it("stops on the final frame instead of going out of bounds", () => {
    const completed = playbackReducer(
      { frame: 4, status: "playing" },
      { type: "advance", total: 6 },
    );
    expect(completed).toEqual({ frame: 5, status: "complete" });
    expect(playbackReducer(completed, { type: "advance", total: 6 })).toEqual(
      completed,
    );
  });

  it("lets the reviewer go backward from the result and prevents stepping before the start", () => {
    expect(
      playbackReducer(
        { frame: 5, status: "complete" },
        { type: "step", direction: -1, total: 6 },
      ),
    ).toEqual({ frame: 4, status: "paused" });
    expect(
      playbackReducer(initialPlayback, {
        type: "step",
        direction: -1,
        total: 6,
      }).frame,
    ).toBe(0);
  });

  it("resets the prior scenario state when another scenario is selected", () => {
    expect(
      playbackReducer({ frame: 5, status: "playing" }, { type: "reset" }),
    ).toEqual(initialPlayback);
  });
});

it("selects the latest observed frame without starting another execution", () => {
  expect(playbackReducer(initialPlayback, { type: "seek", frame: 9, total: 10 })).toEqual({ frame: 9, status: "paused" });
  expect(playbackReducer(initialPlayback, { type: "seek", frame: 99, total: 10 }).frame).toBe(9);
});
