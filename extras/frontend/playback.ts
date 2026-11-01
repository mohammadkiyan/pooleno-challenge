export interface Playback {
  frame: number;
  status: "ready" | "playing" | "paused" | "complete";
}

export type PlaybackAction =
  | { type: "run" | "resume" | "pause" | "reset" }
  | { type: "advance"; total: number }
  | { type: "seek"; frame: number; total: number }
  | { type: "step"; direction: -1 | 1; total: number };

export const initialPlayback: Playback = { frame: 0, status: "ready" };

export function playbackReducer(
  state: Playback,
  action: PlaybackAction,
): Playback {
  switch (action.type) {
    case "run":
      return { frame: 0, status: "playing" };
    case "resume":
      return { ...state, status: "playing" };
    case "pause":
      return state.status === "playing"
        ? { ...state, status: "paused" }
        : state;
    case "reset":
      return initialPlayback;
    case "seek":
      return { frame: Math.max(0, Math.min(action.frame, action.total - 1)), status: "paused" };
    case "advance": {
      if (state.status !== "playing") return state;
      const frame = Math.min(state.frame + 1, action.total - 1);
      return {
        frame,
        status: frame === action.total - 1 ? "complete" : "playing",
      };
    }
    case "step": {
      const frame = Math.max(
        0,
        Math.min(state.frame + action.direction, action.total - 1),
      );
      return {
        frame,
        status: frame === action.total - 1 ? "complete" : "paused",
      };
    }
  }
}
