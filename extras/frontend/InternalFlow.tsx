import { motion, useReducedMotion } from "motion/react";
import { useEffect, useRef, useState } from "react";
import { nodeState, isResponse } from "./flow-state";
import type { Frame } from "./scenarios";
import { createPortal } from "react-dom";
import type { LucideIcon } from "lucide-react";
import {
  ArrowDown,
  ArrowRight,
  ArrowDownUp,
  Braces,
  ChevronDown,
  ChevronRight,
  Database,
  Monitor,
  PackageOpen,
  Send,
  Waypoints,
  Globe2,
} from "lucide-react";

const apiStages = [
  {
    id: "api-validate",
    title: "Validate & normalize",
    detail:
      "Validate the request and market rules. Convert decimal strings to exact integer units; reject excess precision.",
    operation: "schema → canonicalizeOrder",
  },
  {
    id: "api-key",
    title: "Identify the request",
    detail:
      "Fingerprint the normalized order and scope. Claim the idempotency key inside the transaction, or compare it with the saved request.",
    operation: "fingerprintOrder → claimKey",
  },
  {
    id: "api-save",
    title: "Accept in one transaction",
    detail:
      "Save the receipt, advance the symbol sequence, and write the order and Outbox event. Commit them together, or roll everything back.",
    operation: "receipt + sequence + order + event → COMMIT",
  },
  {
    id: "api-reply",
    title: "Return the response",
    detail:
      "A new acceptance returns 202 after commit. An identical retry replays the saved receipt; a conflicting key returns 422 and preserves the original order.",
    operation: "new → 202 · identical retry → saved receipt · conflict → 422",
  },
];

const matchingStages = [
  {
    id: "match-guard",
    title: "Deduplicate & guard sequence",
    detail:
      "Claim the Inbox event and lock matching progress. A handled event is a no-op; a sequence gap blocks processing.",
    operation: "Inbox → next acceptance sequence",
  },
  {
    id: "match-price",
    title: "Find the best opposite price",
    detail:
      "Walk asks from lowest to highest for a buy, or bids from highest to lowest for a sell. Stop if the next price does not cross the limit.",
    operation: "BUY: ask ≤ limit · SELL: bid ≥ limit",
  },
  {
    id: "match-fill",
    title: "Take the oldest maker & compute",
    detail:
      "Within a price level, use FIFO. Execute the smaller remaining quantity at the maker’s price, then continue while the taker can still match.",
    operation: "fill = min(taker remaining, maker remaining)",
  },
  {
    id: "match-save",
    title: "Commit before changing the book",
    detail:
      "Compute without mutating the book. Persist trades, order changes, Inbox and matching progress in one transaction.",
    operation: "matchOrder → persistMatch → COMMIT",
  },
  {
    id: "match-book",
    title: "Apply the book, then advance the offset",
    detail:
      "After commit, apply fills and rest any taker remainder at its limit. Advance the Kafka offset only after durable work and the book update.",
    operation: "applyMatch → offset + 1",
  },
];

type Stage = (typeof apiStages)[number];
type Tip = { stage: Stage; x: number; y: number };

function StepTrack({
  stages,
  current,
  upcoming,
  playing,
  speed,
  onTip,
  tipId,
}: {
  stages: Stage[];
  current: string | null;
  upcoming: string | null | undefined;
  playing: boolean;
  speed: number;
  onTip: (tip: Tip | null) => void;
  tipId: string | undefined;
}) {
  const reduced = useReducedMotion();
  function reveal(stage: Stage, target: HTMLElement) {
    const rect = target.getBoundingClientRect();
    onTip({
      stage,
      x: Math.max(
        12,
        Math.min(
          rect.left,
          window.innerWidth - Math.min(320, window.innerWidth - 24) - 12,
        ),
      ),
      y: Math.max(12, Math.min(rect.bottom + 10, window.innerHeight - 205)),
    });
  }
  return (
    <div className="deep-track">
      {stages.map((stage, i) => (
        <div className="deep-step-wrap" key={stage.id}>
          {i > 0 && (
            <ArrowRight
              className="deep-step-arrow"
              size={14}
              aria-hidden="true"
            />
          )}
          <button
            type="button"
            className={`deep-step${stage.id === current ? " deep-step-active" : stage.id === upcoming ? " deep-step-next" : ""}`}
            data-phase={stage.id}
            aria-label={stage.title}
            aria-current={stage.id === current ? "step" : undefined}
            aria-describedby={tipId === stage.id ? "deep-tooltip" : undefined}
            onMouseEnter={(event) => reveal(stage, event.currentTarget)}
            onMouseLeave={() => onTip(null)}
            onFocus={(event) => reveal(stage, event.currentTarget)}
            onBlur={() => onTip(null)}
            onClick={(event) => reveal(stage, event.currentTarget)}
            onKeyDown={(event) => {
              if (event.key === "Escape") onTip(null);
            }}
          >
            <span className="deep-step-number">
              {String(i + 1).padStart(2, "0")}
            </span>
            <span>{stage.title}</span>
            {stage.id === current && (
              <motion.span
                className="deep-token"
                layoutId={reduced ? undefined : "token-" + stages[0]?.id}
                transition={{ duration: playing && !reduced ? 0.3 / speed : 0 }}
              />
            )}
          </button>
        </div>
      ))}
    </div>
  );
}

function Chip({
  icon: Icon,
  title,
  state = "idle",
}: {
  icon: LucideIcon;
  title: string;
  state?: ReturnType<typeof nodeState>;
}) {
  return (
    <div className={`deep-chip state-${state}`}>
      <Icon size={18} />
      <span>{title}</span>
      {state !== "idle" && (
        <small>
          {state === "blocked"
            ? "Unavailable / uncertain"
            : state === "parallel"
              ? "Parallel branch"
              : state === "next"
                ? "Next step"
                : "Current step"}
        </small>
      )}
    </div>
  );
}

export function InternalFlow({
  frame,
  next,
  playing,
  speed,
}: {
  frame: Frame;
  next?: Frame;
  playing: boolean;
  speed: number;
}) {
  const [apiOpen, setApiOpen] = useState(true);
  const [matchOpen, setMatchOpen] = useState(true);
  const [tip, setTip] = useState<Tip | null>(null);
  const dismissTimer = useRef<number | undefined>(undefined);
  function hoverTip(next: Tip | null) {
    window.clearTimeout(dismissTimer.current);
    if (next) setTip(next);
    else dismissTimer.current = window.setTimeout(() => setTip(null), 150);
  }
  useEffect(() => () => window.clearTimeout(dismissTimer.current), []);
  useEffect(() => {
    const dismiss = (event: KeyboardEvent) => {
      if (event.key === "Escape") setTip(null);
    };
    const reposition = () => setTip(null);
    window.addEventListener("keydown", dismiss);
    window.addEventListener("scroll", reposition, true);
    window.addEventListener("resize", reposition);
    window.addEventListener("wheel", reposition, { passive: true });
    return () => {
      window.removeEventListener("keydown", dismiss);
      window.removeEventListener("scroll", reposition, true);
      window.removeEventListener("resize", reposition);
      window.removeEventListener("wheel", reposition);
    };
  }, []);
  const current = frame.phase;
  const response = isResponse(frame);
  // Each service has its own expansion state. Only authored phases animate; these are not backend spans.
  function group(
    title: string,
    id: string,
    Icon: LucideIcon,
    stages: Stage[],
    open: boolean,
    toggle: () => void,
  ) {
    return (
      <div
        className={`deep-component${open ? " is-expanded" : ""} state-${nodeState(id === "api" ? "api" : "matching", frame, next)}`}
      >
        <button
          type="button"
          className="deep-component-heading"
          aria-label={title + " internals"}
          aria-expanded={open}
          aria-controls={id + "-steps"}
          onClick={() => {
            setTip(null);
            toggle();
          }}
        >
          <Icon size={17} />
          <strong>{title}</strong>
          <span>{open ? "Collapse" : "Expand"}</span>
          {open ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        </button>
        {open && (
          <div id={id + "-steps"}>
            <StepTrack
              stages={stages}
              current={current}
              upcoming={next?.phase}
              playing={playing}
              speed={speed}
              onTip={hoverTip}
              tipId={tip?.stage.id}
            />
          </div>
        )}
      </div>
    );
  }
  return (
    <section
      className="deep-diagram"
      aria-label="Internal processing flows"
      onMouseLeave={() => hoverTip(null)}
    >
      <div className="deep-caption">
        Expanded system flow{" "}
        <span>
          Hover, focus, or tap a step · Both views follow the same request
        </span>
      </div>
      <div
        className={`deep-transfer-label${response ? " transfer-response" : ""}`}
      >
        <strong>{response ? "Response · " : "Request / event · "}</strong>
        {frame.transfers.length
          ? frame.transfers.map(([from, to]) => `${from} → ${to}`).join(" · ")
          : frame.active.join(" · ") + " · local decision"}
      </div>
      <div className="deep-entry">
        <Chip
          icon={Monitor}
          title="Client"
          state={nodeState("client", frame, next)}
        />
        <ArrowRight
          className={`deep-connector${frame.transfers.some(([from, to]) => from === "api" && to === "client") ? " connector-response" : ""}`}
          size={18}
        />
        {group("Order API", "api", Braces, apiStages, apiOpen, () =>
          setApiOpen(!apiOpen),
        )}
        <ArrowRight className="deep-connector" size={18} />
        <Chip
          icon={Database}
          title="PostgreSQL"
          state={nodeState("database", frame, next)}
        />
      </div>
      <div className="deep-bus">
        <span>After acceptance commits</span>
        <ArrowDown size={17} />
        <Chip
          icon={PackageOpen}
          title="Outbox"
          state={nodeState("outbox", frame, next)}
        />
        <ArrowRight size={18} />
        <Chip
          icon={Waypoints}
          title="Kafka"
          state={nodeState("kafka", frame, next)}
        />
      </div>
      <div className="deep-fork">
        <span />
        <ArrowDown size={17} />
        <span />
        <ArrowDown size={17} />
        <span />
      </div>
      <div className="deep-consumers">
        <div>
          <div className="deep-group-label">matching consumer group</div>
          {group(
            "Matching",
            "match",
            ArrowDownUp,
            matchingStages,
            matchOpen,
            () => setMatchOpen(!matchOpen),
          )}
        </div>
        <div>
          <div className="deep-group-label">independent notification group</div>
          <div className="deep-delivery">
            <Chip
              icon={Send}
              title="Durable job"
              state={nodeState("worker", frame, next)}
            />
            <ArrowRight
              className={
                frame.transfers.some(
                  ([from, to]) => from === "external" && to === "worker",
                )
                  ? "connector-response"
                  : ""
              }
              size={18}
            />
            <Chip
              icon={Globe2}
              title="External API"
              state={nodeState("external", frame, next)}
            />
          </div>
          <p>
            Retries reuse the same delivery key. Notification and matching
            progress independently.
          </p>
        </div>
      </div>
      <div className="deep-footnote">
        Existing request key: same meaning → replay receipt; different meaning →
        422. Matching: compute → commit → apply book → advance offset.
      </div>
      {tip &&
        createPortal(
          <div
            id="deep-tooltip"
            role="tooltip"
            onMouseEnter={() => {
              window.clearTimeout(dismissTimer.current);
            }}
            onMouseLeave={() => hoverTip(null)}
            className="deep-tooltip"
            style={{ left: tip.x, top: tip.y }}
          >
            <strong>{tip.stage.title}</strong>
            <p>{tip.stage.detail}</p>
            <code>{tip.stage.operation}</code>
          </div>,
          document.body,
        )}
    </section>
  );
}
