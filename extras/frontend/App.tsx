import { useEffect, useReducer, useState } from "react";
import {
  AnimatePresence,
  MotionConfig,
  motion,
  useReducedMotion,
} from "motion/react";
import {
  ArrowDown,
  ArrowDownUp,
  ArrowRight,
  ArrowUpRight,
  Braces,
  Check,
  ChevronLeft,
  ChevronRight,
  CircleHelp,
  Database,
  FlaskConical,
  Globe2,
  Info,
  Layers3,
  Monitor,
  PackageOpen,
  Pause,
  Play,
  RotateCcw,
  Send,
  ShieldCheck,
  Waypoints,
  Zap,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { initialPlayback, playbackReducer } from "./playback";
import { FlowCanvas } from "./FlowCanvas";
import { nodeState } from "./flow-state";
import { InternalFlow } from "./InternalFlow";
import { firstScenario, scenarios } from "./scenarios";
import type { BookOrder, Frame, NodeId } from "./scenarios";

const icons: LucideIcon[] = [
  ArrowDownUp,
  Layers3,
  RotateCcw,
  ShieldCheck,
  Send,
  Waypoints,
];

const nodes: {
  id: NodeId;
  title: string;
  subtitle: string;
  icon: LucideIcon;
  x: number;
  y: number;
}[] = [
  {
    id: "client",
    title: "Client",
    subtitle: "Order request",
    icon: Monitor,
    x: 9,
    y: 25,
  },
  {
    id: "api",
    title: "Order API",
    subtitle: "Validate & accept",
    icon: Braces,
    x: 29,
    y: 25,
  },
  {
    id: "database",
    title: "PostgreSQL",
    subtitle: "Durable records",
    icon: Database,
    x: 49,
    y: 25,
  },
  {
    id: "outbox",
    title: "Outbox",
    subtitle: "Ordered publication",
    icon: PackageOpen,
    x: 69,
    y: 25,
  },
  {
    id: "kafka",
    title: "Kafka",
    subtitle: "Acceptance events",
    icon: Waypoints,
    x: 89,
    y: 25,
  },
  {
    id: "matching",
    title: "Matching",
    subtitle: "Book & trades",
    icon: ArrowDownUp,
    x: 89,
    y: 79,
  },
  {
    id: "worker",
    title: "Notification",
    subtitle: "Durable retries",
    icon: Send,
    x: 49,
    y: 79,
  },
  {
    id: "external",
    title: "External API",
    subtitle: "Idempotent effects",
    icon: Globe2,
    x: 15,
    y: 79,
  },
];

const edges: {
  from: NodeId;
  to: NodeId;
  path: string;
  x: number[];
  y: number[];
  localOnly?: boolean;
}[] = [
  {
    from: "client",
    to: "api",
    path: "M160 65H220",
    x: [160, 220],
    y: [65, 65],
  },
  {
    from: "api",
    to: "client",
    path: "M220 84H160",
    x: [220, 160],
    y: [84, 84],
  },
  {
    from: "api",
    to: "database",
    path: "M360 65H420",
    x: [360, 420],
    y: [65, 65],
  },
  {
    from: "database",
    to: "outbox",
    path: "M560 65H620",
    x: [560, 620],
    y: [65, 65],
  },
  {
    from: "outbox",
    to: "kafka",
    path: "M760 65H820",
    x: [760, 820],
    y: [65, 65],
  },
  {
    from: "kafka",
    to: "matching",
    path: "M890 104V164",
    x: [890, 890],
    y: [104, 164],
  },
  {
    from: "kafka",
    to: "worker",
    path: "M860 104V135H490V164",
    x: [860, 860, 490, 490],
    y: [104, 135, 135, 164],
  },
  {
    from: "worker",
    to: "external",
    path: "M420 205H220",
    x: [420, 220],
    y: [205, 205],
  },
  {
    from: "external",
    to: "worker",
    path: "M220 225H420",
    x: [220, 420],
    y: [225, 225],
  },
  {
    from: "matching",
    to: "database",
    path: "M960 205H983V10H490V26",
    x: [960, 983, 983, 490, 490],
    y: [205, 205, 10, 10, 26],
    localOnly: true,
  },
  {
    from: "worker",
    to: "database",
    path: "M490 164V105",
    x: [490, 490],
    y: [164, 105],
    localOnly: true,
  },
];

function SystemFlow({
  frame,
  index,
  playing,
  next,
}: {
  frame: Frame;
  next?: Frame;
  index: number;
  playing: boolean;
}) {
  const reducedMotion = useReducedMotion();
  return (
    <div
      className="system-diagram"
      aria-label="Order processing flow. Kafka feeds matching and notification independently."
    >
      <svg
        className="connections"
        viewBox="0 0 1000 260"
        preserveAspectRatio="none"
        aria-hidden="true"
      >
        <defs>
          <marker
            id="arrow"
            viewBox="0 0 10 10"
            refX="8"
            refY="5"
            markerWidth="5"
            markerHeight="5"
            orient="auto-start-reverse"
          >
            <path
              d="M0 1L8 5L0 9"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.5"
            />
          </marker>
        </defs>
        {edges.map((edge) => {
          const blocked =
            frame.blocked.includes(edge.from) ||
            frame.blocked.includes(edge.to);
          const active =
            !blocked &&
            frame.transfers.some(
              ([from, to]) => from === edge.from && to === edge.to,
            );
          if (edge.localOnly && !active) return null;
          const response =
            (edge.from === "api" && edge.to === "client") ||
            (edge.from === "external" && edge.to === "worker");
          return (
            <g
              key={edge.from + edge.to}
              className={
                (response ? "edge-response " : "edge-request ") +
                (blocked
                  ? "edge edge-blocked"
                  : active
                    ? "edge edge-active"
                    : "edge")
              }
            >
              <path
                d={edge.path}
                fill="none"
                stroke="currentColor"
                strokeWidth="1.5"
                markerEnd="url(#arrow)"
              />
              {active && playing && !reducedMotion && (
                <motion.circle
                  key={`${index}-${edge.from}`}
                  r="4"
                  fill="currentColor"
                  initial={{ cx: edge.x[0], cy: edge.y[0], opacity: 0 }}
                  animate={{ cx: edge.x, cy: edge.y, opacity: [0, 1, 1, 0] }}
                  transition={{ duration: 1.25, ease: "linear" }}
                />
              )}
            </g>
          );
        })}
      </svg>
      {nodes.map(({ id, title, subtitle, icon: Icon, x, y }) => {
        const state = nodeState(id, frame, next);
        const blocked = state === "blocked";
        const active = !blocked && frame.active.includes(id);
        return (
          <div
            key={id}
            className={`system-node node-${state}${active && state === "current" ? " node-active" : ""}`}
            style={{ left: `${x}%`, top: `${y}%` }}
          >
            <div className="node-top">
              <Icon size={18} strokeWidth={1.7} />
              <span className="node-indicator" />
            </div>
            <strong>{title}</strong>
            <span>
              {state === "parallel"
                ? "Parallel branch"
                : state === "next"
                  ? "Next step"
                  : blocked
                    ? "Unavailable / uncertain"
                    : active
                      ? "Current step"
                      : subtitle}
            </span>
          </div>
        );
      })}
      <span className="branch-label">Independent consumer groups</span>
    </div>
  );
}

function BookSide({
  orders,
  side,
  animate,
}: {
  orders: BookOrder[];
  side: "BUY" | "SELL";
  animate: boolean;
}) {
  return (
    <div className={`book-side ${side.toLowerCase()}`}>
      <div className="book-side-label">
        {side === "SELL" ? <ArrowDown size={13} /> : <ArrowUpRight size={13} />}{" "}
        {side === "SELL" ? "Asks · sell orders" : "Bids · buy orders"}
        <span>{orders.length}</span>
      </div>
      <AnimatePresence initial={false}>
        {orders.map((order) => (
          <motion.div
            layout={animate}
            key={order.id}
            className="book-row"
            initial={animate ? { opacity: 0, y: -6 } : false}
            animate={{ opacity: 1, y: 0 }}
            exit={animate ? { opacity: 0, y: 6 } : { opacity: 0 }}
            transition={{ duration: animate ? 0.25 : 0 }}
          >
            <span>{order.price}</span>
            <span>{order.quantity}</span>
            <span className="order-id">{order.id}</span>
          </motion.div>
        ))}
      </AnimatePresence>
      {orders.length === 0 && (
        <div className="empty-book">
          No resting {side === "SELL" ? "sell" : "buy"} orders
        </div>
      )}
    </div>
  );
}

export function App() {
  const [selectedIndex, setSelectedIndex] = useState(0);
  const [playback, dispatch] = useReducer(playbackReducer, initialPlayback);
  const [speed, setSpeed] = useState(1);
  const [showInternals, setShowInternals] = useState(false);
  const [tab, setTab] = useState<"explanation" | "request">("explanation");
  const reducedMotion = useReducedMotion();
  const scenario = scenarios[selectedIndex] ?? firstScenario;
  const frame = scenario.frames[playback.frame];
  if (!frame) throw new Error("The selected scenario frame is missing");
  const total = scenario.frames.length;
  const playing = playback.status === "playing";
  const complete = playback.status === "complete";

  useEffect(() => {
    if (!playing) return;
    const timer = window.setTimeout(() => {
      dispatch({ type: "advance", total });
    }, 2100 / speed);
    return () => {
      window.clearTimeout(timer);
    };
  }, [playing, playback.frame, speed, total]);

  function selectScenario(index: number) {
    setSelectedIndex(index);
    setTab("explanation");
    dispatch({ type: "run" });
  }

  function togglePlayback() {
    if (playing) dispatch({ type: "pause" });
    else if (playback.status === "paused") dispatch({ type: "resume" });
    else dispatch({ type: "run" });
  }

  return (
    <MotionConfig reducedMotion="user">
      <div className="app-shell">
        <a className="skip-link" href="#scenario-content">
          Skip to scenario
        </a>
        <header className="masthead">
          <a className="brand" href="./" aria-label="Pooleno scenario lab home">
            <span className="brand-mark">
              <ArrowDownUp size={23} />
            </span>
            <span>
              pooleno<span className="brand-divider">/</span>
              <span className="brand-section">scenario lab</span>
            </span>
          </a>
          <div className="header-label">
            <span className="preview-dot" />
            Illustrated preview
            <span className="optional-label">OPTIONAL EXTRA</span>
          </div>
        </header>

        <div className="workspace">
          <aside className="sidebar" aria-label="Prepared scenarios">
            <div className="sidebar-heading">
              <span>SCENARIOS</span>
              <span className="count">06</span>
            </div>
            <p className="sidebar-description">
              One click. One edge case.
              <br />
              Follow the expected behavior.
            </p>
            {(["Matching", "Reliability"] as const).map((category) => (
              <div className="scenario-group" key={category}>
                <h2>{category}</h2>
                {scenarios.map((item, index) => {
                  if (item.category !== category) return null;
                  const Icon = icons[index] ?? FlaskConical;
                  return (
                    <button
                      type="button"
                      key={item.id}
                      className={`scenario-button${selectedIndex === index ? " selected" : ""}`}
                      aria-pressed={selectedIndex === index}
                      onClick={() => {
                        selectScenario(index);
                      }}
                    >
                      <span className="scenario-icon">
                        <Icon size={18} strokeWidth={1.7} />
                      </span>
                      <span className="scenario-label">
                        <strong>{item.title}</strong>
                        <span>{item.tags[0]}</span>
                      </span>
                      <ChevronRight size={14} className="scenario-chevron" />
                    </button>
                  );
                })}
              </div>
            ))}
            <div className="sidebar-note">
              <FlaskConical size={17} />
              <div>
                <strong>A window into the backend</strong>
                <p>
                  Optional visual walkthroughs, separate from the challenge
                  implementation.
                </p>
              </div>
            </div>
          </aside>

          <main id="scenario-content" className="main-content">
            <section
              className="scenario-heading"
              aria-labelledby="scenario-title"
            >
              <div>
                <div className="section-kicker">
                  SCENARIO {String(selectedIndex + 1).padStart(2, "0")}{" "}
                  <span>/ 06</span>
                </div>
                <h2 id="scenario-title">{scenario.title}</h2>
              </div>
              <button
                type="button"
                className="primary-button"
                onClick={togglePlayback}
              >
                {playing ? (
                  <Pause size={16} fill="currentColor" />
                ) : complete ? (
                  <RotateCcw size={16} />
                ) : (
                  <Play size={16} fill="currentColor" />
                )}
                {playing
                  ? "Pause"
                  : complete
                    ? "Replay scenario"
                    : playback.status === "paused"
                      ? "Resume"
                      : "Run scenario"}
              </button>
            </section>

            <div className="preview-note">
              <Info size={15} />
              <span>
                Authored illustration of expected behavior. This preview does
                not connect to the backend.
              </span>
            </div>

            <div className="request-context">
              <span>FOLLOWING</span>
              <strong>{frame.requestLabel}</strong>
              <span>{frame.request.split("\n")[0]}</span>
            </div>
            <section
              className="panel flow-panel"
              aria-labelledby="flow-heading"
            >
              <div className="panel-heading">
                <h3 id="flow-heading">
                  <Waypoints size={16} />
                  System flow
                </h3>
                <div className="flow-heading-controls">
                  <span className="flow-view-label">
                    {showInternals ? "View 2 · Detailed" : "View 1 · Overview"}
                  </span>
                  <label className="detail-toggle">
                    <input
                      type="checkbox"
                      checked={showInternals}
                      onChange={(event) => {
                        setShowInternals(event.target.checked);
                      }}
                    />
                    View 2 · Show internals
                  </label>
                </div>
              </div>
              <div className="flow-legend flow-legend-row">
                <span>
                  <i className="legend-dot" />
                  Current
                </span>
                <span>
                  <i className="legend-dot legend-next" />
                  Next
                </span>
                <span>
                  <i className="legend-dot legend-parallel" />
                  Parallel branches
                </span>
                <span>
                  <i className="legend-line legend-request" />
                  Request path
                </span>
                <span>
                  <i className="legend-line legend-response" />
                  Response path
                </span>
              </div>
              <div className="flow-now">
                <strong>Now: {frame.title}</strong>
                <span>
                  Next:{" "}
                  {scenario.frames[playback.frame + 1]?.title ??
                    "End of walkthrough"}
                </span>
              </div>
              <FlowCanvas>
                {showInternals ? (
                  <InternalFlow
                    frame={frame}
                    next={scenario.frames[playback.frame + 1]}
                    playing={playing}
                    speed={speed}
                  />
                ) : (
                  <SystemFlow
                    frame={frame}
                    next={scenario.frames[playback.frame + 1]}
                    index={playback.frame}
                    playing={playing}
                  />
                )}
              </FlowCanvas>
              <div className="playback-bar">
                <div className="step-controls">
                  <button
                    type="button"
                    className="icon-button"
                    aria-label="Previous step"
                    disabled={playback.frame === 0}
                    onClick={() => {
                      dispatch({ type: "step", direction: -1, total });
                    }}
                  >
                    <ChevronLeft size={17} />
                  </button>
                  <button
                    type="button"
                    className="icon-button"
                    aria-label={playing ? "Pause playback" : "Play playback"}
                    onClick={togglePlayback}
                  >
                    {playing ? <Pause size={15} /> : <Play size={15} />}
                  </button>
                  <button
                    type="button"
                    className="icon-button"
                    aria-label="Next step"
                    disabled={playback.frame === total - 1}
                    onClick={() => {
                      dispatch({ type: "step", direction: 1, total });
                    }}
                  >
                    <ChevronRight size={17} />
                  </button>
                  <span className="step-count">
                    {String(playback.frame + 1).padStart(2, "0")}
                    <span> / {String(total).padStart(2, "0")}</span>
                  </span>
                </div>
                <div
                  className="step-track"
                  role="progressbar"
                  aria-label="Illustration progress"
                  aria-valuemin={1}
                  aria-valuemax={total}
                  aria-valuenow={playback.frame + 1}
                >
                  {scenario.frames.map((_, index) => (
                    <span
                      key={index}
                      className={index <= playback.frame ? "filled" : ""}
                    />
                  ))}
                </div>
                <label className="speed-control">
                  <span>Speed</span>
                  <select
                    aria-label="Playback speed"
                    value={speed}
                    onChange={(event) => {
                      setSpeed(Number(event.target.value));
                    }}
                  >
                    <option value={0.5}>0.5×</option>
                    <option value={1}>1×</option>
                    <option value={2}>2×</option>
                  </select>
                </label>
              </div>
            </section>

            <div className="detail-grid">
              <section
                className="panel explanation-panel"
                aria-label="Step details"
              >
                <div
                  className="detail-tabs"
                  role="tablist"
                  aria-label="Step information"
                  onKeyDown={(event) => {
                    if (
                      !["ArrowLeft", "ArrowRight", "Home", "End"].includes(
                        event.key,
                      )
                    )
                      return;
                    event.preventDefault();
                    const next =
                      event.key === "Home"
                        ? "explanation"
                        : event.key === "End"
                          ? "request"
                          : tab === "explanation"
                            ? "request"
                            : "explanation";
                    setTab(next);
                    document.getElementById(`${next}-tab`)?.focus();
                  }}
                >
                  <button
                    type="button"
                    id="explanation-tab"
                    role="tab"
                    aria-selected={tab === "explanation"}
                    tabIndex={tab === "explanation" ? 0 : -1}
                    aria-controls="explanation-content"
                    onClick={() => {
                      setTab("explanation");
                    }}
                  >
                    <CircleHelp size={15} />
                    What’s happening
                  </button>
                  <button
                    type="button"
                    id="request-tab"
                    role="tab"
                    aria-selected={tab === "request"}
                    tabIndex={tab === "request" ? 0 : -1}
                    aria-controls="request-content"
                    onClick={() => {
                      setTab("request");
                    }}
                  >
                    <Braces size={15} />
                    Example data
                  </button>
                </div>
                <div
                  id="explanation-content"
                  role="tabpanel"
                  aria-labelledby="explanation-tab"
                  hidden={tab !== "explanation"}
                >
                  <div className="current-step">
                    <span
                      className={`step-badge${complete ? " complete" : ""}`}
                    >
                      {complete ? <Check size={15} /> : <Zap size={15} />}
                    </span>
                    <span>
                      {complete
                        ? "ILLUSTRATION COMPLETE"
                        : `STEP ${String(playback.frame + 1).padStart(2, "0")}`}
                    </span>
                  </div>
                  <div
                    className="step-copy"
                    aria-live="polite"
                    aria-atomic="true"
                  >
                    <h3>{frame.title}</h3>
                    <p>{frame.detail}</p>
                    <div className="decision-note">
                      <strong>Decision</strong>
                      <span>{frame.decision}</span>
                      <code>{frame.source}</code>
                    </div>
                  </div>
                  <div className="facts">
                    {frame.facts.map(([label, value]) => (
                      <div key={label}>
                        <span>{label}</span>
                        <strong>{value}</strong>
                      </div>
                    ))}
                  </div>
                  <div className="guarantee">
                    <ShieldCheck size={17} />
                    <div>
                      <span>The rule behind the result</span>
                      <p>{scenario.guarantee}</p>
                    </div>
                  </div>
                </div>
                <div
                  id="request-content"
                  className="request-content"
                  role="tabpanel"
                  aria-labelledby="request-tab"
                  hidden={tab !== "request"}
                >
                  <span className="code-label">ILLUSTRATIVE REQUEST</span>
                  <pre>{frame.request}</pre>
                  <span className="code-label">EXPECTED RESPONSE / STATE</span>
                  <pre>{frame.response}</pre>
                </div>
              </section>

              <section
                className="panel book-panel"
                aria-labelledby="book-heading"
              >
                <div className="panel-heading">
                  <h3 id="book-heading">
                    <Layers3 size={16} />
                    Order book
                  </h3>
                  <span className="market-label">
                    ETH <span>/</span> USD
                  </span>
                </div>
                <div className="book-columns">
                  <span>Price · USD</span>
                  <span>Size · ETH</span>
                  <span>Order</span>
                </div>
                <BookSide
                  orders={frame.book.filter((order) => order.side === "SELL")}
                  side="SELL"
                  animate={!reducedMotion}
                />
                <div className="book-separator">
                  <span />
                  Best price, then arrival order
                  <span />
                </div>
                <BookSide
                  orders={frame.book.filter((order) => order.side === "BUY")}
                  side="BUY"
                  animate={!reducedMotion}
                />
                <div className="trade-heading">
                  <span>Executions</span>
                  <span>{frame.trades.length} trades</span>
                </div>
                {frame.trades.length === 0 ? (
                  <div className="empty-trades">
                    Trades appear here when orders cross.
                  </div>
                ) : (
                  <div className="trades">
                    {frame.trades.map((trade, index) => (
                      <motion.div
                        key={trade.maker}
                        initial={reducedMotion ? false : { opacity: 0, x: -6 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ duration: reducedMotion ? 0 : 0.2 }}
                      >
                        <span className="trade-number">{index + 1}</span>
                        <span>
                          {trade.maker}
                          <ArrowRight size={12} />
                          B1
                        </span>
                        <strong>
                          {trade.quantity} <span>ETH</span>
                        </strong>
                        <span>@ {trade.price}</span>
                      </motion.div>
                    ))}
                  </div>
                )}
              </section>
            </div>

            <section
              className={`expected-outcome${complete ? " outcome-complete" : ""}`}
              aria-label="Expected scenario outcome"
            >
              <span className="outcome-icon">
                <ShieldCheck size={20} />
              </span>
              <div>
                <span>EXPECTED OUTCOME</span>
                <p>{scenario.outcome}</p>
              </div>
              <span className="outcome-label">
                {complete ? "Illustrated" : "Follow the flow"}
                <ArrowUpRight size={14} />
              </span>
            </section>
            <footer className="page-footer">
              <span>
                <FlaskConical size={13} />
                Built to explain the implementation.
              </span>
              <span>
                Frontend preview · Backend verification lives in the test suite.
              </span>
            </footer>
          </main>
        </div>
      </div>
    </MotionConfig>
  );
}
