import { accept, guard, next, publish, scenarios } from "./scenarios";
import type { BookOrder, Frame, Trade } from "./scenarios";
import { scenarioCoverage } from "./scenario-coverage";
import type { Coverage } from "./scenario-coverage";

export interface WalkthroughVariant {
  id: string;
  label: string;
  description: string;
  guarantee: string;
  outcome: string;
  frames: Frame[];
  coverage: Coverage;
}
const api = "src/modules/orders/";
const match = "src/modules/matching/";
const external = "src/modules/external/";
function coverage(family: string, changes: Partial<Coverage>): Coverage {
  const base = scenarioCoverage[family];
  if (!base) throw new Error("Missing scenario coverage");
  return { ...base, ...changes };
}
function commitAndApply(
  frames: Frame[],
  book: BookOrder[],
  trades: Trade[],
  facts: [string, string][],
) {
  next(
    frames,
    "Commit the planned match",
    "Persist trades, maker/taker changes, Inbox and matching progress together. The book shown still represents the preceding cache state.",
    ["matching", "database"],
    "The whole match commits, or none of its updates do.",
    match + "apply-accepted-order.ts → persistMatch",
    { phase: "match-save", transfers: [["matching", "database"]], trades },
  );
  next(
    frames,
    "Apply the committed book",
    "After commit, apply fills and rest any taker remainder at its own limit. Then the consumer may advance its offset.",
    ["matching"],
    "Apply only committed changes; preserve each remaining maker’s FIFO position.",
    match + "matching-engine.ts → applyMatch",
    { phase: "match-book", book, trades, facts },
  );
}
const bids: BookOrder[] = [
  { id: "B3", side: "BUY", price: "101.00", quantity: "1.0000" },
  { id: "B1", side: "BUY", price: "100.00", quantity: "2.0000" },
  { id: "B2", side: "BUY", price: "100.00", quantity: "3.0000" },
];
const sell = accept(
  "S1",
  "SELL",
  "100",
  "4",
  "Precondition: B1 then B2 rested at 100; later B3 bid 101. Follow the sell taker S1. Highest bid wins, then FIFO.",
  bids,
);
publish(sell);
guard(sell);
next(
  sell,
  "Choose the highest bid first",
  "B3 bids 101, above S1’s sell limit of 100. It has price priority over older bids at 100.",
  ["matching"],
  "SELL crosses when bid ≥ limit. Maker pricing executes B3 at 101.",
  match + "matching-engine.ts → crosses",
  { phase: "match-price" },
);
next(
  sell,
  "Compute SELL-side FIFO fills",
  "Plan 1 ETH from B3 at 101, 2 from B1 at 100, then 1 from B2 at 100. This is a plan; the book remains unchanged until persistence succeeds.",
  ["matching"],
  "Highest bid first, oldest at a shared price next.",
  match + "matching-engine.ts → matchOrder",
  {
    phase: "match-fill",
    facts: [
      ["S1 left to match", "0.0000"],
      ["B2 planned remainder", "2.0000"],
    ],
  },
);
commitAndApply(
  sell,
  [{ ...bids[2]!, quantity: "2.0000" }],
  [
    { maker: "B3", taker: "S1", price: "101.00", quantity: "1.0000" },
    { maker: "B1", taker: "S1", price: "100.00", quantity: "2.0000" },
    { maker: "B2", taker: "S1", price: "100.00", quantity: "1.0000" },
  ],
  [
    ["S1 status", "FILLED"],
    ["B2 remaining", "2.0000"],
  ],
);

const sellers: BookOrder[] = [
  { id: "S1", side: "SELL", price: "99.00", quantity: "1.0000" },
  { id: "S2", side: "SELL", price: "105.00", quantity: "1.0000" },
];
const partial = accept(
  "B1",
  "BUY",
  "100",
  "3",
  "Precondition: S1 offers 1 at 99; S2 offers 1 at 105. B1 buys 3 with a limit of 100.",
  sellers,
);
publish(partial);
guard(partial);
next(
  partial,
  "Only the first maker crosses",
  "S1 at 99 crosses B1’s limit. The next ask, S2 at 105, is too expensive.",
  ["matching"],
  "99 ≤ 100 → fill; 105 > 100 → stop.",
  match + "matching-engine.ts → crosses",
  { phase: "match-price" },
);
next(
  partial,
  "Compute a partial taker fill",
  "Plan 1 ETH at the maker price of 99. B1 still has 2 ETH remaining and cannot consume the ask at 105.",
  ["matching"],
  "Keep the unfilled taker quantity; never raise its price limit.",
  match + "matching-engine.ts → matchOrder",
  {
    phase: "match-fill",
    facts: [
      ["B1 executed", "1.0000"],
      ["B1 remainder", "2.0000"],
    ],
  },
);
commitAndApply(
  partial,
  [sellers[1]!, { id: "B1", side: "BUY", price: "100.00", quantity: "2.0000" }],
  [{ maker: "S1", price: "99.00", quantity: "1.0000" }],
  [
    ["B1 status", "PARTIALLY_FILLED"],
    ["B1 resting quantity", "2.0000"],
  ],
);

const crash: Frame[] = scenarios[0].frames
  .slice(0, -1)
  .map((frame) => ({ ...frame }));
const persistedBook: BookOrder[] = [
  { id: "S2", side: "SELL", price: "100.00", quantity: "2.0000" },
];
next(
  crash,
  "Crash after the matching commit",
  "The database has all three trades, order changes, Inbox and progress. The process dies before applying its book update or committing its Kafka offset. The old RAM cache is discarded.",
  ["matching"],
  "A committed result survives; the uncommitted offset permits redelivery.",
  match + "apply-accepted-order.ts",
  {
    phase: null,
    blocked: ["matching"],
    bookState: "discarded",
    facts: [
      ["Persisted trades", "3"],
      ["Kafka offset", "Not advanced"],
    ],
    response:
      "Database commit: durable\nBook cache: unavailable\nOffset: before original event",
  },
);
next(
  crash,
  "Restore from committed rows",
  "On restart, rebuild the book from committed OPEN/PARTIALLY_FILLED rows in acceptance order and restore matching progress.",
  ["database", "matching"],
  "Restore persisted state, rather than reapplying the old uncommitted RAM plan.",
  match + "restore-book.ts",
  {
    phase: "match-restore",
    blocked: [],
    bookState: "available",
    book: persistedBook,
    facts: [
      ["Restored S2 quantity", "2.0000"],
      ["Persisted trades", "3"],
    ],
  },
);
next(
  crash,
  "Redeliver the original event",
  "The consumer resumes from its old committed offset, so Kafka returns the same acceptance event. This is not a new client request.",
  ["kafka", "matching"],
  "At-least-once delivery may repeat an event already reflected in the database.",
  "src/modules/messaging/matching-consumer.ts",
  { phase: "match-replay", transfers: [["kafka", "matching"]] },
);
next(
  crash,
  "The Inbox makes replay a no-op",
  "The saved Inbox entry identifies this exact event as handled. No match is computed and no trade or quantity is written again.",
  ["matching", "database"],
  "Existing event identity → skip execution; preserve the restored book.",
  match + "apply-accepted-order.ts → claimInboxEvent",
  {
    phase: "match-replay",
    transfers: [["matching", "database"]],
    facts: [
      ["Persisted trades", "3"],
      ["New trades on replay", "0"],
    ],
  },
);
next(
  crash,
  "Advance the replayed offset safely",
  "The consumer can advance beyond the handled event. S2 still has 2 ETH and the trade count remains three.",
  ["matching"],
  "Deduplicate before acknowledging replay; never execute a second match.",
  "src/modules/messaging/matching-consumer.ts",
  {
    phase: "match-replay",
    facts: [
      ["Persisted trades", "3"],
      ["New trades on replay", "0"],
    ],
    response: "Same event: handled\nTrades: still 3\nS2 remaining: still 2 ETH",
  },
);

function invalid(precision: boolean): Frame[] {
  const frames = accept(
    "invalid-order",
    "BUY",
    precision ? "100.001" : "100",
    precision ? "1" : "0",
    "Precondition: no existing order or idempotency receipt for this request. The submitted amount is invalid.",
  ).slice(0, 1);
  next(
    frames,
    precision ? "Reject excess precision" : "Reject a zero quantity",
    precision
      ? "ETH-USD price has two decimal places; 100.001 cannot be represented without rounding."
      : "The schema accepts a string, but the market amount rule rejects zero quantity.",
    ["client", "api"],
    precision
      ? "Reject excess precision; do not round or silently change the client’s price."
      : "Quantity must be strictly positive.",
    api + "canonicalize-order.ts → parseAmount",
    { phase: "api-validate", transfers: [["client", "api"]] },
  );
  next(
    frames,
    "Reject before acceptance",
    "Validation fails before acceptOrder runs. No idempotency receipt, order, sequence advance, or Outbox event is committed.",
    ["api", "client"],
    "Return 400 with a field-specific error. Do not claim acceptance.",
    api + "orders.controller.ts → canonicalizeOrder",
    {
      phase: "api-reply",
      transfers: [["api", "client"]],
      facts: [
        ["Orders / events", "0 / 0"],
        ["Response", "400"],
      ],
      response: precision
        ? '400 Bad Request\n{ "code": "INVALID_PRICE" }'
        : '400 Bad Request\n{ "code": "INVALID_QUANTITY" }',
    },
  );
  return frames;
}

const concurrent = accept(
  "order-A",
  "SELL",
  "200",
  "1",
  "25 simultaneous HTTP requests carry the same payload and key. R1 wins the key; the other requests wait for that transaction.",
);
concurrent[0] = {
  ...concurrent[0]!,
  requestLabel: "R1–R25 · same order intention",
  facts: [
    ["Concurrent requests", "25"],
    ["Accepted orders", "0"],
  ],
};
concurrent.splice(3, 0, {
  ...concurrent[2]!,
  title: "Other requests wait on the key",
  detail:
    "Only one transaction can claim this key. The remaining requests wait for its result. This illustrated run completes within the lock timeout; a longer wait would return 409 with Retry-After.",
  decision:
    "Serialize one intention under one key; do not create a second order.",
  facts: [
    ["Winner", "R1"],
    ["Waiting requests", "24"],
  ],
});
for (let index = 1; index < concurrent.length; index++)
  concurrent[index] = {
    ...concurrent[index]!,
    requestLabel: "R1–R25 · same order intention",
  };
next(
  concurrent,
  "Waiting requests replay the committed receipt",
  "After R1 commits, the waiting requests find its fingerprint and original receipt. They return the same order ID without another acceptance.",
  ["api", "database"],
  "A matching stored key turns each waiting request into a receipt replay.",
  api + "accept-order.ts → replayReceipt",
  { phase: "api-key", transfers: [["api", "database"]] },
);
next(
  concurrent,
  "25 responses identify one order",
  "Each request receives the same saved 202 receipt. Exactly one order and acceptance event were created in this illustrated run.",
  ["api", "client"],
  "Concurrency does not multiply the order intention.",
  api + "orders.controller.ts",
  {
    phase: "api-reply",
    transfers: [["api", "client"]],
    facts: [
      ["HTTP responses", "25"],
      ["Orders / events", "1 / 1"],
    ],
    response:
      "25 × 202 Accepted\nSame orderId: order-A\nOrders: 1\nAcceptance events: 1",
  },
);

function delivery(mode: "temporary" | "permanent" | "exhausted"): Frame[] {
  const frames = accept(
    "S1",
    "SELL",
    "200",
    "1",
    "Precondition: the mock destination will return a controlled failure. Follow the new order from acceptance to a durable notification job.",
  );
  publish(frames);
  next(
    frames,
    "Store the notification job",
    "The external consumer atomically saves its Inbox entry and job before it may advance its own offset. Matching runs independently.",
    ["kafka", "worker", "database"],
    "Persist owed work before acknowledging its event.",
    external + "schedule-notification.ts",
    { phase: "notify-inbox", transfers: [["kafka", "worker"]] },
  );
  next(
    frames,
    "Attempt notification delivery",
    "Send the original payload under external-notify:event-A. This attempt creates no destination effect.",
    ["worker", "external"],
    "The stable key belongs to this job, not this individual HTTP attempt.",
    external + "external-client.ts",
    {
      phase: "notify-send",
      requestLabel: "N1 · notification for S1",
      request:
        "POST /v1/notifications\nIdempotency-Key: external-notify:event-A",
      transfers: [["worker", "external"]],
      facts: [
        ["Attempts", "1"],
        ["Destination effects", "0"],
      ],
    },
  );
  next(
    frames,
    mode === "permanent"
      ? "Destination rejects the notification"
      : "Destination is temporarily unavailable",
    mode === "permanent"
      ? "The destination returns 400. The client classifies this as permanent."
      : "The destination returns 503 with Retry-After: 2. No effect was applied.",
    ["external", "worker"],
    mode === "permanent"
      ? "Permanent rejection → do not automatically retry."
      : "Transient error → check the remaining retry budget.",
    external + "external-client.ts",
    {
      phase: "notify-response",
      transfers: [["external", "worker"]],
      response:
        mode === "permanent"
          ? "400 Bad Request"
          : "503 Service Unavailable\nRetry-After: 2",
    },
  );
  if (mode === "exhausted") {
    next(
      frames,
      "Persist the first retry due time",
      "This compact illustration configures a budget of 2 attempts, rather than the application default. After the first 503, keep the job PENDING with a stored next due time.",
      ["worker", "database"],
      "Attempt 1 is below the illustrated budget; schedule its retry.",
      external + "notification-worker.ts → recordOutcome",
      {
        phase: "notify-retry",
        facts: [
          ["Illustrated retry budget", "2"],
          ["Attempts used", "1"],
        ],
      },
    );
    next(
      frames,
      "Send the final budgeted attempt",
      "When due, the worker sends the original payload and delivery key again. No new client request or Kafka event is involved.",
      ["worker", "external"],
      "Reuse the stable key for attempt 2.",
      external + "notification-worker.ts",
      {
        phase: "notify-send",
        transfers: [["worker", "external"]],
        facts: [
          ["Attempts used", "2"],
          ["Destination effects", "0"],
        ],
      },
    );
    next(
      frames,
      "The final attempt also receives 503",
      "The destination is still unavailable and applies no effect. The worker must now evaluate the exhausted budget.",
      ["external", "worker"],
      "Transient status alone does not permit an unlimited retry loop.",
      external + "external-client.ts",
      {
        phase: "notify-response",
        transfers: [["external", "worker"]],
        response: "503 Service Unavailable",
      },
    );
    next(
      frames,
      "Exhausted retries become FAILED",
      "The second attempt reaches this run’s configured budget. Persist FAILED with no next due time. The order and matching result are unchanged.",
      ["worker", "database"],
      "Budget exhausted → stop automatic retries; retain the job for operator inspection.",
      external + "external-job.repository.ts → markJobFailed",
      {
        phase: "notify-complete",
        facts: [
          ["Job status", "FAILED"],
          ["Destination effects", "0"],
        ],
        response: "FAILED\nAttempts: 2 / 2\nNo automatic next attempt",
      },
    );
    return frames;
  }
  if (mode === "permanent") {
    next(
      frames,
      "Persist the permanent failure",
      "Record FAILED immediately, preserve the error, and clear the next due time. The accepted order is not cancelled or changed.",
      ["worker", "database"],
      "A permanent 400 ends automatic retries; keep a record for the operator.",
      external + "external-job.repository.ts → markJobFailed",
      {
        phase: "notify-complete",
        facts: [
          ["Job status", "FAILED"],
          ["Automatic retry", "No"],
        ],
        response: "FAILED\nAttempts: 1\nOrder unchanged",
      },
    );
    next(
      frames,
      "Operator redrives the failed job",
      "After the destination is corrected, the operator explicitly redrives this FAILED event. Reset the attempt budget to zero and make it due now, preserving the event, payload, and destination key.",
      ["worker", "database"],
      "Recovery is explicit; do not create a new job identity or key.",
      "src/entrypoints/redrive-external.ts",
      {
        phase: "notify-retry",
        requestLabel: "Operator · redrive event-A",
        request: "npm run external:redrive -- event-A",
        facts: [
          ["Job status", "PENDING"],
          ["Attempts after redrive", "0"],
        ],
        response: "Existing FAILED job → PENDING\nSame event and delivery key",
      },
    );
  } else {
    next(
      frames,
      "Schedule a durable retry",
      "Leave the job PENDING and store the next attempt time. Retry-After is a minimum within the configured cap; jitter does not undercut that minimum.",
      ["worker", "database"],
      "503 is retryable. Wait until the persisted due time; do not busy-loop.",
      external + "notification-worker.ts → computeRetryDelayMs",
      {
        phase: "notify-retry",
        facts: [
          ["Job status", "PENDING"],
          ["Retry-After", "2 seconds"],
        ],
      },
    );
  }
  next(
    frames,
    "Retry the same delivery job",
    "When due, send the same payload and delivery key. The corrected destination applies one effect.",
    ["worker", "external"],
    "A retry resumes at the worker, independently of Kafka or matching.",
    external + "notification-worker.ts",
    {
      phase: "notify-send",
      requestLabel: "N1 · same notification job",
      request:
        "POST /v1/notifications\nIdempotency-Key: external-notify:event-A",
      transfers: [["worker", "external"]],
    },
  );
  next(
    frames,
    "Destination confirms success",
    "The destination returns success. The worker can now record a terminal result.",
    ["external", "worker"],
    "Confirmed success → persist SUCCEEDED.",
    external + "external-client.ts",
    {
      phase: "notify-response",
      transfers: [["external", "worker"]],
      response: "200 OK",
    },
  );
  next(
    frames,
    "Record SUCCEEDED without changing the order",
    "Persist the successful attempt. The destination holds one effect; the order and its matching state were never changed by delivery failures.",
    ["worker", "database"],
    "Notification recovery must not re-submit or mutate the order.",
    external + "external-job.repository.ts → markJobSucceeded",
    {
      phase: "notify-complete",
      facts: [
        ["Job status", "SUCCEEDED"],
        ["Destination effects", "1"],
      ],
      response: "SUCCEEDED\nOne destination effect\nSame notification key",
    },
  );
  return frames;
}
const validationCoverage = coverage("conflict", {
  area: "Input validation",
  kind: "Explicit challenge behavior",
  requirement:
    "Receive and validate orders before acceptance; reject unsupported precision and non-positive amounts.",
  signals: [
    "Boundary-case reasoning",
    "Exact-number handling",
    "No partial acceptance",
  ],
  choice:
    "Validate exact decimal strings against market rules before touching acceptance state.",
  evidence: [
    "tests/unit/decimal.test.ts",
    "tests/integration/acceptance.test.ts",
  ],
});
export const walkthroughVariants: Record<string, WalkthroughVariant[]> = {
  matching: [
    {
      id: "sell-taker",
      guarantee:
        "A SELL consumes the highest bid first, then the oldest maker at a shared price, always at the maker’s price.",
      label: "SELL taker: highest bid, then FIFO",
      description: "Mirror the BUY fixture with a SELL taker.",
      outcome:
        "S1 fills 4 ETH at maker prices 101, 100, 100. B2 retains 2 ETH at 100.",
      frames: sell,
      coverage: coverage("matching", {}),
    },
    {
      id: "partial-taker",
      guarantee:
        "A partially filled taker rests only its unfilled quantity at its own limit.",
      label: "Partially filled taker rests at its limit",
      description:
        "Fill available liquidity, stop at the price boundary, and rest the taker remainder.",
      outcome:
        "B1 executes 1 ETH at 99 and rests its remaining 2 ETH at 100. S2 stays at 105.",
      frames: partial,
      coverage: coverage("matching", {}),
    },
    {
      id: "crash-replay",
      guarantee:
        "A committed event can be redelivered without creating another trade or changing the restored quantities.",
      label: "Crash after matching commit → safe redelivery",
      description:
        "Follow durable matching through a process crash, book restore, and duplicate event.",
      outcome:
        "The original three trades survive. Replay adds zero trades; S2 remains at 2 ETH.",
      frames: crash,
      coverage: coverage("matching", {
        area: "Crash recovery & event deduplication",
        kind: "Reliability demonstration",
        requirement:
          "Handle event duplicates and offset boundaries reliably, including a crash after matching persistence.",
        signals: [
          "Recovery reasoning",
          "Atomicity and offset ordering",
          "Duplicate-trade prevention",
        ],
        choice:
          "Commit Inbox and matching results together; restore the book and skip the handled event on redelivery.",
        evidence: [
          "tests/e2e/crash-recovery.test.ts",
          "tests/integration/matching-recovery.test.ts",
        ],
      }),
    },
  ],
  duplicate: [
    {
      id: "concurrent-retries",
      guarantee:
        "Concurrent requests under one key represent at most one accepted order intention.",
      label: "25 concurrent requests under one key",
      description:
        "Show the winning acceptance transaction and the waiting receipt replays.",
      outcome:
        "25 responses identify one accepted order and one acceptance event.",
      frames: concurrent,
      coverage: coverage("duplicate", {
        signals: [
          "Concurrency reasoning",
          "Idempotency design",
          "Transaction isolation",
        ],
      }),
    },
  ],
  conflict: [
    {
      id: "invalid-precision",
      guarantee:
        "Unsupported precision is rejected before any acceptance state is written; the price is never rounded.",
      label: "Invalid price precision → 400, no writes",
      description: "Reject a price that would require rounding.",
      outcome:
        "INVALID_PRICE is returned before any order, receipt, sequence change, or event is saved.",
      frames: invalid(true),
      coverage: validationCoverage,
    },
    {
      id: "invalid-quantity",
      guarantee:
        "A non-positive quantity is rejected before any acceptance state is written.",
      label: "Zero quantity → 400, no writes",
      description: "Reject a non-positive amount before acceptance.",
      outcome:
        "INVALID_QUANTITY is returned with zero accepted orders and events.",
      frames: invalid(false),
      coverage: validationCoverage,
    },
  ],
  "lost-response": [
    {
      id: "temporary-503",
      guarantee:
        "Transient delivery retries preserve the job identity and respect its durable due time.",
      label: "503 + Retry-After → durable retry → success",
      description:
        "Differentiate a temporary destination outage from permanent rejection.",
      outcome:
        "The retry respects its stored due time and succeeds under the original delivery key.",
      frames: delivery("temporary"),
      coverage: coverage("lost-response", {}),
    },
    {
      id: "permanent-redrive",
      guarantee:
        "Permanent rejection stops automatic retries; explicit redrive preserves the existing event, payload and delivery key.",
      label: "Permanent 400 → FAILED → operator redrive",
      description:
        "A permanent failure needs explicit recovery, not automatic retries.",
      outcome:
        "The failed job is explicitly redriven with a fresh attempt budget and the same key, then succeeds.",
      frames: delivery("permanent"),
      coverage: coverage("lost-response", {
        choice:
          "Classify permanent failures, retain FAILED jobs, and require an explicit redrive under the same identity.",
      }),
    },
    {
      id: "retry-exhaustion",
      guarantee:
        "A finite configured budget bounds automatic retries; exhausted jobs remain FAILED for inspection.",
      label: "Transient errors exhaust the retry budget",
      description:
        "A retryable error becomes terminal once the configured budget is used.",
      outcome:
        "At the illustrated 2-attempt budget, the job becomes FAILED. The order remains unchanged.",
      frames: delivery("exhausted"),
      coverage: coverage("lost-response", {
        choice:
          "A bounded retry budget prevents endless automatic retries and preserves a failed job for inspection.",
      }),
    },
  ],
};
