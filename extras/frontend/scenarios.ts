export type NodeId =
  | "client"
  | "api"
  | "database"
  | "outbox"
  | "kafka"
  | "matching"
  | "worker"
  | "external";

export interface BookOrder {
  id: string;
  side: "BUY" | "SELL";
  price: string;
  quantity: string;
}

export interface Trade {
  maker: string;
  price: string;
  quantity: string;
}

export interface Frame {
  title: string;
  detail: string;
  active: NodeId[];
  blocked: NodeId[];
  book: BookOrder[];
  trades: Trade[];
  facts: [string, string][];
  request: string;
  response: string;
}

export interface Scenario {
  id: string;
  title: string;
  subtitle: string;
  category: "Matching" | "Reliability";
  description: string;
  guarantee: string;
  outcome: string;
  tags: string[];
  frames: Frame[];
}

function step(
  title: string,
  detail: string,
  active: NodeId[],
  state: Partial<Frame> = {},
): Frame {
  return {
    title,
    detail,
    active,
    blocked: [],
    book: [],
    trades: [],
    facts: [],
    request: "No request shown for this step.",
    response: "Run the illustration to follow the expected behavior.",
    ...state,
  };
}

const s1: BookOrder = {
  id: "S1",
  side: "SELL",
  price: "100.00",
  quantity: "2.0000",
};
const s2: BookOrder = {
  id: "S2",
  side: "SELL",
  price: "100.00",
  quantity: "3.0000",
};
const s3: BookOrder = {
  id: "S3",
  side: "SELL",
  price: "99.00",
  quantity: "1.0000",
};
const remainder: BookOrder = { ...s2, quantity: "2.0000" };
const fixture = [s3, s1, s2];
const trades: Trade[] = [
  { maker: "S3", price: "99.00", quantity: "1.0000" },
  { maker: "S1", price: "100.00", quantity: "2.0000" },
  { maker: "S2", price: "100.00", quantity: "1.0000" },
];
const sellRequest = JSON.stringify(
  {
    symbol: "ETH-USD",
    side: "SELL",
    type: "LIMIT",
    price: "200.00",
    quantity: "1",
  },
  null,
  2,
);
const receipt = '202 Accepted\n{ "orderId": "order-A", "status": "QUEUED" }';
const notifyFacts: [string, string][] = [
  ["Delivery attempts", "1"],
  ["Destination effects", "1"],
];

/** Authored illustrations of expected behavior. These are not captured backend runs. */
export const scenarios: [Scenario, ...Scenario[]] = [
  {
    id: "matching",
    title: "Price, time & partial fills",
    subtitle: "The matching engine, step by step",
    category: "Matching",
    description:
      "Watch a buy order sweep the best ask, respect FIFO at the next price, and leave a maker partially filled.",
    guarantee:
      "Best price first. Oldest at that price next. Trades execute at the maker’s price.",
    outcome:
      "B1 fills 4 ETH in three trades. S2 keeps its queue position with 2 ETH remaining.",
    tags: ["Price priority", "FIFO", "Partial execution"],
    frames: [
      step(
        "Start with an empty book",
        "Three sell orders will arrive before a buy order crosses them. All amounts below use ETH-USD.",
        [],
        {
          facts: [
            ["Resting orders", "0"],
            ["Trades", "0"],
          ],
        },
      ),
      step(
        "S1 is first at 100",
        "After acceptance and matching, sell order S1 rests with a quantity of 2 ETH.",
        ["matching"],
        {
          book: [s1],
          facts: [
            ["Resting orders", "1"],
            ["Trades", "0"],
          ],
        },
      ),
      step(
        "S2 queues behind S1",
        "S2 offers 3 ETH at the same price. Arrival order puts it behind S1.",
        ["matching"],
        {
          book: [s1, s2],
          facts: [
            ["At price 100", "S1 → S2"],
            ["Trades", "0"],
          ],
        },
      ),
      step(
        "S3 offers a better price",
        "S3 arrives later, but its price of 99 has priority over both orders at 100.",
        ["matching"],
        {
          book: fixture,
          facts: [
            ["Best ask", "99.00"],
            ["Priority", "S3 → S1 → S2"],
          ],
        },
      ),
      step(
        "B1 is accepted durably",
        "The API commits B1 and its Outbox event together. A 202 response confirms acceptance; the order has not necessarily matched yet.",
        ["api", "database", "outbox"],
        {
          book: fixture,
          request:
            'POST /v1/orders\n\n{ "symbol": "ETH-USD", "side": "BUY",\n  "type": "LIMIT", "price": "100", "quantity": "4" }',
          response: '202 Accepted\n{ "orderId": "B1", "status": "QUEUED" }',
          facts: [
            ["B1 quantity", "4.0000"],
            ["Limit price", "100.00"],
          ],
        },
      ),
      step(
        "Kafka feeds two independent branches",
        "Matching and notification consume the acceptance event independently. Either branch may finish first.",
        ["kafka", "matching", "worker"],
        {
          book: fixture,
          facts: [
            ["Matching", "Computing"],
            ["Notification", "Independent"],
          ],
        },
      ),
      step(
        "Inside the match: best price first",
        "The computation takes S3’s 1 ETH at 99. This is an explanatory computation step; the trades and order changes will commit together.",
        ["matching"],
        {
          book: [s1, s2],
          trades: trades.slice(0, 1),
          facts: [
            ["B1 left to match", "3.0000"],
            ["Maker price", "99.00"],
          ],
        },
      ),
      step(
        "Inside the match: oldest at 100",
        "S1 arrived before S2, so its full 2 ETH executes next at 100.",
        ["matching"],
        {
          book: [s2],
          trades: trades.slice(0, 2),
          facts: [
            ["B1 left to match", "1.0000"],
            ["FIFO winner", "S1"],
          ],
        },
      ),
      step(
        "Commit the match and keep the remainder",
        "B1 takes 1 ETH from S2. All three trades, order updates, and matching progress commit atomically. S2 remains first at 100.",
        ["database", "matching"],
        {
          book: [remainder],
          trades,
          facts: [
            ["B1 status", "FILLED"],
            ["S2 remaining", "2.0000"],
          ],
          response:
            "Expected persisted result\nB1: FILLED, remaining 0\nS2: PARTIALLY_FILLED, remaining 2\nTrades: 1 @ 99, 2 @ 100, 1 @ 100",
        },
      ),
    ],
  },
  {
    id: "non-crossing",
    title: "Prices that never cross",
    subtitle: "A valid order does not need a trade",
    category: "Matching",
    description:
      "A buyer offers 100 while a seller asks 101. Both orders are valid, but neither may trade outside its limit.",
    guarantee:
      "A buy trades only at or below its limit. A sell trades only at or above its limit.",
    outcome:
      "Both orders stay OPEN. The spread is 1.00 and no trade is created.",
    tags: ["Limit boundary", "No execution"],
    frames: [
      step(
        "Two limits, one gap",
        "A seller asks 101 and a buyer will pay at most 100.",
        [],
        {
          facts: [
            ["Sell limit", "101.00"],
            ["Buy limit", "100.00"],
          ],
        },
      ),
      step(
        "The seller rests at 101",
        "S1 enters the book without an opposing buyer.",
        ["matching"],
        { book: [{ ...s1, price: "101.00" }], facts: [["S1 status", "OPEN"]] },
      ),
      step(
        "The buyer is accepted",
        "A valid buy order is accepted even when it cannot execute immediately.",
        ["api", "database", "outbox"],
        {
          book: [{ ...s1, price: "101.00" }],
          response: receipt,
          facts: [["B1 limit", "100.00"]],
        },
      ),
      step(
        "The matcher checks the boundary",
        "The best ask, 101, exceeds the buyer’s limit of 100. The matcher cannot cross.",
        ["matching"],
        {
          book: [
            { ...s1, price: "101.00" },
            { id: "B1", side: "BUY", price: "100.00", quantity: "1.0000" },
          ],
          facts: [
            ["Comparison", "101 > 100"],
            ["Trades", "0"],
          ],
        },
      ),
      step(
        "Both orders stay open",
        "The buy rests at 100 and the sell stays at 101. Acceptance succeeded without a trade.",
        ["database"],
        {
          book: [
            { ...s1, price: "101.00" },
            { id: "B1", side: "BUY", price: "100.00", quantity: "1.0000" },
          ],
          facts: [
            ["Spread", "1.00"],
            ["Trades", "0"],
          ],
          response: "Expected persisted result\nS1: OPEN\nB1: OPEN\nTrades: []",
        },
      ),
    ],
  },
  {
    id: "duplicate",
    title: "One order, two requests",
    subtitle: "Safe retries with an idempotency key",
    category: "Reliability",
    description:
      "Send the same order twice under the same key. The second request should return the original acceptance receipt.",
    guarantee:
      "The same key and the same request meaning identify the same order.",
    outcome: "Two HTTP requests resolve to one order and one acceptance event.",
    tags: ["Idempotency", "Safe retry"],
    frames: [
      step(
        "One key identifies one intention",
        "Both requests will carry the key demo-duplicate-0001 and an equivalent order.",
        [],
        {
          request: "Idempotency-Key: demo-duplicate-0001\n\n" + sellRequest,
          facts: [
            ["HTTP requests", "0"],
            ["Orders", "0"],
          ],
        },
      ),
      step(
        "Commit the first acceptance",
        "The order, fingerprint, receipt, sequence, and Outbox event are saved in one transaction.",
        ["api", "database", "outbox"],
        {
          request: sellRequest,
          response: receipt,
          facts: [
            ["HTTP requests", "1"],
            ["Orders", "1"],
          ],
        },
      ),
      step(
        "The same request arrives again",
        "A retry carries the original key. Decimal spelling can differ while the order’s meaning stays the same.",
        ["client", "api"],
        {
          request:
            'Idempotency-Key: demo-duplicate-0001\n\n{ "price": "200", "quantity": "1.0000",\n  "symbol": "ETH-USD", "side": "SELL", "type": "LIMIT" }',
          facts: [
            ["HTTP requests", "2"],
            ["Orders", "1"],
          ],
        },
      ),
      step(
        "Find the existing receipt",
        "The stored fingerprint matches. The API returns the original receipt instead of accepting another order.",
        ["api", "database"],
        {
          response: receipt,
          facts: [
            ["Returned order", "order-A"],
            ["Orders", "1"],
          ],
        },
      ),
      step(
        "Two requests. One acceptance.",
        "The retry creates neither a second order nor another acceptance event. It returns the original QUEUED receipt even if the order has since progressed.",
        ["database"],
        {
          response: receipt,
          facts: [
            ["HTTP requests", "2"],
            ["Orders / events", "1 / 1"],
          ],
        },
      ),
    ],
  },
  {
    id: "conflict",
    title: "Same key, different order",
    subtitle: "Protect the original intention",
    category: "Reliability",
    description:
      "Reuse an accepted request’s key for a different quantity. The API must reject the change without modifying the original order.",
    guarantee:
      "An idempotency key cannot silently change the meaning of an accepted request.",
    outcome:
      "The changed request receives 422. The original order and receipt remain unchanged.",
    tags: ["Conflict detection", "422 response"],
    frames: [
      step(
        "Accept an order for 1 ETH",
        "The first request establishes the meaning of demo-conflict-0001.",
        [],
        {
          request: "Idempotency-Key: demo-conflict-0001\n\n" + sellRequest,
          facts: [["Original quantity", "1.0000"]],
        },
      ),
      step(
        "Store the original receipt",
        "The order and normalized request fingerprint commit together.",
        ["api", "database"],
        {
          response: receipt,
          facts: [
            ["Orders", "1"],
            ["Original quantity", "1.0000"],
          ],
        },
      ),
      step(
        "Change the quantity to 2",
        "The next request reuses the key but asks to sell twice as much.",
        ["client", "api"],
        {
          request:
            'Idempotency-Key: demo-conflict-0001\n\n{ "symbol": "ETH-USD", "side": "SELL",\n  "type": "LIMIT", "price": "200", "quantity": "2" }',
          facts: [
            ["Stored quantity", "1.0000"],
            ["Requested quantity", "2.0000"],
          ],
        },
      ),
      step(
        "Detect the meaning mismatch",
        "The key exists, but the request fingerprint differs. The change is rejected.",
        ["api", "database"],
        {
          blocked: ["api"],
          response:
            '422 Unprocessable Entity\n{ "code": "IDEMPOTENCY_KEY_REUSED" }',
          facts: [
            ["Response", "422"],
            ["Orders", "1"],
          ],
        },
      ),
      step(
        "The original order is intact",
        "No new order or acceptance event is created. The saved receipt still belongs to the original request.",
        ["database"],
        {
          response:
            "Expected result\nOriginal quantity: 1 ETH\nOriginal receipt: unchanged\nNew orders: 0",
          facts: [
            ["Original quantity", "1.0000"],
            ["New events", "0"],
          ],
        },
      ),
    ],
  },
  {
    id: "lost-response",
    title: "Delivered. Response lost.",
    subtitle: "Retry the request, preserve one effect",
    category: "Reliability",
    description:
      "The destination applies a notification, but its response disappears. Follow the retry that safely resolves the uncertain outcome.",
    guarantee:
      "Every attempt uses the same delivery key, and the destination deduplicates that key.",
    outcome:
      "Two delivery attempts produce one destination effect. The job eventually succeeds.",
    tags: ["Uncertain outcome", "Destination idempotency"],
    frames: [
      step(
        "Prepare a lost-response fault",
        "The illustrated destination will commit an effect and drop the connection before replying.",
        [],
        {
          facts: [
            ["Delivery attempts", "0"],
            ["Destination effects", "0"],
          ],
        },
      ),
      step(
        "Publish the accepted order",
        "The durable acceptance event reaches Kafka. Matching and notification work proceed independently.",
        ["kafka", "matching", "worker"],
        {
          response: receipt,
          facts: [
            ["Order", "Accepted"],
            ["Notification", "Pending"],
          ],
        },
      ),
      step(
        "Send the notification",
        "The worker uses external-notify:event-A. The destination saves its effect under that key.",
        ["worker", "external"],
        {
          request:
            "POST /v1/notifications\nIdempotency-Key: external-notify:event-A",
          facts: notifyFacts,
        },
      ),
      step(
        "The response never arrives",
        "The destination has applied the effect, but the worker cannot confirm success. It schedules a durable retry.",
        ["worker"],
        {
          blocked: ["external"],
          response:
            "Connection closed before a response\nLocal delivery outcome: uncertain",
          facts: [
            ["Delivery attempts", "1"],
            ["Destination effects", "1"],
          ],
        },
      ),
      step(
        "Retry with the original key",
        "The worker sends the same payload and key. The destination finds its original result and replays it.",
        ["worker", "external"],
        {
          request:
            "POST /v1/notifications\nIdempotency-Key: external-notify:event-A\n(same payload)",
          response: "200 OK\nidempotent-replayed: true",
          facts: [
            ["Delivery attempts", "2"],
            ["Destination effects", "1"],
          ],
        },
      ),
      step(
        "One effect, confirmed",
        "The job is marked SUCCEEDED. Destination idempotency closes the gap between a remote success and the local delivery record.",
        ["database", "external"],
        {
          response:
            "Expected persisted result\nJob: SUCCEEDED\nAttempts: 2\nDestination effects: 1",
          facts: [
            ["Delivery attempts", "2"],
            ["Destination effects", "1"],
          ],
        },
      ),
    ],
  },
  {
    id: "kafka-outage",
    title: "Kafka pauses. Orders don’t.",
    subtitle: "Durable acceptance through an outage",
    category: "Reliability",
    description:
      "Accept an order while Kafka is unavailable, then follow the stored work as publication resumes after recovery.",
    guarantee:
      "A confirmed order survives a broker outage. Publication preserves each symbol’s acceptance order.",
    outcome:
      "The pending acceptance event publishes after recovery, and the order eventually becomes OPEN.",
    tags: ["Outbox", "Recovery", "Ordering"],
    frames: [
      step(
        "The broker is unavailable",
        "Kafka is deliberately offline in this illustration. PostgreSQL and the API remain available.",
        [],
        {
          blocked: ["kafka"],
          facts: [
            ["Kafka", "Unavailable"],
            ["API", "Available"],
          ],
        },
      ),
      step(
        "Accept and save the order",
        "The API commits the order and its Outbox event without waiting for Kafka.",
        ["api", "database", "outbox"],
        {
          blocked: ["kafka"],
          request: sellRequest,
          response: receipt,
          facts: [
            ["Order status", "QUEUED"],
            ["Pending events", "1"],
          ],
        },
      ),
      step(
        "Keep work in the Outbox",
        "Publication fails and will retry. The event stays durable, and later events for this symbol wait behind it.",
        ["outbox"],
        {
          blocked: ["kafka"],
          facts: [
            ["Order status", "QUEUED"],
            ["Pending events", "1"],
          ],
          response:
            "Expected during outage\nOutbox event: pending\nOrder: durable\nMatching: waiting",
        },
      ),
      step(
        "Kafka becomes available",
        "The publisher retries the earliest pending event for this symbol and receives the broker’s acknowledgement.",
        ["outbox", "kafka"],
        {
          facts: [
            ["Kafka", "Available"],
            ["Publication", "Acknowledged"],
          ],
        },
      ),
      step(
        "Resume independent consumers",
        "Matching processes the accepted order; notification work can proceed in its own consumer group.",
        ["kafka", "matching", "worker"],
        {
          facts: [
            ["Matching", "Processing"],
            ["Notification", "Independent"],
          ],
        },
      ),
      step(
        "The accepted order reaches the book",
        "The non-crossing sell order becomes OPEN. No confirmed order was discarded during the outage.",
        ["database", "matching"],
        {
          book: [
            { id: "S1", side: "SELL", price: "200.00", quantity: "1.0000" },
          ],
          facts: [
            ["Order status", "OPEN"],
            ["Pending events", "0"],
          ],
          response:
            "Expected persisted result\nOrder: OPEN\nRemaining: 1 ETH\nOutbox event: published",
        },
      ),
    ],
  },
];

export const firstScenario = scenarios[0];
