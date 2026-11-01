export interface Coverage {
  area: string;
  kind:
    | "Explicit challenge behavior"
    | "Implementation extension"
    | "Reliability demonstration";
  requirement: string;
  signals: string[];
  choice: string;
  evidence: string[];
}

/** Requirements follow docs/architecture.md's assignment mapping; signals are suggestions, not the employer's rubric. */
export const scenarioCoverage: Record<string, Coverage> = {
  matching: {
    area: "Matching rules",
    kind: "Explicit challenge behavior",
    requirement:
      "Price priority, FIFO, partial execution, and an explanation of data structures and complexity.",
    signals: [
      "Algorithm correctness",
      "Data structure reasoning",
      "State consistency",
    ],
    choice:
      "A price-level tree and FIFO queues select makers. Compute first, persist the complete match, then apply the committed book changes.",
    evidence: [
      "tests/unit/matching.test.ts",
      "tests/integration/matching-recovery.test.ts",
    ],
  },
  "non-crossing": {
    area: "Price boundaries",
    kind: "Explicit challenge behavior",
    requirement:
      "Correct limit-order matching: an accepted order may rest when the opposite price does not cross its limit.",
    signals: [
      "Boundary-case reasoning",
      "Order lifecycle clarity",
      "Algorithm correctness",
    ],
    choice:
      "Keep acceptance separate from execution. Stop at the first non-crossing price and rest the remaining order at its own limit.",
    evidence: ["tests/unit/matching.test.ts"],
  },
  duplicate: {
    area: "Request retry safety",
    kind: "Implementation extension",
    requirement:
      "Supports reliable order acceptance. HTTP idempotency is a chosen extension; consumer event deduplication is a separate requirement.",
    signals: [
      "API contract reasoning",
      "Idempotency design",
      "Consistency under retries",
    ],
    choice:
      "Normalize the request before fingerprinting. An identical retry returns the original receipt and skips creating another order or acceptance event.",
    evidence: ["tests/integration/acceptance.test.ts"],
  },
  conflict: {
    area: "Request conflicts",
    kind: "Implementation extension",
    requirement:
      "Supports validation and reliable acceptance. The HTTP key-conflict policy and 422 response are implementation decisions.",
    signals: [
      "API contract reasoning",
      "Explicit failure handling",
      "Data integrity",
    ],
    choice:
      "Reject a different request under an existing key. Preserve the original order and receipt rather than silently changing the accepted intention.",
    evidence: ["tests/integration/acceptance.test.ts"],
  },
  "lost-response": {
    area: "Delivery & retries",
    kind: "Reliability demonstration",
    requirement:
      "Consume the event, call a mock API, and handle failures, retries, duplicates and offsets.",
    signals: [
      "Failure recovery reasoning",
      "Distributed-systems boundaries",
      "Duplicate-effect prevention",
    ],
    choice:
      "Persist the delivery job before advancing its consumer offset. Reuse one destination key when a lost response leaves the remote outcome uncertain.",
    evidence: [
      "tests/integration/external-retry.test.ts",
      "tests/integration/external-jobs.test.ts",
    ],
  },
  "kafka-outage": {
    area: "Publishing & recovery",
    kind: "Reliability demonstration",
    requirement:
      "Publish accepted orders to Kafka and explain reliable processing and ordering. The Outbox is the chosen mechanism.",
    signals: [
      "Durability reasoning",
      "Failure isolation",
      "Ordering guarantees",
    ],
    choice:
      "Commit acceptance without waiting for Kafka. Keep failed publication pending, preserve per-symbol order, and mark sent only after broker acknowledgement.",
    evidence: [
      "tests/integration/outbox.test.ts",
      "tests/e2e/crash-recovery.test.ts",
    ],
  },
};
