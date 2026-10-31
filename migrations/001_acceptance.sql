-- Durable order acceptance: an order, its receipt, its Outbox event and the
-- advanced symbol counter commit together or not at all.

-- The row lock taken when a counter advances is held until commit, so within a
-- symbol the sequence order is also the commit order. A database sequence could
-- not guarantee this.
CREATE TABLE symbol_sequences (
  symbol   text   PRIMARY KEY,
  last_seq bigint NOT NULL CHECK (last_seq > 0)
);

CREATE TABLE orders (
  id              uuid           PRIMARY KEY,
  scope           text           NOT NULL,
  symbol          text           NOT NULL,
  side            text           NOT NULL CHECK (side IN ('BUY', 'SELL')),
  order_type      text           NOT NULL CHECK (order_type = 'LIMIT'),
  price_units     numeric(38, 0) NOT NULL CHECK (price_units > 0),
  original_units  numeric(38, 0) NOT NULL CHECK (original_units > 0),
  remaining_units numeric(38, 0) NOT NULL CHECK (remaining_units BETWEEN 0 AND original_units),
  status          text           NOT NULL
    CHECK (status IN ('QUEUED', 'OPEN', 'PARTIALLY_FILLED', 'FILLED', 'REJECTED')),
  acceptance_seq  bigint         NOT NULL CHECK (acceptance_seq > 0),
  created_at      timestamptz    NOT NULL,
  updated_at      timestamptz    NOT NULL,
  UNIQUE (symbol, acceptance_seq)
);

-- Rows are never deleted automatically: a key maps to its order for good.
CREATE TABLE idempotency_requests (
  scope             text        NOT NULL,
  operation         text        NOT NULL,
  idempotency_key   text        NOT NULL,
  fingerprint       text        NOT NULL CHECK (fingerprint ~ '^[0-9a-f]{64}$'),
  -- Deferred because this row is inserted first: it claims the key before any other write.
  order_id          uuid        NOT NULL UNIQUE REFERENCES orders (id) DEFERRABLE INITIALLY DEFERRED,
  response_status   smallint    NOT NULL,
  -- `json` rather than `jsonb`, which would reorder keys: a replay must match the original.
  response_body     json        NOT NULL,
  response_location text        NOT NULL,
  created_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (scope, operation, idempotency_key)
);

-- `published_at`, `attempts`, `next_attempt_at` and `last_error` belong to the
-- publisher. `payload` is immutable: a resend must carry the same event.
CREATE TABLE outbox_events (
  event_id        uuid        PRIMARY KEY,
  event_type      text        NOT NULL,
  schema_version  integer     NOT NULL,
  order_id        uuid        NOT NULL REFERENCES orders (id),
  symbol          text        NOT NULL,
  acceptance_seq  bigint      NOT NULL CHECK (acceptance_seq > 0),
  payload         json        NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  published_at    timestamptz,
  attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  last_error      text,
  UNIQUE (event_type, order_id)
);
