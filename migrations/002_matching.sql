-- One row per event a consumer has fully handled. It commits with the consumer's
-- local result, so a redelivered event can be recognised and ignored.
CREATE TABLE consumer_inbox (
  consumer_name text        NOT NULL,
  event_id      uuid        NOT NULL,
  processed_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_name, event_id)
);

-- The last acceptance sequence applied to each book, the guard against gaps and stale events.
CREATE TABLE matching_progress (
  symbol           text        PRIMARY KEY,
  last_applied_seq bigint      NOT NULL DEFAULT 0 CHECK (last_applied_seq >= 0),
  updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE trades (
  id              uuid           PRIMARY KEY DEFAULT gen_random_uuid(),
  event_id        uuid           NOT NULL,
  execution_index integer        NOT NULL CHECK (execution_index >= 0),
  maker_id        uuid           NOT NULL REFERENCES orders (id),
  taker_id        uuid           NOT NULL REFERENCES orders (id),
  symbol          text           NOT NULL,
  price_units     numeric(38, 0) NOT NULL CHECK (price_units > 0),
  quantity_units  numeric(38, 0) NOT NULL CHECK (quantity_units > 0),
  executed_at     timestamptz    NOT NULL DEFAULT now(),
  UNIQUE (event_id, execution_index),
  CHECK (maker_id <> taker_id)
);

CREATE INDEX trades_maker_idx ON trades (maker_id);
CREATE INDEX trades_taker_idx ON trades (taker_id);

-- Finds each symbol's earliest unpublished event without scanning the published ones.
CREATE INDEX outbox_events_unpublished_idx
  ON outbox_events (symbol, acceptance_seq)
  WHERE published_at IS NULL;

-- Restoring a book reads only the orders that rest in it.
CREATE INDEX orders_resting_idx
  ON orders (symbol, acceptance_seq)
  WHERE status IN ('OPEN', 'PARTIALLY_FILLED');

ALTER TABLE orders
  ADD CONSTRAINT orders_filled_iff_nothing_remains
  CHECK ((status = 'FILLED') = (remaining_units = 0));
