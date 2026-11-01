-- Validation happens before an order is accepted, and nothing rejects one afterwards, so this
-- status could never be reached. Removing it makes that promise part of the schema.
ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders
  ADD CONSTRAINT orders_status_check
  CHECK (status IN ('QUEUED', 'OPEN', 'PARTIALLY_FILLED', 'FILLED'));
