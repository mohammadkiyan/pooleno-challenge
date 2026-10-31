import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';
import { ApiError } from '../../app/api-error.js';
import { setLockTimeout, withTransaction } from '../../infrastructure/transaction.js';
import type { OrderAcceptedV1 } from '../messaging/order-accepted.schema.js';
import { insertOutboxEvent } from '../messaging/outbox.repository.js';
import { claimKey, loadStoredRequest } from './idempotency.js';
import { fingerprintOrder } from './order-fingerprint.js';
import { insertOrder, nextAcceptanceSequence } from './order.repository.js';
import type { AcceptanceReceipt, CanonicalOrder } from './order.types.js';

export interface AcceptOrderCommand {
  scope: string;
  idempotencyKey: string;
  order: CanonicalOrder;
  correlationId: string;
}

export type AcceptOrderResult =
  | { replayed: false; receipt: AcceptanceReceipt; event: OrderAcceptedV1 }
  | { replayed: true; receipt: AcceptanceReceipt };

/**
 * Durably accepts an order, or replays the receipt of an earlier identical request.
 *
 * The receipt, order, sequence and Outbox event commit in one transaction, so the
 * caller may report acceptance only after this resolves. Locks are always taken in
 * the same order (key, then symbol counter), and nothing here calls Kafka or HTTP.
 */
export function acceptOrder(
  pool: Pool,
  lockTimeoutMs: number,
  command: AcceptOrderCommand,
): Promise<AcceptOrderResult> {
  const { scope, idempotencyKey, order, correlationId } = command;
  const fingerprint = fingerprintOrder(scope, order);
  const orderId = randomUUID();
  const receipt = buildReceipt(orderId);

  return withTransaction(pool, async (tx) => {
    await setLockTimeout(tx, lockTimeoutMs);

    const claimed = await claimKey(tx, { scope, idempotencyKey, fingerprint, orderId, receipt });
    if (!claimed) {
      const replay = await replayReceipt(tx, scope, idempotencyKey, fingerprint);
      return { replayed: true, receipt: replay };
    }

    const acceptanceSeq = await nextAcceptanceSequence(tx, order.symbol);
    const acceptedAt = new Date();

    await insertOrder(tx, { ...order, id: orderId, scope, acceptanceSeq, acceptedAt });

    const event: OrderAcceptedV1 = {
      eventId: randomUUID(),
      eventType: 'OrderAccepted',
      schemaVersion: 1,
      orderId,
      symbol: order.symbol,
      acceptanceSequence: acceptanceSeq.toString(),
      side: order.side,
      orderType: order.type,
      priceUnits: order.priceUnits.toString(),
      quantityUnits: order.quantityUnits.toString(),
      acceptedAt: acceptedAt.toISOString(),
      correlationId,
    };
    await insertOutboxEvent(tx, event);

    return { replayed: false, receipt, event };
  });
}

function buildReceipt(orderId: string): AcceptanceReceipt {
  const location = `/v1/orders/${orderId}`;
  return {
    status: 202,
    location,
    body: { orderId, status: 'QUEUED', statusUrl: location },
  };
}

async function replayReceipt(
  tx: PoolClient,
  scope: string,
  idempotencyKey: string,
  fingerprint: string,
): Promise<AcceptanceReceipt> {
  const stored = await loadStoredRequest(tx, scope, idempotencyKey);

  if (stored.fingerprint !== fingerprint) {
    throw new ApiError(
      422,
      'IDEMPOTENCY_KEY_REUSED',
      'This Idempotency-Key was already used with a different request.',
    );
  }
  return stored.receipt;
}
