import { DatabaseError, type PoolClient } from 'pg';
import { ApiError } from '../../app/api-error.js';
import type { AcceptanceReceipt } from './order.types.js';

/** Identifies the operation in key uniqueness and fingerprints: its method and canonical path. */
export const CREATE_ORDER_OPERATION = 'POST /v1/orders';

const LOCK_NOT_AVAILABLE = '55P03';

export interface KeyClaim {
  scope: string;
  idempotencyKey: string;
  fingerprint: string;
  orderId: string;
  receipt: AcceptanceReceipt;
}

export interface StoredRequest {
  fingerprint: string;
  receipt: AcceptanceReceipt;
}

/**
 * Inserts the receipt and returns true when this request now owns the key.
 *
 * If another uncommitted transaction holds the key, PostgreSQL waits for it to
 * finish, bounded by lock_timeout. Once it commits, this insert does nothing and
 * returns false. If it rolled back, this request takes the key instead.
 */
export async function claimKey(tx: PoolClient, claim: KeyClaim): Promise<boolean> {
  try {
    const { rowCount } = await tx.query(
      `INSERT INTO idempotency_requests
         (scope, operation, idempotency_key, fingerprint, order_id,
          response_status, response_body, response_location)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (scope, operation, idempotency_key) DO NOTHING`,
      [
        claim.scope,
        CREATE_ORDER_OPERATION,
        claim.idempotencyKey,
        claim.fingerprint,
        claim.orderId,
        claim.receipt.status,
        JSON.stringify(claim.receipt.body),
        claim.receipt.location,
      ],
    );
    return rowCount === 1;
  } catch (error) {
    if (error instanceof DatabaseError && error.code === LOCK_NOT_AVAILABLE) {
      throw new ApiError(
        409,
        'IDEMPOTENCY_REQUEST_IN_FLIGHT',
        'A request with this Idempotency-Key is still being processed. Retry with the same key.',
        1,
      );
    }
    throw error;
  }
}

export async function loadStoredRequest(
  tx: PoolClient,
  scope: string,
  idempotencyKey: string,
): Promise<StoredRequest> {
  const { rows } = await tx.query<{
    fingerprint: string;
    response_status: AcceptanceReceipt['status'];
    response_body: AcceptanceReceipt['body'];
    response_location: string;
  }>(
    `SELECT fingerprint, response_status, response_body, response_location
       FROM idempotency_requests
      WHERE scope = $1 AND operation = $2 AND idempotency_key = $3`,
    [scope, CREATE_ORDER_OPERATION, idempotencyKey],
  );

  const row = rows[0];
  if (!row) {
    throw new Error('A claimed idempotency key has no stored request');
  }

  return {
    fingerprint: row.fingerprint,
    receipt: {
      status: row.response_status,
      location: row.response_location,
      body: row.response_body,
    },
  };
}
