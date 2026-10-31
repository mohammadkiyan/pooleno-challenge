import type { Pool } from 'pg';
import type { Logger } from 'pino';
import { pause } from '../../infrastructure/pause.js';
import type { EventProducer } from './kafka-producer.js';
import { findDueHeads, markPublished, recordSendFailure } from './outbox.repository.js';

export interface RetryPolicy {
  baseMs: number;
  capMs: number;
}

export interface PublishPass {
  pool: Pool;
  producer: Pick<EventProducer, 'send'>;
  retry: RetryPolicy;
  signal: AbortSignal;
  log: Logger;
}

/**
 * Sends each symbol's earliest due event and waits for the broker's acknowledgement.
 * Each symbol advances by at most one event per pass, and only after the previous
 * event is marked published, so per-symbol order holds. A failing symbol does not
 * hold back the others. Resolves to the number of events published.
 *
 * Only a failed send is recorded as a failure. If marking fails after a successful
 * send, Kafka has the event, so the error propagates and stops the publisher. Its
 * successor resends the same event, and consumers ignore the duplicate.
 */
export async function publishDueEvents(pass: PublishPass): Promise<number> {
  const { pool, producer, retry, signal, log } = pass;
  let published = 0;

  for (const head of await findDueHeads(pool)) {
    if (signal.aborted) {
      break;
    }
    const context = {
      eventId: head.eventId,
      symbol: head.symbol,
      acceptanceSequence: head.acceptanceSeq,
    };

    try {
      await producer.send({ key: head.symbol, value: head.payload });
    } catch (error) {
      const retryAfterMs = Math.min(retry.capMs, retry.baseMs * 2 ** head.attempts);
      log.warn({ ...context, err: error, retryAfterMs }, 'send failed; will retry this event');
      await recordSendFailure(pool, head.eventId, { error: String(error), retryAfterMs });
      continue;
    }

    await markPublished(pool, head.eventId);
    published += 1;
    log.info(context, 'event published');
  }

  return published;
}

/**
 * Publishes until `signal` aborts. Checking ownership before each pass stops the
 * loop if the lock's connection has gone quiet without yet reporting an error.
 */
export async function publishOutbox(
  pass: PublishPass & { assertHeld: () => Promise<void>; pollIntervalMs: number },
): Promise<void> {
  const { assertHeld, pollIntervalMs, signal } = pass;

  while (!signal.aborted) {
    await assertHeld();
    const published = await publishDueEvents(pass);
    if (published === 0) {
      await pause(pollIntervalMs, signal);
    }
  }
}
