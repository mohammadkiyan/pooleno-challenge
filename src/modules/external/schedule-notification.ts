import type { Pool } from 'pg';
import { withTransaction } from '../../infrastructure/transaction.js';
import { claimInboxEvent } from '../messaging/inbox.repository.js';
import { insertExternalJob } from './external-job.repository.js';

/** Fixed, so that a new Kafka group replaying the topic still recognises handled events. */
const EXTERNAL_CONSUMER_NAME = 'external-notification-v1';

/**
 * Records that a notification is owed, in one transaction with the Inbox row. Resolves to
 * false if the event was already scheduled. The caller may commit the Kafka offset only after
 * this resolves. A scheduled job says nothing about delivery, which the worker owns from here.
 */
export function scheduleNotification(
  pool: Pool,
  { eventId, payload }: { eventId: string; payload: string },
): Promise<boolean> {
  return withTransaction(pool, async (tx) => {
    if (!(await claimInboxEvent(tx, EXTERNAL_CONSUMER_NAME, eventId))) {
      return false;
    }
    await insertExternalJob(tx, { eventId, targetKey: `external-notify:${eventId}`, payload });
    return true;
  });
}
