import { Pool } from 'pg';
import { pino } from 'pino';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { KafkaMessage } from '../../src/modules/messaging/kafka-producer.js';
import { publishDueEvents } from '../../src/modules/messaging/publish-outbox.js';
import { acceptEvent, ethOrder, validOrder } from '../support/orders.js';
import { resetDatabase, TEST_DATABASE_URL } from '../support/test-database.js';

const FAILS_ON_MARK_PUBLISHED = `
  CREATE OR REPLACE FUNCTION fail_mark_published() RETURNS trigger AS $$
  BEGIN RAISE EXCEPTION 'simulated failure while marking published'; END;
  $$ LANGUAGE plpgsql;

  CREATE TRIGGER fail_mark_published BEFORE UPDATE ON outbox_events
  FOR EACH ROW WHEN (NEW.published_at IS NOT NULL) EXECUTE FUNCTION fail_mark_published();
`;
const REMOVES_THE_FAILURE = `
  DROP TRIGGER IF EXISTS fail_mark_published ON outbox_events;
  DROP FUNCTION IF EXISTS fail_mark_published;
`;

let pool: Pool;

beforeAll(() => {
  pool = new Pool({ connectionString: TEST_DATABASE_URL });
});

afterAll(() => pool.end());

beforeEach(() => resetDatabase(pool));

afterEach(() => pool.query(REMOVES_THE_FAILURE));

/** Records what reached the "broker", and refuses anything for the given symbols. */
function fakeProducer({ failing = [] as string[] } = {}) {
  const sent: KafkaMessage[] = [];
  return {
    sent,
    send: (message: KafkaMessage) => {
      if (failing.includes(message.key)) {
        return Promise.reject(new Error(`broker unavailable for ${message.key}`));
      }
      sent.push(message);
      return Promise.resolve();
    },
  };
}

function runPass(producer: ReturnType<typeof fakeProducer>, signal = new AbortController().signal) {
  return publishDueEvents({
    pool,
    producer,
    retry: { baseMs: 60_000, capMs: 240_000 },
    signal,
    log: pino({ level: 'silent' }),
  });
}

async function readEvent(eventId: string) {
  const { rows } = await pool.query<{
    published: boolean;
    attempts: number;
    last_error: string | null;
    delay_seconds: number;
  }>(
    `SELECT published_at IS NOT NULL AS published, attempts, last_error,
            round(extract(epoch FROM next_attempt_at - now()))::int AS delay_seconds
       FROM outbox_events
      WHERE event_id = $1`,
    [eventId],
  );
  const row = rows[0];
  if (!row) {
    throw new Error(`Event ${eventId} does not exist`);
  }
  return row;
}

function sentSequences(producer: ReturnType<typeof fakeProducer>) {
  return producer.sent.map(
    (message) => (JSON.parse(message.value) as { acceptanceSequence: string }).acceptanceSequence,
  );
}

const sell = () => ethOrder('SELL', '100.00', '0.0010');

describe('publishing the Outbox', () => {
  it('publishes each symbol in sequence order, keyed by symbol, with the stored payload', async () => {
    const first = await acceptEvent(pool, sell());
    const second = await acceptEvent(pool, sell());
    const producer = fakeProducer();

    expect(await runPass(producer)).toBe(1);
    expect(await readEvent(first.eventId)).toMatchObject({ published: true, last_error: null });
    expect(await readEvent(second.eventId)).toMatchObject({ published: false });

    expect(await runPass(producer)).toBe(1);
    expect(await runPass(producer)).toBe(0);

    expect(producer.sent.map((message) => message.key)).toEqual(['ETH-USD', 'ETH-USD']);
    expect(producer.sent.map((message) => JSON.parse(message.value) as unknown)).toEqual([
      first,
      second,
    ]);
  });

  it('records a failed send durably and holds back that symbol while others continue', async () => {
    const head = await acceptEvent(pool, sell());
    const later = await acceptEvent(pool, sell());
    const other = await acceptEvent(pool, validOrder);
    const producer = fakeProducer({ failing: ['ETH-USD'] });

    expect(await runPass(producer)).toBe(1);

    const failure = await readEvent(head.eventId);
    expect(failure).toMatchObject({
      published: false,
      attempts: 1,
      last_error: 'Error: broker unavailable for ETH-USD',
    });
    expect(failure.delay_seconds).toBe(60);
    expect(await readEvent(later.eventId)).toMatchObject({ published: false, attempts: 0 });
    expect(await readEvent(other.eventId)).toMatchObject({ published: true });
    expect(producer.sent.map((message) => message.key)).toEqual(['BTC-USD']);
  });

  it('never sends a later event while the head is waiting to retry', async () => {
    const head = await acceptEvent(pool, sell());
    const later = await acceptEvent(pool, sell());
    const failing = fakeProducer({ failing: ['ETH-USD'] });
    await runPass(failing);

    const healthy = fakeProducer();
    expect(await runPass(healthy)).toBe(0);
    expect(healthy.sent).toEqual([]);

    await pool.query('UPDATE outbox_events SET next_attempt_at = now() WHERE event_id = $1', [
      head.eventId,
    ]);
    await runPass(healthy);
    await runPass(healthy);

    expect(sentSequences(healthy)).toEqual([head.acceptanceSequence, later.acceptanceSequence]);
  });

  it('doubles the delay between attempts up to the cap', async () => {
    const { eventId } = await acceptEvent(pool, sell());
    const producer = fakeProducer({ failing: ['ETH-USD'] });
    const delays: number[] = [];

    for (let attempt = 0; attempt < 4; attempt += 1) {
      await runPass(producer);
      delays.push((await readEvent(eventId)).delay_seconds);
      await pool.query('UPDATE outbox_events SET next_attempt_at = now() WHERE event_id = $1', [
        eventId,
      ]);
    }

    expect(delays).toEqual([60, 120, 240, 240]);
    expect((await readEvent(eventId)).attempts).toBe(4);
  });

  it('does not mistake a failure after the send for a failed send, and resends the same event', async () => {
    const event = await acceptEvent(pool, sell());
    const producer = fakeProducer();
    await pool.query(FAILS_ON_MARK_PUBLISHED);

    await expect(runPass(producer)).rejects.toThrow('simulated failure while marking published');

    expect(await readEvent(event.eventId)).toMatchObject({
      published: false,
      attempts: 0,
      last_error: null,
    });

    await pool.query(REMOVES_THE_FAILURE);
    await runPass(producer);

    expect(producer.sent).toHaveLength(2);
    expect(producer.sent[1]).toEqual(producer.sent[0]);
    expect(await readEvent(event.eventId)).toMatchObject({ published: true, attempts: 0 });
  });

  it('sends nothing once it has been told to stop', async () => {
    await acceptEvent(pool, sell());
    const stopped = new AbortController();
    stopped.abort();
    const producer = fakeProducer();

    expect(await runPass(producer, stopped.signal)).toBe(0);

    expect(producer.sent).toEqual([]);
  });
});
