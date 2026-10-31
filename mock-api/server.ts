// A stand-in for the external API, which shares no code or storage with the application.
// `/v1/notifications` is the destination's contract, and `/__control` is how a test or
// a reader breaks it on purpose and sees the effects. Nothing here belongs in production.
import Fastify from 'fastify';
import { IdempotencyStore } from './idempotency-store.js';

interface Fault {
  /**
   * - `lost-response`: commits the effect, then resets the connection.
   * - `timeout-after-commit`: commits the effect, then never answers.
   * - `timeout`: never answers, and causes no effect.
   * - `status`: answers with `status` and optional `retryAfter`, and causes no effect.
   */
  mode: 'lost-response' | 'timeout-after-commit' | 'timeout' | 'status';
  status?: number;
  retryAfter?: string;
}

const databasePath = process.env.MOCK_API_DATABASE;
if (!databasePath) {
  throw new Error('MOCK_API_DATABASE is required: durable storage is the point of this mock');
}

const store = new IdempotencyStore(databasePath);
const faults: Fault[] = [];

const app = Fastify({
  logger: { level: process.env.LOG_LEVEL ?? 'info' },
  // A request left unanswered on purpose must not hold up shutdown.
  forceCloseConnections: true,
});

function isJson(body: string | undefined): body is string {
  try {
    JSON.parse(body ?? '');
    return true;
  } catch {
    return false;
  }
}

await app.register((destination, _options, done) => {
  // The body must reach the store as sent, because the same key must mean the same bytes.
  destination.removeAllContentTypeParsers();
  destination.addContentTypeParser(
    'application/json',
    { parseAs: 'string' },
    (_request, body, parsed) => {
      parsed(null, body);
    },
  );

  destination.post<{ Body: string | undefined }>('/v1/notifications', (request, reply) => {
    const idempotencyKey = request.headers['idempotency-key'];
    if (typeof idempotencyKey !== 'string' || idempotencyKey === '') {
      return reply.code(400).send({ code: 'MISSING_IDEMPOTENCY_KEY' });
    }
    if (!isJson(request.body)) {
      return reply.code(400).send({ code: 'INVALID_BODY' });
    }

    const fault = faults.shift();
    if (fault?.mode === 'status') {
      if (fault.retryAfter !== undefined) {
        void reply.header('retry-after', fault.retryAfter);
      }
      return reply.code(fault.status ?? 503).send({ code: 'INJECTED_FAULT' });
    }
    if (fault?.mode === 'timeout') {
      return reply.hijack();
    }

    const result = store.apply(idempotencyKey, request.body);
    request.log.info({ idempotencyKey, outcome: result.outcome }, 'notification handled');

    if (result.outcome === 'conflict') {
      return reply.code(422).send({ code: 'IDEMPOTENCY_KEY_REUSED' });
    }
    if (fault?.mode === 'lost-response') {
      request.raw.socket.destroy();
      return reply.hijack();
    }
    if (fault?.mode === 'timeout-after-commit') {
      return reply.hijack();
    }

    if (result.outcome === 'replayed') {
      void reply.header('idempotent-replayed', 'true');
    }
    return reply
      .code(result.response.status)
      .type('application/json')
      .send(result.response.body);
  });

  done();
});

app.post<{ Body: Fault & { times?: number } }>(
  '/__control/faults',
  {
    schema: {
      body: {
        type: 'object',
        additionalProperties: false,
        required: ['mode'],
        properties: {
          mode: { enum: ['lost-response', 'timeout-after-commit', 'timeout', 'status'] },
          status: { type: 'integer', minimum: 400, maximum: 599 },
          retryAfter: { type: 'string' },
          times: { type: 'integer', minimum: 1 },
        },
      },
    },
  },
  (request, reply) => {
    const { times = 1, ...fault } = request.body;
    for (let injected = 0; injected < times; injected += 1) {
      faults.push(fault);
    }
    return reply.code(204).send();
  },
);

app.delete('/__control/faults', (_request, reply) => {
  faults.length = 0;
  return reply.code(204).send();
});

app.get('/__control/effects', () => ({ effects: store.listEffects() }));

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    void app.close().finally(() => {
      store.close();
    });
  });
}

await app.listen({
  host: process.env.MOCK_API_HOST ?? '127.0.0.1',
  port: Number(process.env.MOCK_API_PORT ?? 58080),
});
