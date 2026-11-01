import type { FastifyPluginCallbackTypebox } from '@fastify/type-provider-typebox';
import type { Pool } from 'pg';
import { readBacklog } from './backlog.repository.js';
import type { KafkaLag } from './kafka-lag.js';

export interface OperationsRoutesOptions {
  pool: Pool;
  readKafkaLag: () => Promise<KafkaLag>;
}

/**
 * Health and backlog, for operators and supervisors rather than clients, so an ingress must
 * not route these paths. Answers carry only counts, ages and fixed reasons. Health probes are
 * frequent and say nothing new, so they stay out of the logs.
 */
export const operationsRoutes: FastifyPluginCallbackTypebox<OperationsRoutesOptions> = (
  app,
  { pool, readKafkaLag },
  done,
) => {
  app.get('/health/live', { logLevel: 'silent' }, () => ({ status: 'ok' }));

  // A pool that cannot lend a connection within its timeout is also out of admission capacity,
  // so this covers both of the things this role needs. Kafka is neither: acceptance outlives it.
  app.get('/health/ready', { logLevel: 'silent' }, async (_request, reply) => {
    try {
      await pool.query('SELECT 1');
      return { status: 'ready' };
    } catch {
      return reply.code(503).send({ status: 'not_ready', reason: 'database unavailable' });
    }
  });

  app.get('/ops/backlog', async () => {
    const [backlog, kafka] = await Promise.all([readBacklog(pool), readKafkaLag()]);
    return { ...backlog, kafka };
  });

  done();
};
