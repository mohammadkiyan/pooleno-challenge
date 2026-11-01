import { Controller, Get, Inject, Res } from '@nestjs/common';
import { RouteConfig } from '@nestjs/platform-fastify';
import type { FastifyReply } from 'fastify';
import type { Pool } from 'pg';
import { PG_POOL } from '../../infrastructure/infrastructure.module.js';
import { readBacklog } from './backlog.repository.js';
import type { KafkaLag } from './kafka-lag.js';

export const READ_KAFKA_LAG = Symbol('ReadKafkaLag');

/**
 * Health and backlog, for operators and supervisors rather than clients, so an ingress must
 * not route these paths. Answers carry only counts, ages and fixed reasons. Health probes are
 * frequent and say nothing new, so they stay out of the logs.
 */
@Controller()
export class OperationsController {
  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(READ_KAFKA_LAG) private readonly readKafkaLag: () => Promise<KafkaLag>,
  ) {}

  @Get('health/live')
  @RouteConfig({ logLevel: 'silent' })
  live() {
    return { status: 'ok' };
  }

  // A pool that cannot lend a connection within its timeout is also out of admission capacity,
  // so this covers both of the things this role needs. Kafka is neither: acceptance outlives it.
  @Get('health/ready')
  @RouteConfig({ logLevel: 'silent' })
  async ready(@Res() reply: FastifyReply): Promise<void> {
    try {
      await this.pool.query('SELECT 1');
      void reply.send({ status: 'ready' });
    } catch {
      void reply.code(503).send({ status: 'not_ready', reason: 'database unavailable' });
    }
  }

  @Get('ops/backlog')
  async backlog() {
    const [backlog, kafka] = await Promise.all([readBacklog(this.pool), this.readKafkaLag()]);
    return { ...backlog, kafka };
  }
}
