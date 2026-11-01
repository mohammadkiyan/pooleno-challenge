import { Inject, Injectable, Module, type DynamicModule } from '@nestjs/common';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { ExternalConfig } from '../../app/config.js';
import { OWNERSHIP, type Role } from '../../app/run-role.js';
import type { Ownership } from '../../app/sole-owner.js';
import {
  InfrastructureModule,
  LOGGER,
  PG_POOL,
} from '../../infrastructure/infrastructure.module.js';
import { runExternalConsumer } from '../messaging/external-consumer.js';
import { deliverNotification } from './external-client.js';
import { runNotificationWorker } from './notification-worker.js';

const CONFIG = Symbol('ExternalConfig');

@Injectable()
export class ExternalRole implements Role {
  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(LOGGER) private readonly log: Logger,
    @Inject(CONFIG) private readonly config: ExternalConfig,
    @Inject(OWNERSHIP) private readonly ownership: Ownership,
  ) {}

  async run(): Promise<void> {
    const { pool, log, config } = this;
    const { signal, assertHeld, reportReadiness } = this.ownership;
    const target = { url: config.targetUrl, timeoutMs: config.timeoutMs };

    // The destination is deliberately not part of this: its outage is a delivery condition.
    let assigned = false;
    reportReadiness(() => Promise.resolve(assigned ? undefined : 'no partitions assigned'));

    // Neither loop rejects, because each resumes from durable state until the signal
    // aborts. Kafka being down therefore stops no delivery, and the API being down no intake.
    await Promise.all([
      runExternalConsumer({
        pool,
        kafka: config.kafka,
        groupId: config.groupId,
        sessionTimeoutMs: config.sessionTimeoutMs,
        restartDelayMs: config.restartDelayMs,
        signal,
        log,
        onAssignmentChange: (value) => {
          assigned = value;
        },
      }),
      runNotificationWorker({
        pool,
        deliver: (job) => deliverNotification(target, job),
        retry: config.retry,
        pollIntervalMs: config.pollIntervalMs,
        restartDelayMs: config.restartDelayMs,
        assertHeld,
        signal,
        log,
      }),
    ]);
  }
}

@Module({})
export class ExternalModule {
  static forRole(config: ExternalConfig, log: Logger, ownership: Ownership): DynamicModule {
    return {
      module: ExternalModule,
      imports: [InfrastructureModule.forRoot({ databaseUrl: config.databaseUrl, log })],
      providers: [
        ExternalRole,
        { provide: CONFIG, useValue: config },
        { provide: OWNERSHIP, useValue: ownership },
      ],
    };
  }
}
