import { Inject, Injectable, Module, type DynamicModule } from '@nestjs/common';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { MatchingConfig } from '../../app/config.js';
import { OWNERSHIP, type Role } from '../../app/run-role.js';
import type { Ownership } from '../../app/sole-owner.js';
import {
  InfrastructureModule,
  LOGGER,
  PG_POOL,
} from '../../infrastructure/infrastructure.module.js';
import { runMatchingConsumer } from '../messaging/matching-consumer.js';

const CONFIG = Symbol('MatchingConfig');

@Injectable()
export class MatchingRole implements Role {
  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(LOGGER) private readonly log: Logger,
    @Inject(CONFIG) private readonly config: MatchingConfig,
    @Inject(OWNERSHIP) private readonly ownership: Ownership,
  ) {}

  run(): Promise<void> {
    const { pool, log, config } = this;
    const { signal, reportReadiness } = this.ownership;

    let assigned = false;
    let held = false;
    reportReadiness(() => {
      if (!assigned) {
        return Promise.resolve('no partitions assigned');
      }
      return Promise.resolve(held ? 'a partition is held on a failing event' : undefined);
    });

    return runMatchingConsumer({
      pool,
      kafka: config.kafka,
      groupId: config.groupId,
      sessionTimeoutMs: config.sessionTimeoutMs,
      restartDelayMs: config.restartDelayMs,
      laneCapacity: config.laneCapacity,
      signal,
      log,
      onAssignmentChange: (value) => {
        assigned = value;
      },
      onHeldChange: (value) => {
        held = value;
      },
    });
  }
}

@Module({})
export class MatchingModule {
  static forRole(config: MatchingConfig, log: Logger, ownership: Ownership): DynamicModule {
    return {
      module: MatchingModule,
      imports: [
        InfrastructureModule.forRoot({
          databaseUrl: config.databaseUrl,
          log,
          pool: { statementTimeoutMs: config.statementTimeoutMs },
        }),
      ],
      providers: [
        MatchingRole,
        { provide: CONFIG, useValue: config },
        { provide: OWNERSHIP, useValue: ownership },
      ],
    };
  }
}
