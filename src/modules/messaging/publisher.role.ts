import {
  Inject,
  Injectable,
  Module,
  type DynamicModule,
  type OnApplicationShutdown,
} from '@nestjs/common';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import type { PublisherConfig } from '../../app/config.js';
import { closeOrFail } from '../../app/lifecycle.js';
import { OWNERSHIP, type Role } from '../../app/run-role.js';
import type { Ownership } from '../../app/sole-owner.js';
import {
  InfrastructureModule,
  LOGGER,
  PG_POOL,
} from '../../infrastructure/infrastructure.module.js';
import { createEventProducer, type EventProducer } from './kafka-producer.js';
import { publishOutbox } from './publish-outbox.js';

const CONFIG = Symbol('PublisherConfig');
const EVENT_PRODUCER = Symbol('EventProducer');

@Injectable()
export class PublisherRole implements Role {
  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(LOGGER) private readonly log: Logger,
    @Inject(EVENT_PRODUCER) private readonly producer: EventProducer,
    @Inject(CONFIG) private readonly config: PublisherConfig,
    @Inject(OWNERSHIP) private readonly ownership: Ownership,
  ) {}

  run(): Promise<void> {
    const { pool, log, producer, config } = this;
    const { signal, assertHeld, reportReadiness } = this.ownership;

    reportReadiness(() =>
      producer.assertReachable().then(
        () => undefined,
        () => 'kafka unreachable',
      ),
    );

    return publishOutbox({
      pool,
      producer,
      retry: { baseMs: config.retryBaseMs, capMs: config.retryCapMs },
      pollIntervalMs: config.pollIntervalMs,
      assertHeld,
      signal,
      log,
    });
  }
}

@Module({})
export class PublisherModule implements OnApplicationShutdown {
  constructor(
    @Inject(EVENT_PRODUCER) private readonly producer: EventProducer,
    @Inject(LOGGER) private readonly log: Logger,
  ) {}

  static forRole(config: PublisherConfig, log: Logger, ownership: Ownership): DynamicModule {
    return {
      module: PublisherModule,
      imports: [InfrastructureModule.forRoot({ databaseUrl: config.databaseUrl, log })],
      providers: [
        PublisherRole,
        { provide: CONFIG, useValue: config },
        { provide: OWNERSHIP, useValue: ownership },
        { provide: EVENT_PRODUCER, useFactory: () => createEventProducer(config.kafka) },
      ],
    };
  }

  onApplicationShutdown(): Promise<void> {
    return closeOrFail(this.log, () => this.producer.close());
  }
}
