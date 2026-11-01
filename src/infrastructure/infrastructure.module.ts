import { Inject, Module, type DynamicModule, type OnApplicationShutdown } from '@nestjs/common';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import { closeOrFail } from '../app/lifecycle.js';
import { createPool, type PoolOptions } from './db.js';

export const LOGGER = Symbol('Logger');
export const PG_POOL = Symbol('PgPool');

export interface InfrastructureOptions {
  databaseUrl: string;
  log: Logger;
  pool?: PoolOptions;
}

/** Owns the process's connection pool: it exists as long as the Nest application does. */
@Module({})
export class InfrastructureModule implements OnApplicationShutdown {
  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(LOGGER) private readonly log: Logger,
  ) {}

  static forRoot({ databaseUrl, log, pool }: InfrastructureOptions): DynamicModule {
    return {
      module: InfrastructureModule,
      global: true,
      providers: [
        { provide: LOGGER, useValue: log },
        { provide: PG_POOL, useFactory: () => createPool(databaseUrl, log, pool) },
      ],
      exports: [LOGGER, PG_POOL],
    };
  }

  /** Runs after the HTTP server has drained, so no request is left to need a connection. */
  onApplicationShutdown(): Promise<void> {
    return closeOrFail(this.log, () => this.pool.end());
  }
}
