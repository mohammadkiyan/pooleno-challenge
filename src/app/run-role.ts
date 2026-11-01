import type { DynamicModule, Type } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import type { Logger } from 'pino';
import { NestLogger } from './nest-logger.js';
import type { Ownership } from './sole-owner.js';

export const OWNERSHIP = Symbol('Ownership');

export interface Role {
  /** Works until `Ownership.signal` aborts, and returns once what it started has drained. */
  run(): Promise<void>;
}

export interface RunRoleOptions {
  module: DynamicModule;
  role: Type<Role>;
  log: Logger;
  ownership: Ownership;
}

/**
 * Creates a role's resources, runs it and closes them, all inside ownership: `runAsSoleOwner`
 * calls this once it holds the lock, and releases the lock once this returns. A standby
 * therefore holds no connections, and a successor starts after everything here has stopped.
 */
export async function runRole({ module, role, log, ownership }: RunRoleOptions): Promise<void> {
  // Ownership alone would read as ready until the role reports what else it needs.
  ownership.reportReadiness(() => Promise.resolve('starting'));

  const app = await NestFactory.createApplicationContext(module, {
    abortOnError: false,
    logger: new NestLogger(log),
  });
  try {
    await app.get(role).run();
  } finally {
    await app.close();
  }
}
