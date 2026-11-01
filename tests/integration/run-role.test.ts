import { Inject, Injectable, Module, type DynamicModule } from '@nestjs/common';
import type { Pool } from 'pg';
import { pino } from 'pino';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { OWNERSHIP, runRole, type Role } from '../../src/app/run-role.js';
import type { Ownership } from '../../src/app/sole-owner.js';
import {
  InfrastructureModule,
  PG_POOL,
} from '../../src/infrastructure/infrastructure.module.js';
import { TEST_DATABASE_URL } from '../support/test-database.js';

const log = pino({ level: 'silent' });

function ownershipRecording(): { ownership: Ownership; readiness: () => Promise<string | undefined> } {
  let check: () => Promise<string | undefined> = () => Promise.resolve(undefined);
  return {
    ownership: {
      signal: new AbortController().signal,
      assertHeld: () => Promise.resolve(),
      reportReadiness: (next) => {
        check = next;
      },
      stopping: () => undefined,
    },
    readiness: () => check(),
  };
}

@Injectable()
class StubRole implements Role {
  static body: (pool: Pool, ownership: Ownership) => Promise<void> = () => Promise.resolve();
  static seenPool: Pool | undefined;

  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(OWNERSHIP) private readonly ownership: Ownership,
  ) {}

  run(): Promise<void> {
    StubRole.seenPool = this.pool;
    return StubRole.body(this.pool, this.ownership);
  }
}

@Module({})
class StubModule {
  static forRole(ownership: Ownership): DynamicModule {
    return {
      module: StubModule,
      imports: [InfrastructureModule.forRoot({ databaseUrl: TEST_DATABASE_URL, log })],
      providers: [StubRole, { provide: OWNERSHIP, useValue: ownership }],
    };
  }
}

function run(ownership: Ownership): Promise<void> {
  return runRole({ module: StubModule.forRole(ownership), role: StubRole, log, ownership });
}

describe('runRole', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    process.exitCode = undefined;
  });

  it('keeps the pool usable for the whole role, and closes it before returning', async () => {
    const { ownership } = ownershipRecording();
    let during: { ok: number } | undefined;
    StubRole.body = async (pool) => {
      during = (await pool.query<{ ok: number }>('SELECT 1 AS ok')).rows[0];
    };

    await run(ownership);

    expect(during).toEqual({ ok: 1 });
    expect(StubRole.seenPool?.ended).toBe(true);
  });

  it('reports not ready until the role has said what it needs', async () => {
    const { ownership, readiness } = ownershipRecording();
    let beforeRoleReports: string | undefined;
    StubRole.body = async (_pool, { reportReadiness }) => {
      beforeRoleReports = await readiness();
      reportReadiness(() => Promise.resolve(undefined));
    };

    await run(ownership);

    expect(beforeRoleReports).toBe('starting');
    expect(await readiness()).toBeUndefined();
  });

  it('closes the resources and still rejects when the role fails', async () => {
    const { ownership } = ownershipRecording();
    StubRole.body = () => Promise.reject(new Error('role failed'));

    await expect(run(ownership)).rejects.toThrow('role failed');

    expect(StubRole.seenPool?.ended).toBe(true);
  });

  it('starts the deadline before closing anything when the role fails with no signal', async () => {
    const stopping = vi.fn<() => void>();
    const { ownership } = ownershipRecording();
    let end: { mock: { invocationCallOrder: number[] } } | undefined;
    StubRole.body = (pool) => {
      end = vi.spyOn(pool, 'end');
      return Promise.reject(new Error('role failed'));
    };

    await expect(run({ ...ownership, stopping })).rejects.toThrow('role failed');

    expect(stopping).toHaveBeenCalledOnce();
    expect(stopping.mock.invocationCallOrder[0]).toBeLessThan(
      end?.mock.invocationCallOrder[0] ?? 0,
    );
  });

  it('exits non-zero when a resource fails to close', async () => {
    const { ownership } = ownershipRecording();
    StubRole.body = (pool) => {
      vi.spyOn(pool, 'end').mockRejectedValue(new Error('close failed'));
      return Promise.resolve();
    };

    await run(ownership);

    expect(process.exitCode).toBe(1);
    vi.restoreAllMocks();
    await StubRole.seenPool?.end();
  });
});
