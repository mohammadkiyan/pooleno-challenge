import { setTimeout as sleep } from 'node:timers/promises';

/** Waits for `ms`, or until `signal` aborts. It never rejects. */
export async function pause(ms: number, signal: AbortSignal): Promise<void> {
  await sleep(ms, undefined, { signal }).catch(() => undefined);
}
