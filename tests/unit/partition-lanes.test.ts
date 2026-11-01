import { describe, expect, it, vi } from 'vitest';
import { PartitionLanes } from '../../src/modules/messaging/partition-lanes.js';

/** Work that finishes only when the test says so, to control the interleaving. */
function controlledWork() {
  const started: string[] = [];
  const finish = new Map<string, { resolve: () => void; reject: (error: Error) => void }>();

  const work = (item: string): Promise<void> =>
    new Promise<void>((resolve, reject) => {
      started.push(item);
      finish.set(item, { resolve, reject });
    });

  return {
    started,
    work,
    finish: async (item: string) => {
      finish.get(item)?.resolve();
      await vi.waitFor(() => undefined);
    },
    fail: async (item: string, error: Error) => {
      finish.get(item)?.reject(error);
      await vi.waitFor(() => undefined);
    },
  };
}

async function isSettled(promise: Promise<void>): Promise<boolean> {
  let settled = false;
  void promise.then(() => {
    settled = true;
  });
  await new Promise((resolve) => setImmediate(resolve));
  return settled;
}

describe('PartitionLanes', () => {
  it('runs one partition in order, one item at a time', async () => {
    const { started, work, finish } = controlledWork();
    const lanes = new PartitionLanes<string>({ capacity: 10, work, onFailure: vi.fn() });

    await lanes.push(0, 'a1');
    await lanes.push(0, 'a2');
    expect(started).toEqual(['a1']);

    await finish('a1');
    expect(started).toEqual(['a1', 'a2']);
  });

  it('lets another partition finish while one waits', async () => {
    const { started, work, finish } = controlledWork();
    const lanes = new PartitionLanes<string>({ capacity: 10, work, onFailure: vi.fn() });

    await lanes.push(0, 'btc');
    await lanes.push(1, 'eth1');
    await lanes.push(1, 'eth2');
    await finish('eth1');

    expect(started).toEqual(['btc', 'eth1', 'eth2']);
  });

  it('holds back the stream only once a lane reaches its capacity', async () => {
    const { work, finish } = controlledWork();
    const lanes = new PartitionLanes<string>({ capacity: 2, work, onFailure: vi.fn() });

    await lanes.push(0, 'a1');
    const full = lanes.push(0, 'a2');
    expect(await isSettled(full)).toBe(false);

    await finish('a1');
    expect(await isSettled(full)).toBe(true);
  });

  it('with a capacity of 1, handles one item at a time overall', async () => {
    const { started, work, finish } = controlledWork();
    const lanes = new PartitionLanes<string>({ capacity: 1, work, onFailure: vi.fn() });

    const first = lanes.push(0, 'a');
    expect(await isSettled(first)).toBe(false);
    expect(started).toEqual(['a']);

    await finish('a');
    expect(await isSettled(first)).toBe(true);
  });

  it('reports a failure and leaves the failed item unfinished in its lane', async () => {
    const { started, work, fail } = controlledWork();
    const onFailure = vi.fn();
    const lanes = new PartitionLanes<string>({ capacity: 10, work, onFailure });

    await lanes.push(0, 'a1');
    await lanes.push(0, 'a2');
    const error = new Error('boom');
    await fail('a1', error);

    expect(onFailure).toHaveBeenCalledWith(error);
    expect(started).toEqual(['a1']);
  });

  it('on stop, starts nothing new, releases the stream and waits for work in progress', async () => {
    const { started, work, finish } = controlledWork();
    const lanes = new PartitionLanes<string>({ capacity: 2, work, onFailure: vi.fn() });

    await lanes.push(0, 'a1');
    const full = lanes.push(0, 'a2');
    lanes.stop();
    expect(await isSettled(full)).toBe(true);

    const settled = lanes.settled();
    expect(await isSettled(settled)).toBe(false);

    await finish('a1');
    await settled;
    expect(started).toEqual(['a1']);
  });

  it('ignores items pushed after stop', async () => {
    const { started, work } = controlledWork();
    const lanes = new PartitionLanes<string>({ capacity: 10, work, onFailure: vi.fn() });

    lanes.stop();
    await lanes.push(0, 'late');

    expect(started).toEqual([]);
  });
});
