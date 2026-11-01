export interface PartitionLanesOptions<T> {
  /** How many items one lane may hold, counting the one in progress. */
  capacity: number;
  work: (item: T) => Promise<void>;
  /** Told when work rejects. That lane stops, and the caller decides about the others. */
  onFailure: (error: unknown) => void;
}

interface Lane<T> {
  queue: T[];
  running?: Promise<void> | undefined;
  roomAvailable?: () => void;
}

/**
 * Works through each partition's items in order, one at a time, while other partitions
 * proceed. All partitions arrive through one stream, so a full lane holds back that stream
 * and with it every partition, until the lane has room.
 */
export class PartitionLanes<T> {
  readonly #options: PartitionLanesOptions<T>;
  readonly #lanes = new Map<number, Lane<T>>();
  #stopped = false;

  constructor(options: PartitionLanesOptions<T>) {
    this.#options = options;
  }

  /** Resolves once the stream may deliver another item. */
  async push(partition: number, item: T): Promise<void> {
    if (this.#stopped) {
      return;
    }

    const lane = this.#laneOf(partition);
    lane.queue.push(item);
    lane.running ??= this.#run(lane);

    if (lane.queue.length >= this.#options.capacity) {
      await new Promise<void>((resolve) => {
        lane.roomAvailable = resolve;
      });
    }
  }

  /** Starts no more work and releases a waiting `push`. Work in progress carries on. */
  stop(): void {
    this.#stopped = true;
    for (const lane of this.#lanes.values()) {
      lane.roomAvailable?.();
    }
  }

  /** Resolves once no work is in progress. */
  async settled(): Promise<void> {
    await Promise.all([...this.#lanes.values()].map((lane) => lane.running ?? Promise.resolve()));
  }

  #laneOf(partition: number): Lane<T> {
    let lane = this.#lanes.get(partition);
    if (!lane) {
      lane = { queue: [] };
      this.#lanes.set(partition, lane);
    }
    return lane;
  }

  async #run(lane: Lane<T>): Promise<void> {
    try {
      while (!this.#stopped && lane.queue.length > 0) {
        await this.#options.work(lane.queue[0] as T);
        // Not the linked list the book uses, because `capacity` bounds what this has to move.
        lane.queue.shift();
        lane.roomAvailable?.();
      }
    } catch (error) {
      this.#options.onFailure(error);
    } finally {
      lane.running = undefined;
    }
  }
}
