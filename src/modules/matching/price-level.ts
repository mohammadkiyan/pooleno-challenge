export interface RestingOrder {
  orderId: string;
  priceUnits: bigint;
  remainingUnits: bigint;
}

interface QueueNode {
  order: RestingOrder;
  next: QueueNode | undefined;
}

/** The orders resting at one price, oldest first. Appending and removing the oldest are O(1). */
export class PriceLevel {
  #head: QueueNode | undefined;
  #tail: QueueNode | undefined;

  get oldest(): RestingOrder | undefined {
    return this.#head?.order;
  }

  append(order: RestingOrder): void {
    const node: QueueNode = { order, next: undefined };
    if (this.#tail) {
      this.#tail.next = node;
    } else {
      this.#head = node;
    }
    this.#tail = node;
  }

  removeOldest(): void {
    this.#head = this.#head?.next;
    if (!this.#head) {
      this.#tail = undefined;
    }
  }

  *[Symbol.iterator](): Generator<RestingOrder> {
    for (let node = this.#head; node; node = node.next) {
      yield node.order;
    }
  }
}
