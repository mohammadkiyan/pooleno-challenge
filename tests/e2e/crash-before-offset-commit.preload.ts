// Loaded with --import into a real matching process. It dies at the worst moment for a
// consumer: the database transaction has committed, but its Kafka offset has not.
import { Consumer } from '@platformatic/kafka';

const crashAt = BigInt(process.env.CRASH_BEFORE_COMMITTING_OFFSET ?? '-1');
const commit = Reflect.get(Consumer.prototype, 'commit') as (
  this: unknown,
  ...args: unknown[]
) => Promise<void>;

(Consumer.prototype as { commit: unknown }).commit = function (
  this: unknown,
  options: { offsets: { offset: bigint }[] },
) {
  if (options.offsets.some(({ offset }) => offset === crashAt)) {
    process.kill(process.pid, 'SIGKILL');
  }
  return commit.call(this, options);
};
