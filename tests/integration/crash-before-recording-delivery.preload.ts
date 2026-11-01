// Loaded with --import into a real external process. It dies at the worst moment for delivery:
// the destination has answered and has its effect, but nothing local records that yet.
import pg from 'pg';

const query = Reflect.get(pg.Pool.prototype, 'query') as (
  this: unknown,
  ...args: unknown[]
) => unknown;

(pg.Pool.prototype as { query: unknown }).query = function (this: unknown, ...args: unknown[]) {
  if (typeof args[0] === 'string' && args[0].includes(`SET status = 'SUCCEEDED'`)) {
    process.kill(process.pid, 'SIGKILL');
  }
  return query.apply(this, args);
};
