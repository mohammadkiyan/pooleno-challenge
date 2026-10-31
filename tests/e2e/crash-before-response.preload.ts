// Loaded with --import into a real API process. It dies at the worst moment for a
// client: the transaction has committed, but the first response has not been sent.
import { ServerResponse } from 'node:http';

ServerResponse.prototype.writeHead = function (this: ServerResponse) {
  process.kill(process.pid, 'SIGKILL');
  return this;
};
