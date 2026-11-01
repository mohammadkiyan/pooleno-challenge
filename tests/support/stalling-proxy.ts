import { connect, createServer, type AddressInfo, type Socket } from 'node:net';

export interface StallingProxy {
  /** The database URL to connect through. */
  url: string;
  /** How many connections are open through the proxy. */
  connections(): number;
  /** How long ago anything last passed through. */
  quietForMs(): number;
  /**
   * Goes quiet with every connection left open, so neither side gets an error and only a timeout
   * can notice. `replies` still passes what the client sends, so a query runs and only its answer
   * is lost. `everything` also swallows the client closing, as across a dead network path.
   */
  stall(scope?: 'replies' | 'everything'): void;
  /** Ends every connection, as when a dead path recovers and PostgreSQL finds the client gone. */
  close(): Promise<void>;
}

/** A TCP proxy in front of PostgreSQL, because a real stall can't be produced from inside pg. */
export async function startStallingProxy(databaseUrl: string): Promise<StallingProxy> {
  const target = new URL(databaseUrl);
  const sockets = new Set<Socket>();
  let stalled: 'replies' | 'everything' | undefined;
  let lastTraffic = Date.now();

  const relay = (from: Socket, to: Socket, isQuiet: () => boolean): void => {
    sockets.add(from);
    from.on('data', (chunk) => {
      lastTraffic = Date.now();
      if (!isQuiet()) {
        to.write(chunk);
      }
    });
    from.on('end', () => {
      if (!isQuiet()) {
        to.end();
      }
    });
    from.on('error', () => {
      to.destroy();
    });
    from.on('close', () => {
      sockets.delete(from);
    });
  };

  // Half-open, so that a client's closing can be swallowed rather than answered by Node itself.
  const server = createServer({ allowHalfOpen: true }, (client) => {
    const database = connect({
      host: target.hostname,
      port: Number(target.port),
      allowHalfOpen: true,
    });
    relay(client, database, () => stalled === 'everything');
    relay(database, client, () => stalled !== undefined);
  });
  await new Promise<void>((resolve) => {
    server.listen(0, '127.0.0.1', resolve);
  });

  const proxied = new URL(databaseUrl);
  proxied.hostname = '127.0.0.1';
  proxied.port = String((server.address() as AddressInfo).port);

  return {
    url: proxied.href,
    // Each connection is two sockets: the client's and the database's.
    connections: () => sockets.size / 2,
    quietForMs: () => Date.now() - lastTraffic,
    stall: (scope = 'everything') => {
      stalled = scope;
    },
    close: () =>
      new Promise((resolve) => {
        for (const socket of sockets) {
          socket.destroy();
        }
        server.close(() => {
          resolve();
        });
      }),
  };
}
