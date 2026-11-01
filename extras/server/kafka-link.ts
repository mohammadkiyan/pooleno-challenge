import { createServer, createConnection, type Socket } from "node:net";

/** A real TCP interruption, scoped to the Kafka VIEWER listener; existing clients use other listeners. */
export async function createKafkaLink(port: number, upstream: { host: string; port: number }) {
  let online = true;
  const sockets = new Set<Socket>();
  const server = createServer((client) => {
    if (!online) { client.destroy(); return; }
    const remote = createConnection(upstream);
    for (const socket of [client, remote]) {
      sockets.add(socket);
      socket.on("close", () => sockets.delete(socket));
      socket.on("error", () => { client.destroy(); remote.destroy(); });
    }
    client.on("close", () => remote.destroy());
    remote.on("close", () => client.destroy());
    client.pipe(remote).pipe(client);
  });
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "0.0.0.0", () => { server.off("error", reject); resolve(); });
  });
  return {
    address: server.address(),
    setOnline(value: boolean) {
      online = value;
      if (!value) for (const socket of sockets) socket.destroy();
    },
    close() {
      for (const socket of sockets) socket.destroy();
      return new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    },
  };
}
