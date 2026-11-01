import { createServer, createConnection, type Socket } from "node:net";
import { expect, it } from "vitest";
import { createKafkaLink } from "../server/kafka-link.ts";

it("cuts existing and new TCP connections, then forwards traffic after recovery", async () => {
  const upstream = createServer(socket => socket.pipe(socket));
  await new Promise<void>(resolve => upstream.listen(0, "127.0.0.1", resolve));
  const addr = upstream.address();
  if (!addr || typeof addr === "string") throw new Error("No listener");
  const link = await createKafkaLink(0, { host: "127.0.0.1", port: addr.port });
  if (!link.address || typeof link.address === "string") throw new Error("No proxy");
  const port = link.address.port;
  const connect = () => new Promise<Socket>((resolve, reject) => {
    const socket = createConnection({ host: "127.0.0.1", port });
    socket.once("connect", () => resolve(socket)); socket.once("error", reject);
  });
  const echo = (socket: Socket) => new Promise<string>((resolve, reject) => {
    socket.once("data", data => resolve(data.toString())); socket.once("error", reject); socket.write("real bytes");
  });
  try {
    const first = await connect();
    expect(await echo(first)).toBe("real bytes");
    const closed = new Promise<void>(resolve => first.once("close", () => resolve()));
    link.setOnline(false);
    await closed;
    const rejected = await connect();
    await new Promise<void>(resolve => rejected.once("close", () => resolve()));
    link.setOnline(true);
    const recovered = await connect();
    expect(await echo(recovered)).toBe("real bytes");
    recovered.destroy();
  } finally {
    await link.close();
    await new Promise<void>(resolve => upstream.close(() => resolve()));
  }
});
