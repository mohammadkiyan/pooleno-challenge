# API

The API listens on `http://127.0.0.1:8080`. Every request shares one fixed identity scope, because authentication is out of scope for this project.

## `POST /v1/orders`

```bash
curl -i http://127.0.0.1:8080/v1/orders \
  -H 'content-type: application/json' \
  -H 'idempotency-key: order-2026-10-31-0001' \
  -d '{"symbol":"BTC-USD","side":"BUY","type":"LIMIT","price":"100.00","quantity":"2.500"}'
```

```http
HTTP/1.1 202 Accepted
location: /v1/orders/fbe6a61c-bbdd-4346-886d-af24e9de3847
content-type: application/json; charset=utf-8

{"orderId":"fbe6a61c-bbdd-4346-886d-af24e9de3847","status":"QUEUED","statusUrl":"/v1/orders/fbe6a61c-bbdd-4346-886d-af24e9de3847"}
```

`202` means the order is durable, not that it has matched. An accepted order is never rejected later.

| Field | Rules |
|---|---|
| `Idempotency-Key` header | Required. 16 to 128 characters of `A-Za-z0-9_-`. Use one key per order, and reuse it for every retry of that order |
| `symbol` | A market from the table below |
| `side` | `BUY` or `SELL` |
| `type` | `LIMIT` |
| `price`, `quantity` | JSON strings holding plain decimals. Excess precision is rejected, never rounded |

A retry with the same key and the same meaning replays the original response, even after the order has moved on. `2.5` and `2.500` have the same meaning.

| Symbol | Price tick | Maximum price | Quantity step | Maximum quantity |
|---|---|---|---|---|
| `BTC-USD` | 0.01 | 10,000,000.00 | 0.00000001 | 10,000.00000000 |
| `ETH-USD` | 0.05 | 1,000,000.00 | 0.0010 | 100,000.0000 |

## `GET /v1/orders/{orderId}`

Reports the current state, with the order's trades in execution order:

```bash
curl http://127.0.0.1:8080/v1/orders/fbe6a61c-bbdd-4346-886d-af24e9de3847
```

```json
{
  "orderId": "fbe6a61c-bbdd-4346-886d-af24e9de3847",
  "symbol": "ETH-USD",
  "side": "BUY",
  "type": "LIMIT",
  "status": "PARTIALLY_FILLED",
  "price": "100.00",
  "originalQuantity": "4.0000",
  "executedQuantity": "1.0000",
  "remainingQuantity": "3.0000",
  "acceptanceSequence": "2",
  "acceptedAt": "2026-10-31T09:47:27.367Z",
  "updatedAt": "2026-10-31T09:47:27.412Z",
  "trades": [
    {
      "tradeId": "0b3f7c3e-5a43-4a53-9d3b-6f1d1f6a2c10",
      "role": "TAKER",
      "price": "99.00",
      "quantity": "1.0000",
      "executedAt": "2026-10-31T09:47:27.412Z"
    }
  ]
}
```

| `status` | Meaning |
|---|---|
| `QUEUED` | Accepted and durable, not yet matched |
| `OPEN` | In the book with nothing executed |
| `PARTIALLY_FILLED` | In the book with part of it executed |
| `FILLED` | Fully executed |

A trade's `role` is `MAKER` if the order was resting and `TAKER` if it arrived and crossed. Trades execute at the maker's price. The counterparty's order is not shown.

## Errors

Every error has the body `{"code": "...", "message": "..."}` and never carries acceptance data.

| Status | `code` | Meaning | What the client should do |
|---|---|---|---|
| 400 | `INVALID_REQUEST` | Missing, malformed or repeated key; invalid body; malformed `orderId` | Fix the request |
| 400 | `UNSUPPORTED_SYMBOL` | No market for this symbol | Fix the request |
| 400 | `INVALID_PRICE`, `INVALID_QUANTITY` | Not a plain decimal, zero, too precise, off-increment or too large | Fix the request |
| 404 | `ORDER_NOT_FOUND`, `NOT_FOUND` | Unknown order or route | Fix the request |
| 409 | `IDEMPOTENCY_REQUEST_IN_FLIGHT` | The first request with this key is still running | Retry with the same key after `Retry-After` |
| 413, 415 | `INVALID_REQUEST` | Body over 16 KiB, or an unsupported content type | Fix the request |
| 422 | `IDEMPOTENCY_KEY_REUSED` | This key was used for a different order | Use a new key for a new order |
| 503 | `SERVICE_UNAVAILABLE` | The outcome could not be confirmed | Retry with the same key after `Retry-After` |

Health and backlog endpoints are in [Operations](operations.md).
