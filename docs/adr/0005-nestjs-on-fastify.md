# ADR 0005: NestJS on Fastify

- **Status:** accepted
- **Date:** 2026-11-01

## Context

The API was a standalone Fastify application, and each worker entrypoint created and closed its own resources by hand. This ADR moves both to NestJS without changing what any role does from the outside: the API's paths, bodies, status codes, headers and logs stay as they were, and so do the guarantees in [ADR 0002](0002-messaging-and-matching.md) and [ADR 0004](0004-shutdown-health-and-visibility.md).

## Decision: what Nest owns

Nest owns assembly: controllers, modules, injection, and the order resources close in. Everything else stays plain TypeScript.

| Stays free of Nest | Why |
|---|---|
| Matching engine, order book, price levels | Pure code, tested without any runtime |
| SQL repositories and business functions | They take a pool or a transaction as an argument, which is already explicit |
| Kafka clients, consumer loops, partition lanes | Offsets are committed by hand after durable work; Nest's Kafka transporter would hide that |
| `runAsSoleOwner`, the health server | They must outlive the Nest context, as described below |
| Mock external API | An independent service, on plain Fastify |

Providers are values and factories behind `Symbol` tokens. Business code stays functions: the only new classes are the two controllers, the three worker roles, their modules, the exception filter and a logger adapter.

## Decision: no decorator metadata

`experimentalDecorators` is on and `emitDecoratorMetadata` is off, and every constructor parameter names its token with `@Inject`. tsx compiles with esbuild, which can't emit that metadata, so relying on it would make a production build work and a development run fail. With explicit tokens, `tsc`, tsx and Vitest behave the same with no extra configuration, and `erasableSyntaxOnly` is the only setting given up.

## Decision: Fastify still validates

Routes hand the existing TypeBox schemas to Fastify through `@RouteSchema`. Fastify's Ajv keeps validating with `coerceTypes` and `removeAdditional` off, as [ADR 0001](0001-tooling-and-budgets.md) requires, and Fastify keeps serializing responses. There is no `ValidationPipe`, `class-validator` or second schema language. `@fastify/type-provider-typebox` is gone, because handler parameters now use `Static<typeof Schema>`.

## Decision: adapter settings that keep behavior

A comparison of raw requests and log lines against the previous implementation found these defaults, each of which changes something a client or an operator can see:

| Setting | Without it |
|---|---|
| `bodyParser: false` | Nest registers a form parser, so a form body reaches validation instead of getting 415 |
| `frameworkErrors: undefined` | A malformed URL or an over-long parameter goes through the exception filter, with different headers, body and log level |
| `skipMiddie: true` | Nest loads a middleware layer that nothing here uses |
| `abortOnError: false` | Nest ends the process on a start-up error, instead of letting it reach the entrypoint's handler |
| An `onRoute` hook that copies `config.logLevel` | `@RouteConfig` only forwards `config`, so health checks would log every request |

One `@Catch()` filter, [api-exception.filter.ts](../../src/app/api-exception.filter.ts), turns everything else into the existing error body.

`POST /v1/orders` takes the reply with `@Res`, because the status and location come from the stored receipt.

## Decision: a role's context lives inside its ownership

Each worker role runs as `runAsSoleOwner(..., (ownership) => runRole(...))`. The order that matters:

1. The process takes the PostgreSQL session lock. A standby holds no other connections.
2. `runRole` reports readiness as `starting`, so ownership alone never reads as ready, then creates the Nest application context.
3. The role runs until `ownership.signal` aborts, and drains what it started.
4. `app.close()` closes the pool, and the publisher's producer, in `onApplicationShutdown`. Consumers have already closed themselves when their loops ended.
5. The lock is released, so a successor starts only after all of this has stopped.

Keeping the lock outside the context is what preserves the guarantee: if it were a provider, its release would depend on the order Nest closes things in, and a successor could start while the old role still had resources open.

Resources close in `onApplicationShutdown` because it runs last: for the API that is after the HTTP server has drained, so a request in flight still has its pool.

Nest logs a rejected shutdown hook and carries on, so `app.close()` doesn't reject. `closeOrFail` keeps a failed close visible: it logs `shutdown failed` and sets the exit code to 1, and the other resources still close.

## Versions

Versions are exact, from the registry on 2026-11-01. `npm audit --omit=dev` reported 0 vulnerabilities.

| Package | Pinned | Released | Reason |
|---|---|---|---|
| `@nestjs/common`, `@nestjs/core`, `@nestjs/platform-fastify` | 12.3.2 | 2026-10-27 | Newest stable; the adapter resolves to the project's Fastify 5.12.7 |
| `reflect-metadata` | 0.2.2 | 2024-03-29 | Required by Nest; `emitDecoratorMetadata` stays off |
| `rxjs` | 7.8.2 | 2025-02-22 | Required by Nest; the code here doesn't use it |

## Consequences

- Dependencies and close order are declared in one place for each role, and a test can run a role's whole lifecycle through `runRole`.
- Each role repeats a small module, `CONFIG` and `OWNERSHIP` tokens, and explicit `@Inject` parameters.
- The API's behavior now depends on the adapter settings above. A Nest upgrade should repeat the raw-request comparison, because a changed default wouldn't fail to compile.
- `buildApi` is now asynchronous, so a signal in the few milliseconds before the API's handlers are installed ends the process with the default action.
- A failed close in a worker now logs `shutdown failed`, where it used to log the failure as an error while stopping.
- At `LOG_LEVEL=debug` or below, Nest's own start-up messages appear, through pino.
