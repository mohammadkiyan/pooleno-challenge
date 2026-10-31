import { randomUUID } from 'node:crypto';
import type { TypeBoxTypeProvider } from '@fastify/type-provider-typebox';
import Fastify, { LogController } from 'fastify';
import type { Pool } from 'pg';
import type { Logger } from 'pino';
import { orderRoutes } from '../modules/orders/order.routes.js';
import { ApiError } from './api-error.js';

export interface ApiDependencies {
  pool: Pool;
  log: Logger;
  lockTimeoutMs: number;
}

export function buildApi({ pool, log, lockTimeoutMs }: ApiDependencies) {
  const app = Fastify({
    loggerInstance: log,
    genReqId: () => randomUUID(),
    logController: new LogController({ requestIdLogLabel: 'requestId' }),
    bodyLimit: 16 * 1024,
    ajv: {
      customOptions: {
        // Fastify's defaults would turn the JSON number 100.5 into the string "100.5"
        // after it has passed through a float, and silently drop unknown properties.
        coerceTypes: false,
        removeAdditional: false,
      },
    },
  }).withTypeProvider<TypeBoxTypeProvider>();

  app.setErrorHandler((error, request, reply) => {
    const apiError = toApiError(error);

    if (apiError.statusCode >= 500) {
      request.log.error({ err: error }, 'request failed');
    }
    if (apiError.retryAfterSeconds !== undefined) {
      void reply.header('retry-after', apiError.retryAfterSeconds);
    }

    return reply.code(apiError.statusCode).send({ code: apiError.code, message: apiError.message });
  });

  app.setNotFoundHandler((request, reply) =>
    reply.code(404).send({ code: 'NOT_FOUND', message: `No route for ${request.method} ${request.url}` }),
  );

  void app.register(orderRoutes, { pool, lockTimeoutMs });

  return app;
}

function toApiError(error: unknown): ApiError {
  if (error instanceof ApiError) {
    return error;
  }
  if (isFastifyClientError(error)) {
    return new ApiError(error.statusCode, 'INVALID_REQUEST', error.message);
  }

  // After validation the only work left is PostgreSQL I/O, and the commit may or may
  // not have happened. Retrying with the same Idempotency-Key is safe in either case.
  return new ApiError(
    503,
    'SERVICE_UNAVAILABLE',
    'The outcome could not be confirmed. Retry with the same Idempotency-Key.',
    1,
  );
}

function isFastifyClientError(error: unknown): error is Error & { statusCode: number } {
  return (
    error instanceof Error &&
    'statusCode' in error &&
    typeof error.statusCode === 'number' &&
    error.statusCode >= 400 &&
    error.statusCode < 500
  );
}
