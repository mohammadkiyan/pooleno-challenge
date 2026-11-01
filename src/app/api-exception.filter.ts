import {
  Catch,
  HttpException,
  NotFoundException,
  type ArgumentsHost,
  type ExceptionFilter,
} from '@nestjs/common';
import type { FastifyReply, FastifyRequest } from 'fastify';
import { ApiError } from './api-error.js';

/**
 * Gives every failure that reaches Nest the same shape. Fastify still answers what it rejects
 * before routing, such as a malformed URL, as ADR 0005 explains.
 */
@Catch()
export class ApiExceptionFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost): void {
    const http = host.switchToHttp();
    const request = http.getRequest<FastifyRequest>();
    const reply = http.getResponse<FastifyReply>();
    const apiError = toApiError(exception, request);

    if (apiError.statusCode >= 500) {
      request.log.error({ err: exception }, 'request failed');
    }
    if (apiError.retryAfterSeconds !== undefined) {
      void reply.header('retry-after', apiError.retryAfterSeconds);
    }

    void reply
      .code(apiError.statusCode)
      .send({ code: apiError.code, message: apiError.message });
  }
}

function toApiError(error: unknown, request: FastifyRequest): ApiError {
  if (error instanceof ApiError) {
    return error;
  }
  // Only Nest's handler for an unmatched route throws this. No controller does.
  if (error instanceof NotFoundException) {
    return new ApiError(404, 'NOT_FOUND', `No route for ${request.method} ${request.url}`);
  }
  const clientStatus = clientErrorStatus(error);
  if (clientStatus !== undefined) {
    return new ApiError(clientStatus, 'INVALID_REQUEST', (error as Error).message);
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

/** Nest wraps a client error from Fastify's own pipeline in an HttpException. */
function clientErrorStatus(error: unknown): number | undefined {
  const status =
    error instanceof HttpException
      ? error.getStatus()
      : error instanceof Error && 'statusCode' in error
        ? error.statusCode
        : undefined;
  return typeof status === 'number' && status >= 400 && status < 500 ? status : undefined;
}
