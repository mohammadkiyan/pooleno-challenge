/** A failure the client can act on. `code` is part of the API contract. */
export class ApiError extends Error {
  readonly statusCode: number;
  readonly code: string;
  readonly retryAfterSeconds: number | undefined;

  constructor(statusCode: number, code: string, message: string, retryAfterSeconds?: number) {
    super(message);
    this.name = 'ApiError';
    this.statusCode = statusCode;
    this.code = code;
    this.retryAfterSeconds = retryAfterSeconds;
  }
}
