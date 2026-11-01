const MAX_STORED_ERROR_LENGTH = 500;

/** Text from outside this system can contain NUL, which PostgreSQL refuses in a text column. */
export function toStoredError(error: string): string {
  return error.replaceAll('\0', '').slice(0, MAX_STORED_ERROR_LENGTH);
}
