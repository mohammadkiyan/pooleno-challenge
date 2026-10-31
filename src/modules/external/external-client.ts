export type DeliveryOutcome =
  | { kind: 'delivered' }
  | { kind: 'transient'; error: string; retryAfterMs: number | undefined }
  | { kind: 'permanent'; error: string };

export interface NotificationTarget {
  url: string;
  timeoutMs: number;
}

/** Statuses that may succeed if repeated. 409 is a destination still working on this key. */
const TRANSIENT_STATUSES = new Set([408, 409, 425, 429, 500, 502, 503, 504]);

/**
 * Sends one attempt under the job's stable key, and classifies how it ended. It never
 * rejects: a network failure or timeout may have reached the destination, so it is
 * transient, and repeating it under the same key cannot repeat the effect.
 */
export async function deliverNotification(
  { url, timeoutMs }: NotificationTarget,
  job: { targetKey: string; payload: string },
): Promise<DeliveryOutcome> {
  // Also bounds reading the response, because `fetch` applies it to the body.
  const signal = AbortSignal.timeout(timeoutMs);

  let response: Response;
  try {
    response = await fetch(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'idempotency-key': job.targetKey,
        // Node.js 26.7's `fetch` can stall on a reused idle connection, which would eat into
        // the timeout here. See docs/adr/0003-phase-3-external-notifications.md.
        connection: 'close',
      },
      body: job.payload,
      // A redirected POST can become a GET, so a redirect is reported rather than followed.
      redirect: 'manual',
      signal,
    });
  } catch (error) {
    return { kind: 'transient', error: describeFailure(error), retryAfterMs: undefined };
  }

  if (response.ok) {
    await response.body?.cancel().catch(() => undefined);
    return { kind: 'delivered' };
  }

  const body = await response.text().catch(() => '');
  const error = `HTTP ${response.status}: ${body}`;
  if (TRANSIENT_STATUSES.has(response.status)) {
    const retryAfterMs = parseRetryAfter(response.headers.get('retry-after'));
    return { kind: 'transient', error, retryAfterMs };
  }
  return { kind: 'permanent', error };
}

/** Reads `Retry-After` in either of its forms: a number of seconds or an HTTP date. */
export function parseRetryAfter(value: string | null, now = Date.now()): number | undefined {
  if (value === null) {
    return undefined;
  }
  if (/^\d+$/.test(value)) {
    return Number(value) * 1_000;
  }

  const date = Date.parse(value);
  return Number.isNaN(date) ? undefined : Math.max(0, date - now);
}

function describeFailure(error: unknown): string {
  // `fetch` reports "fetch failed", and keeps the reason in `cause`.
  const cause = error instanceof Error ? error.cause : undefined;
  return cause instanceof Error ? `${String(error)}: ${cause.message}` : String(error);
}
