import { createHash, randomUUID } from 'node:crypto';
import { DatabaseSync } from 'node:sqlite';

export interface StoredResponse {
  status: number;
  body: string;
}

export type ApplyResult =
  | { outcome: 'applied' | 'replayed'; response: StoredResponse }
  | { outcome: 'conflict' };

export interface Effect {
  id: string;
  idempotencyKey: string;
  body: string;
  appliedAt: string;
}

/**
 * The destination's own durable state, which shares nothing with the application. The record
 * of a key and the effect it caused commit in one SQLite transaction, so after a crash they
 * exist together or not at all, and a restart still recognises every key.
 */
export class IdempotencyStore {
  readonly #db: DatabaseSync;

  constructor(path: string) {
    this.#db = new DatabaseSync(path);
    this.#db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = FULL;

      CREATE TABLE IF NOT EXISTS idempotency_records (
        idempotency_key TEXT    PRIMARY KEY,
        body_hash       TEXT    NOT NULL,
        response_status INTEGER NOT NULL,
        response_body   TEXT    NOT NULL
      );

      CREATE TABLE IF NOT EXISTS effects (
        id              TEXT PRIMARY KEY,
        idempotency_key TEXT NOT NULL UNIQUE REFERENCES idempotency_records (idempotency_key),
        body            TEXT NOT NULL,
        applied_at      TEXT NOT NULL
      );
    `);
  }

  /** Causes the effect once for a key. The same key and body replay; another body conflicts. */
  apply(idempotencyKey: string, body: string): ApplyResult {
    const bodyHash = createHash('sha256').update(body).digest('hex');
    const existing = this.#db
      .prepare(
        `SELECT body_hash AS bodyHash, response_status AS status, response_body AS responseBody
           FROM idempotency_records WHERE idempotency_key = ?`,
      )
      .get(idempotencyKey) as
      { bodyHash: string; status: number; responseBody: string } | undefined;

    if (existing) {
      return existing.bodyHash === bodyHash
        ? {
            outcome: 'replayed',
            response: { status: existing.status, body: existing.responseBody },
          }
        : { outcome: 'conflict' };
    }

    const appliedAt = new Date().toISOString();
    const id = randomUUID();
    const response = {
      status: 201,
      body: JSON.stringify({ notificationId: id, receivedAt: appliedAt }),
    };

    this.#db.exec('BEGIN');
    try {
      this.#db
        .prepare(
          `INSERT INTO idempotency_records
             (idempotency_key, body_hash, response_status, response_body)
           VALUES (?, ?, ?, ?)`,
        )
        .run(idempotencyKey, bodyHash, response.status, response.body);
      this.#db
        .prepare('INSERT INTO effects (id, idempotency_key, body, applied_at) VALUES (?, ?, ?, ?)')
        .run(id, idempotencyKey, body, appliedAt);
      this.#db.exec('COMMIT');
    } catch (error) {
      this.#db.exec('ROLLBACK');
      throw error;
    }

    return { outcome: 'applied', response };
  }

  listEffects(): Effect[] {
    return this.#db
      .prepare(
        `SELECT id, idempotency_key AS idempotencyKey, body, applied_at AS appliedAt
           FROM effects ORDER BY rowid`,
      )
      .all() as unknown as Effect[];
  }

  close(): void {
    this.#db.close();
  }
}
