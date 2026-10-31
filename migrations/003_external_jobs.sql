-- One notification owed to the external API for each accepted order. It is created
-- with the consumer's Inbox row, and holds what the worker needs to retry after a restart.
CREATE TABLE external_jobs (
  event_id        uuid        PRIMARY KEY,
  target_key      text        NOT NULL UNIQUE,
  payload         json        NOT NULL,
  status          text        NOT NULL DEFAULT 'PENDING'
                              CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
  attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz DEFAULT now(),
  last_error      text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  completed_at    timestamptz,
  -- Only a pending job has a next attempt, and only a finished one has a completion time.
  CONSTRAINT external_jobs_pending_iff_next_attempt
    CHECK ((status = 'PENDING') = (next_attempt_at IS NOT NULL)),
  CONSTRAINT external_jobs_finished_iff_completed
    CHECK ((status = 'PENDING') = (completed_at IS NULL))
);

-- Finds due jobs without scanning the finished ones.
CREATE INDEX external_jobs_due_idx ON external_jobs (next_attempt_at) WHERE status = 'PENDING';

-- Inspecting failures should not scan every job ever made.
CREATE INDEX external_jobs_failed_idx ON external_jobs (completed_at) WHERE status = 'FAILED';

-- A message the external consumer could not read as an event, recorded before its offset
-- moves past it. Positions are not unique: Kafka's offsets restart if its data is lost, and
-- repeating a record after a redelivery is better than losing one. The value is bytes
-- because a malformed message can contain what a text column refuses, such as NUL.
CREATE TABLE external_quarantine (
  id              bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  topic           text        NOT NULL,
  kafka_partition integer     NOT NULL,
  kafka_offset    bigint      NOT NULL,
  raw_value       bytea       NOT NULL,
  error           text        NOT NULL,
  quarantined_at  timestamptz NOT NULL DEFAULT now()
);
