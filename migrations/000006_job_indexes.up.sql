CREATE INDEX IF NOT EXISTS jobs_scheduled_idx ON jobs (scheduled_at, status) WHERE status = 'scheduled';
CREATE INDEX IF NOT EXISTS jobs_pending_idx ON jobs (queue_name, status, created_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS jobs_running_idx ON jobs (worker_id, heartbeat_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS jobs_job_id_idx ON jobs (job_id);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_idx ON jobs (idempotency_key, created_at) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS job_events_job_id_idx ON job_events (job_id);
