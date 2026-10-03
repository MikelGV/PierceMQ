CREATE TABLE IF NOT EXISTS retry_idempotency (idempotency_key TEXT PRIMARY KEY, job_id UUID NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
