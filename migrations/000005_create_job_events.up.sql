CREATE TABLE job_events (
    event_id UUID NOT NULL,
    job_id UUID NOT NULL,
    old_status job_status,
    new_status job_status NOT NULL,
    worker_id TEXT,
    reason TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
) PARTITION BY RANGE (occurred_at);
