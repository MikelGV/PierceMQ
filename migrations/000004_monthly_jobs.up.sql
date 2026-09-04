-- 000004_monthly_jobs: bootstrap RANGE partitions for jobs(created_at).
-- DEFAULT catches anything outside the explicit monthlies so inserts
-- never fail with "no partition of relation found".
-- Coverage: Sep-Nov 2026. Before December 2026, add a new migration
-- with the next monthlies (or automate creation); rows for uncovered
-- months land in jobs_default until then.
-- Multi-statement file: requires x-multi-statement=true (see migrate.go).
CREATE TABLE jobs_default PARTITION OF jobs DEFAULT;
CREATE TABLE jobs_p2026_09 PARTITION OF jobs FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE jobs_p2026_10 PARTITION OF jobs FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE jobs_p2026_11 PARTITION OF jobs FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
