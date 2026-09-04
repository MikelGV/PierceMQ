-- 000007_job_events_partitions: bootstrap RANGE partitions for
-- job_events(occurred_at). Without these (and without a DEFAULT), every
-- insert fails with "no partition of relation found".
-- Coverage: Sep-Nov 2026. Same rule as 000004: before December 2026, add
-- a new migration with the next monthlies; rows for uncovered months land
-- in job_events_default until then.
-- Multi-statement file: requires x-multi-statement=true (see migrate.go).
CREATE TABLE job_events_default PARTITION OF job_events DEFAULT;
CREATE TABLE job_events_p2026_09 PARTITION OF job_events FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE job_events_p2026_10 PARTITION OF job_events FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE job_events_p2026_11 PARTITION OF job_events FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
