ALTER TABLE export_jobs ADD COLUMN application_id text NOT NULL DEFAULT '';
UPDATE export_jobs
SET status = 'canceled', completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP), version = version + 1,
    updated_at = CURRENT_TIMESTAMP, updated_by = 'application-scope-migration'
WHERE status IN ('queued', 'running') AND application_id = '';
ALTER TABLE export_jobs DROP CONSTRAINT export_jobs_idempotency_unique;
ALTER TABLE export_jobs ADD CONSTRAINT export_jobs_idempotency_unique UNIQUE (tenant_id, application_id, idempotency_key);
DROP INDEX export_jobs_tenant_created_idx;
CREATE INDEX export_jobs_tenant_created_idx ON export_jobs (tenant_id, application_id, created_at DESC, id DESC);
DROP INDEX export_jobs_tenant_status_created_idx;
CREATE INDEX export_jobs_tenant_status_created_idx ON export_jobs (tenant_id, application_id, status, created_at DESC);
