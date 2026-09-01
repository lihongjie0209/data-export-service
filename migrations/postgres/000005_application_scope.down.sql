DROP INDEX export_jobs_tenant_status_created_idx;
CREATE INDEX export_jobs_tenant_status_created_idx ON export_jobs (tenant_id, status, created_at DESC);
DROP INDEX export_jobs_tenant_created_idx;
CREATE INDEX export_jobs_tenant_created_idx ON export_jobs (tenant_id, created_at DESC, id DESC);
ALTER TABLE export_jobs DROP CONSTRAINT export_jobs_idempotency_unique;
ALTER TABLE export_jobs ADD CONSTRAINT export_jobs_idempotency_unique UNIQUE (tenant_id, idempotency_key);
ALTER TABLE export_jobs DROP COLUMN application_id;
