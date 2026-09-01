ALTER TABLE export_jobs ADD COLUMN application_id varchar(64) NOT NULL DEFAULT '' AFTER tenant_id;
UPDATE export_jobs
SET status = 'canceled', completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP(6)), version = version + 1,
    updated_at = CURRENT_TIMESTAMP(6), updated_by = 'application-scope-migration'
WHERE status IN ('queued', 'running') AND application_id = '';
ALTER TABLE export_jobs
    DROP INDEX export_jobs_idempotency_unique,
    ADD UNIQUE KEY export_jobs_idempotency_unique (tenant_id(128), application_id, idempotency_key(191)),
    DROP INDEX export_jobs_tenant_created_idx,
    ADD KEY export_jobs_tenant_created_idx (tenant_id(128), application_id, created_at DESC, id(64)),
    DROP INDEX export_jobs_tenant_status_created_idx,
    ADD KEY export_jobs_tenant_status_created_idx (tenant_id(128), application_id, status(32), created_at DESC);
