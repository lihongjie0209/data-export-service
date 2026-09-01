ALTER TABLE export_jobs
    DROP INDEX export_jobs_tenant_status_created_idx,
    ADD KEY export_jobs_tenant_status_created_idx (tenant_id(128), status(32), created_at DESC),
    DROP INDEX export_jobs_tenant_created_idx,
    ADD KEY export_jobs_tenant_created_idx (tenant_id(128), created_at DESC, id(64)),
    DROP INDEX export_jobs_idempotency_unique,
    ADD UNIQUE KEY export_jobs_idempotency_unique (tenant_id(128), idempotency_key(191)),
    DROP COLUMN application_id;
