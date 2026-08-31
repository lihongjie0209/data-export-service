CREATE TABLE export_jobs (
    id text PRIMARY KEY, tenant_id text NOT NULL, dataset_code text NOT NULL, provider_service text NOT NULL,
    format text NOT NULL, filename text NOT NULL, query_json text NOT NULL DEFAULT '{}', selected_columns_json text NOT NULL DEFAULT '[]',
    idempotency_key text NOT NULL, status text NOT NULL, rows_exported bigint NOT NULL DEFAULT 0,
    bytes_written bigint NOT NULL DEFAULT 0, progress_percent integer NOT NULL DEFAULT 0, object_key text NOT NULL,
    content_type text NOT NULL DEFAULT '', checksum text NOT NULL DEFAULT '', error_code text NOT NULL DEFAULT '',
    error_message text NOT NULL DEFAULT '', started_at timestamptz, completed_at timestamptz, expires_at timestamptz,
    version bigint NOT NULL DEFAULT 1, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
    created_by text NOT NULL, updated_by text NOT NULL,
    CONSTRAINT export_jobs_status_check CHECK (status IN ('queued','running','succeeded','failed','canceled')),
    CONSTRAINT export_jobs_format_check CHECK (format IN ('csv','jsonl','xlsx')),
    CONSTRAINT export_jobs_progress_check CHECK (progress_percent BETWEEN 0 AND 100),
    CONSTRAINT export_jobs_idempotency_unique UNIQUE (tenant_id, idempotency_key)
);
CREATE INDEX export_jobs_tenant_created_idx ON export_jobs (tenant_id, created_at DESC, id DESC);
CREATE INDEX export_jobs_tenant_status_created_idx ON export_jobs (tenant_id, status, created_at DESC);
CREATE INDEX export_jobs_queued_idx ON export_jobs (status, created_at, id);
CREATE INDEX export_jobs_expiry_idx ON export_jobs (status, expires_at);
CREATE TABLE export_outbox_events (
    id text PRIMARY KEY, subject text NOT NULL, envelope bytea NOT NULL, attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL, published_at timestamptz, last_error text NOT NULL DEFAULT '', version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, created_by text NOT NULL, updated_by text NOT NULL
);
CREATE INDEX export_outbox_pending_idx ON export_outbox_events (published_at, available_at, created_at);
