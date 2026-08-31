CREATE TABLE export_jobs (
    id text NOT NULL, tenant_id text NOT NULL, dataset_code text NOT NULL, provider_service text NOT NULL,
    format text NOT NULL, filename text NOT NULL, query_json longtext NOT NULL, selected_columns_json longtext NOT NULL,
    idempotency_key text NOT NULL, status text NOT NULL, rows_exported bigint NOT NULL DEFAULT 0,
    bytes_written bigint NOT NULL DEFAULT 0, progress_percent int NOT NULL DEFAULT 0, object_key text NOT NULL,
    content_type text NOT NULL, checksum text NOT NULL, error_code text NOT NULL, error_message text NOT NULL,
    started_at timestamp(6) NULL, completed_at timestamp(6) NULL, expires_at timestamp(6) NULL,
    version bigint NOT NULL DEFAULT 1, created_at timestamp(6) NOT NULL, updated_at timestamp(6) NOT NULL,
    created_by text NOT NULL, updated_by text NOT NULL, PRIMARY KEY (id(191)),
    UNIQUE KEY export_jobs_idempotency_unique (tenant_id(128), idempotency_key(191)),
    KEY export_jobs_tenant_created_idx (tenant_id(128), created_at DESC, id(64)),
    KEY export_jobs_tenant_status_created_idx (tenant_id(128), status(32), created_at DESC),
    KEY export_jobs_queued_idx (status(32), created_at, id(64)), KEY export_jobs_expiry_idx (status(32), expires_at),
    CONSTRAINT export_jobs_status_check CHECK (status IN ('queued','running','succeeded','failed','canceled')),
    CONSTRAINT export_jobs_format_check CHECK (format IN ('csv','jsonl','xlsx')),
    CONSTRAINT export_jobs_progress_check CHECK (progress_percent BETWEEN 0 AND 100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE export_outbox_events (
    id text NOT NULL, subject text NOT NULL, envelope longblob NOT NULL, attempts int NOT NULL DEFAULT 0,
    available_at timestamp(6) NOT NULL, published_at timestamp(6) NULL, last_error text NOT NULL,
    version bigint NOT NULL DEFAULT 1, created_at timestamp(6) NOT NULL, updated_at timestamp(6) NOT NULL,
    created_by text NOT NULL, updated_by text NOT NULL, PRIMARY KEY (id(191)),
    KEY export_outbox_pending_idx (published_at, available_at, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
