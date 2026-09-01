package export

import "time"

const (
	StatusQueued    = "queued"
	StatusRunning   = "running"
	StatusSucceeded = "succeeded"
	StatusFailed    = "failed"
	StatusCanceled  = "canceled"
	StatusExpired   = "expired"

	FormatCSV   = "csv"
	FormatJSONL = "jsonl"
	FormatXLSX  = "xlsx"
)

type Job struct {
	ID                  string     `db:"id" json:"id"`
	TenantID            string     `db:"tenant_id" json:"tenant_id"`
	ApplicationID       string     `db:"application_id" json:"application_id"`
	DatasetCode         string     `db:"dataset_code" json:"dataset_code"`
	ProviderService     string     `db:"provider_service" json:"provider_service"`
	Format              string     `db:"format" json:"format"`
	Filename            string     `db:"filename" json:"filename"`
	QueryJSON           string     `db:"query_json" json:"-"`
	SelectedColumnsJSON string     `db:"selected_columns_json" json:"-"`
	IdempotencyKey      string     `db:"idempotency_key" json:"-"`
	Status              string     `db:"status" json:"status"`
	RowsExported        int64      `db:"rows_exported" json:"rows_exported"`
	BytesWritten        int64      `db:"bytes_written" json:"bytes_written"`
	ProgressPercent     int32      `db:"progress_percent" json:"progress_percent"`
	ObjectKey           string     `db:"object_key" json:"object_key"`
	ContentType         string     `db:"content_type" json:"content_type"`
	Checksum            string     `db:"checksum" json:"checksum"`
	ErrorCode           string     `db:"error_code" json:"error_code"`
	ErrorMessage        string     `db:"error_message" json:"error_message"`
	StartedAt           *time.Time `db:"started_at" json:"started_at,omitempty"`
	CompletedAt         *time.Time `db:"completed_at" json:"completed_at,omitempty"`
	ExpiresAt           *time.Time `db:"expires_at" json:"expires_at,omitempty"`
	Version             int64      `db:"version" json:"version"`
	CreatedAt           time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt           time.Time  `db:"updated_at" json:"updated_at"`
	CreatedBy           string     `db:"created_by" json:"created_by"`
	UpdatedBy           string     `db:"updated_by" json:"updated_by"`
}

type Column struct {
	Key, Title, Type, Format string
	Sensitive                bool
}

type Batch struct {
	Columns            []Column
	Rows               []map[string]any
	NextCursor         string
	SnapshotToken      string
	EstimatedTotalRows int64
	Done               bool
}

type CreateInput struct {
	TenantID, ApplicationID, DatasetCode, ProviderService, Format, Filename string
	QueryJSON, SelectedColumnsJSON, IdempotencyKey, ActorID                 string
}

type ListFilter struct {
	TenantID, ApplicationID, Status, DatasetCode string
	CreatedFrom, CreatedTo                       *time.Time
	Page, PageSize                               int32
}

type Page struct {
	Items []Job
	Total int64
}

type OutboxEvent struct {
	ID, Subject                       string
	Envelope                          []byte
	AvailableAt, CreatedAt, UpdatedAt time.Time
	CreatedBy, UpdatedBy              string
}
