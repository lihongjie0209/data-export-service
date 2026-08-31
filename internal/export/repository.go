package export

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

var (
	ErrNotFound     = errors.New("export job not found")
	ErrStaleVersion = errors.New("stale export job version")
	ErrConflict     = errors.New("export job conflict")
)

type Repository interface {
	Create(context.Context, sqlx.ExtContext, Job) (Job, bool, error)
	Get(context.Context, string, string) (Job, error)
	List(context.Context, ListFilter) (Page, error)
	Cancel(context.Context, sqlx.ExtContext, string, string, int64, string, time.Time) error
	Retry(context.Context, sqlx.ExtContext, Job, int64) error
	Claim(context.Context, string, string, time.Time) (Job, bool, error)
	Progress(context.Context, string, int64, int64, int32, time.Time) error
	Succeed(context.Context, sqlx.ExtContext, Job) error
	Fail(context.Context, sqlx.ExtContext, Job) error
	AddOutbox(context.Context, sqlx.ExtContext, OutboxEvent) error
}

type SQLRepository struct{ db *sqlx.DB }

func NewRepository(db *sqlx.DB) Repository { return &SQLRepository{db: db} }

const jobColumns = "id,tenant_id,dataset_code,provider_service,format,filename,query_json,selected_columns_json,idempotency_key,status,rows_exported,bytes_written,progress_percent,object_key,content_type,checksum,error_code,error_message,started_at,completed_at,expires_at,version,created_at,updated_at,created_by,updated_by"

func (r *SQLRepository) Create(ctx context.Context, e sqlx.ExtContext, value Job) (Job, bool, error) {
	var existing Job
	err := sqlx.GetContext(ctx, e, &existing, r.db.Rebind("SELECT "+jobColumns+" FROM export_jobs WHERE tenant_id=? AND idempotency_key=? FOR UPDATE"), value.TenantID, value.IdempotencyKey)
	if err == nil {
		return existing, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return Job{}, false, err
	}
	args := []any{value.ID, value.TenantID, value.DatasetCode, value.ProviderService, value.Format, value.Filename, value.QueryJSON, value.SelectedColumnsJSON, value.IdempotencyKey, value.Status, value.RowsExported, value.BytesWritten, value.ProgressPercent, value.ObjectKey, value.ContentType, value.Checksum, value.ErrorCode, value.ErrorMessage, value.StartedAt, value.CompletedAt, value.ExpiresAt, value.Version, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy}
	_, err = e.ExecContext(ctx, r.db.Rebind("INSERT INTO export_jobs ("+jobColumns+") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"), args...)
	return value, true, err
}
func (r *SQLRepository) Get(ctx context.Context, tenantID, id string) (Job, error) {
	var value Job
	err := r.db.GetContext(ctx, &value, r.db.Rebind("SELECT "+jobColumns+" FROM export_jobs WHERE tenant_id=? AND id=?"), tenantID, id)
	return value, notFound(err)
}
func (r *SQLRepository) List(ctx context.Context, filter ListFilter) (Page, error) {
	where, args := "tenant_id=?", []any{filter.TenantID}
	if filter.Status != "" {
		where += " AND status=?"
		args = append(args, filter.Status)
	}
	if filter.DatasetCode != "" {
		where += " AND dataset_code=?"
		args = append(args, filter.DatasetCode)
	}
	if filter.CreatedFrom != nil {
		where += " AND created_at>=?"
		args = append(args, *filter.CreatedFrom)
	}
	if filter.CreatedTo != nil {
		where += " AND created_at<?"
		args = append(args, *filter.CreatedTo)
	}
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind("SELECT COUNT(*) FROM export_jobs WHERE "+where), args...); err != nil {
		return Page{}, err
	}
	items := []Job{}
	limit, offset := filter.PageSize, (filter.Page-1)*filter.PageSize
	pageArgs := append(append([]any{}, args...), limit, offset)
	err := r.db.SelectContext(ctx, &items, r.db.Rebind("SELECT "+jobColumns+" FROM export_jobs WHERE "+where+" ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?"), pageArgs...)
	return Page{Items: items, Total: total}, err
}
func (r *SQLRepository) Cancel(ctx context.Context, e sqlx.ExtContext, tenantID, id string, expected int64, actor string, now time.Time) error {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE export_jobs SET status='canceled',completed_at=?,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND version=? AND status IN ('queued','running')"), now, now, actor, tenantID, id, expected)
	return optimistic(result, err)
}
func (r *SQLRepository) Retry(ctx context.Context, e sqlx.ExtContext, value Job, expected int64) error {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE export_jobs SET status='queued',idempotency_key=?,rows_exported=0,bytes_written=0,progress_percent=0,content_type='',checksum='',error_code='',error_message='',started_at=NULL,completed_at=NULL,expires_at=NULL,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND version=? AND status IN ('failed','canceled')"), value.IdempotencyKey, value.UpdatedAt, value.UpdatedBy, value.TenantID, value.ID, expected)
	return optimistic(result, err)
}
func (r *SQLRepository) Claim(ctx context.Context, tenantID, id string, now time.Time) (Job, bool, error) {
	result, err := r.db.ExecContext(ctx, r.db.Rebind("UPDATE export_jobs SET status='running',started_at=?,version=version+1,updated_at=?,updated_by='data-export-worker' WHERE tenant_id=? AND id=? AND status='queued'"), now, now, tenantID, id)
	if err != nil {
		return Job{}, false, err
	}
	rows, err := result.RowsAffected()
	if err != nil || rows == 0 {
		return Job{}, false, err
	}
	value, err := r.Get(ctx, tenantID, id)
	return value, true, err
}
func (r *SQLRepository) Progress(ctx context.Context, id string, rows, bytes int64, percent int32, now time.Time) error {
	_, err := r.db.ExecContext(ctx, r.db.Rebind("UPDATE export_jobs SET rows_exported=?,bytes_written=?,progress_percent=?,version=version+1,updated_at=?,updated_by='data-export-worker' WHERE id=? AND status='running'"), rows, bytes, percent, now, id)
	return err
}
func (r *SQLRepository) Succeed(ctx context.Context, e sqlx.ExtContext, value Job) error {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE export_jobs SET status='succeeded',rows_exported=?,bytes_written=?,progress_percent=100,content_type=?,checksum=?,completed_at=?,expires_at=?,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND status='running'"), value.RowsExported, value.BytesWritten, value.ContentType, value.Checksum, value.CompletedAt, value.ExpiresAt, value.UpdatedAt, value.UpdatedBy, value.TenantID, value.ID)
	return optimistic(result, err)
}
func (r *SQLRepository) Fail(ctx context.Context, e sqlx.ExtContext, value Job) error {
	result, err := e.ExecContext(ctx, r.db.Rebind("UPDATE export_jobs SET status='failed',error_code=?,error_message=?,completed_at=?,version=version+1,updated_at=?,updated_by=? WHERE tenant_id=? AND id=? AND status='running'"), value.ErrorCode, value.ErrorMessage, value.CompletedAt, value.UpdatedAt, value.UpdatedBy, value.TenantID, value.ID)
	return optimistic(result, err)
}
func (r *SQLRepository) AddOutbox(ctx context.Context, e sqlx.ExtContext, value OutboxEvent) error {
	_, err := e.ExecContext(ctx, r.db.Rebind("INSERT INTO export_outbox_events (id,subject,envelope,attempts,available_at,published_at,last_error,version,created_at,updated_at,created_by,updated_by) VALUES (?,?,?,0,?,NULL,'',1,?,?,?,?)"), value.ID, value.Subject, value.Envelope, value.AvailableAt, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy)
	return err
}
func optimistic(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err == nil && rows == 0 {
		return ErrStaleVersion
	}
	return err
}
func notFound(err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	return err
}
func normalize(value string) string { return strings.TrimSpace(value) }
