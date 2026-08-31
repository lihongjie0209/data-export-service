package export

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
)

type fakeTransaction struct{}

func (fakeTransaction) Within(_ context.Context, _ *sql.TxOptions, fn func(*sqlx.Tx) error) error {
	return fn(nil)
}

type fakeRepository struct {
	job       Job
	created   bool
	events    []OutboxEvent
	cancelErr error
	progress  []Progress
}

func (r *fakeRepository) Create(_ context.Context, _ sqlx.ExtContext, value Job) (Job, bool, error) {
	if r.job.ID != "" {
		return r.job, false, nil
	}
	r.job = value
	r.created = true
	return value, true, nil
}
func (r *fakeRepository) Get(_ context.Context, tenant, id string) (Job, error) {
	if r.job.ID != id || r.job.TenantID != tenant {
		return Job{}, ErrNotFound
	}
	return r.job, nil
}
func (*fakeRepository) List(context.Context, ListFilter) (Page, error) { return Page{}, nil }
func (r *fakeRepository) Cancel(_ context.Context, _ sqlx.ExtContext, tenant, id string, expected int64, actor string, now time.Time) error {
	if r.cancelErr != nil {
		return r.cancelErr
	}
	if r.job.ID != id || r.job.TenantID != tenant || r.job.Version != expected {
		return ErrStaleVersion
	}
	r.job.Status = StatusCanceled
	r.job.Version++
	r.job.UpdatedBy = actor
	r.job.UpdatedAt = now
	return nil
}
func (r *fakeRepository) Retry(_ context.Context, _ sqlx.ExtContext, value Job, expected int64) error {
	if r.job.Version != expected {
		return ErrStaleVersion
	}
	r.job.Status = StatusQueued
	r.job.IdempotencyKey = value.IdempotencyKey
	r.job.Version++
	r.job.UpdatedAt = value.UpdatedAt
	r.job.UpdatedBy = value.UpdatedBy
	return nil
}
func (r *fakeRepository) Claim(_ context.Context, tenant, id string, now time.Time) (Job, bool, error) {
	if r.job.TenantID != tenant || r.job.ID != id || r.job.Status != StatusQueued {
		return Job{}, false, nil
	}
	r.job.Status = StatusRunning
	r.job.Version++
	r.job.StartedAt = &now
	return r.job, true, nil
}
func (r *fakeRepository) Progress(_ context.Context, _ string, rows, bytes int64, percent int32, _ time.Time) error {
	r.progress = append(r.progress, Progress{Rows: rows, Bytes: bytes})
	r.job.Version++
	r.job.ProgressPercent = percent
	return nil
}
func (r *fakeRepository) Succeed(_ context.Context, _ sqlx.ExtContext, value Job) error {
	r.job = value
	r.job.Version++
	return nil
}
func (r *fakeRepository) Fail(_ context.Context, _ sqlx.ExtContext, value Job) error {
	r.job = value
	r.job.Version++
	return nil
}
func (r *fakeRepository) AddOutbox(_ context.Context, _ sqlx.ExtContext, event OutboxEvent) error {
	r.events = append(r.events, event)
	return nil
}

func TestServiceCreateIsIdempotentAndWritesRequestedEvent(t *testing.T) {
	repository := &fakeRepository{}
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	service := &Service{repository: repository, transactor: fakeTransaction{}, now: func() time.Time { return now }, resultTTL: 7 * 24 * time.Hour}
	ctx := userContext("tenant-1")
	input := CreateInput{TenantID: "tenant-1", DatasetCode: "billing.invoices", ProviderService: "billing-service", Format: "csv", Filename: "账单", QueryJSON: `{"status":"paid"}`, SelectedColumnsJSON: `["id"]`, IdempotencyKey: "request-1"}
	job, duplicate, err := service.Create(ctx, input)
	if err != nil || duplicate {
		t.Fatalf("create duplicate=%v err=%v", duplicate, err)
	}
	if job.Filename != "账单.csv" || job.Status != StatusQueued || job.Version != 1 {
		t.Fatalf("job=%+v", job)
	}
	if len(repository.events) != 1 || repository.events[0].Subject != "platform.export.job.requested.v1" {
		t.Fatalf("events=%+v", repository.events)
	}
	again, duplicate, err := service.Create(ctx, input)
	if err != nil || !duplicate || again.ID != job.ID {
		t.Fatalf("duplicate job=%+v duplicate=%v err=%v", again, duplicate, err)
	}
	if len(repository.events) != 1 {
		t.Fatalf("duplicate emitted event: %d", len(repository.events))
	}
}
func TestServiceRejectsIdempotencyKeyReuseWithDifferentPayload(t *testing.T) {
	repository := &fakeRepository{}
	service := &Service{repository: repository, transactor: fakeTransaction{}, now: time.Now}
	ctx := userContext("tenant-1")
	input := CreateInput{TenantID: "tenant-1", DatasetCode: "users", ProviderService: "identity-service", Format: "csv", IdempotencyKey: "same"}
	if _, _, err := service.Create(ctx, input); err != nil {
		t.Fatal(err)
	}
	input.Format = "jsonl"
	_, _, err := service.Create(ctx, input)
	var appErr interface{ Error() string }
	if !errors.As(err, &appErr) {
		t.Fatalf("error=%v", err)
	}
}
func TestServiceEnforcesTenantAndOptimisticCancel(t *testing.T) {
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", Status: StatusQueued, Version: 2}}
	service := &Service{repository: repository, transactor: fakeTransaction{}, now: time.Now}
	if _, err := service.Get(userContext("tenant-2"), "tenant-1", "job-1"); err == nil {
		t.Fatal("cross tenant access accepted")
	}
	_, err := service.Cancel(userContext("tenant-1"), "tenant-1", "job-1", 1)
	if err == nil {
		t.Fatal("stale cancel accepted")
	}
	job, err := service.Cancel(userContext("tenant-1"), "tenant-1", "job-1", 2)
	if err != nil || job.Status != StatusCanceled {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}
func TestServiceValidatesJSONAndFilename(t *testing.T) {
	service := &Service{repository: &fakeRepository{}, transactor: fakeTransaction{}, now: time.Now}
	ctx := userContext("tenant-1")
	_, _, err := service.Create(ctx, CreateInput{TenantID: "tenant-1", DatasetCode: "users", ProviderService: "identity-service", Format: "csv", Filename: "../../x", QueryJSON: "[]", IdempotencyKey: "key"})
	if err == nil {
		t.Fatal("invalid query accepted")
	}
	if got := safeFilename("../../secret", "users", "csv"); got != "secret.csv" {
		t.Fatalf("filename=%q", got)
	}
}
func userContext(tenant string) context.Context {
	return platformprincipal.WithContext(context.Background(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: tenant})
}
