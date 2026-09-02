package export

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/data-export-service/internal/apperror"
	"github.com/lihongjie0209/data-export-service/internal/config"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
)

type fakeTransaction struct{}

type applicationVerifier struct{ err error }

func (v applicationVerifier) Verify(context.Context, string, string) error { return v.err }

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
	if r.job.ID != "" && r.job.TenantID == value.TenantID && r.job.ApplicationID == value.ApplicationID {
		return r.job, false, nil
	}
	r.job = value
	r.created = true
	return value, true, nil
}
func (r *fakeRepository) Get(_ context.Context, tenant, application, id string) (Job, error) {
	if r.job.ID != id || r.job.TenantID != tenant || r.job.ApplicationID != application {
		return Job{}, ErrNotFound
	}
	return r.job, nil
}
func (*fakeRepository) List(context.Context, ListFilter) (Page, error) { return Page{}, nil }
func (r *fakeRepository) Cancel(_ context.Context, _ sqlx.ExtContext, tenant, application, id string, expected int64, actor string, now time.Time) error {
	if r.cancelErr != nil {
		return r.cancelErr
	}
	if r.job.ID != id || r.job.TenantID != tenant || r.job.ApplicationID != application || r.job.Version != expected {
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
func (r *fakeRepository) Claim(_ context.Context, tenant, application, id string, now time.Time) (Job, bool, error) {
	if r.job.TenantID != tenant || r.job.ApplicationID != application || r.job.ID != id || r.job.Status != StatusQueued {
		return Job{}, false, nil
	}
	r.job.Status = StatusRunning
	r.job.Version++
	r.job.StartedAt = &now
	return r.job, true, nil
}
func (r *fakeRepository) EnsureRunning(_ context.Context, tenantID, applicationID, id string) error {
	if r.job.TenantID != tenantID || r.job.ApplicationID != applicationID || r.job.ID != id || r.job.Status != StatusRunning {
		return ErrStaleVersion
	}
	return nil
}
func (r *fakeRepository) Progress(_ context.Context, tenantID, applicationID, id string, rows, bytes int64, percent int32, _ time.Time) error {
	if r.job.TenantID != tenantID || r.job.ApplicationID != applicationID || r.job.ID != id || r.job.Status != StatusRunning {
		return ErrStaleVersion
	}
	r.progress = append(r.progress, Progress{Rows: rows, Bytes: bytes})
	r.job.Version++
	r.job.ProgressPercent = percent
	return nil
}
func (r *fakeRepository) Succeed(_ context.Context, _ sqlx.ExtContext, value Job) (Job, error) {
	if r.job.Status != StatusRunning {
		return Job{}, ErrStaleVersion
	}
	r.job = value
	r.job.Version++
	return r.job, nil
}
func (r *fakeRepository) Fail(_ context.Context, _ sqlx.ExtContext, value Job) (Job, error) {
	if r.job.Status != StatusRunning {
		return Job{}, ErrStaleVersion
	}
	r.job = value
	r.job.Version++
	return r.job, nil
}
func (r *fakeRepository) ListExpired(_ context.Context, now time.Time, limit int) ([]Job, error) {
	if limit > 0 && r.job.Status == StatusSucceeded && r.job.ExpiresAt != nil && !r.job.ExpiresAt.After(now) {
		return []Job{r.job}, nil
	}
	return nil, nil
}
func (r *fakeRepository) Expire(_ context.Context, _ sqlx.ExtContext, value Job, _ time.Time) (Job, error) {
	if r.job.Status != StatusSucceeded {
		return Job{}, ErrStaleVersion
	}
	r.job = value
	r.job.Version++
	return r.job, nil
}
func (r *fakeRepository) ListExpiredMetadataBefore(_ context.Context, before time.Time, limit int) ([]Job, error) {
	if limit > 0 && r.job.Status == StatusExpired && r.job.UpdatedAt.Before(before) {
		return []Job{r.job}, nil
	}
	return nil, nil
}
func (r *fakeRepository) DeleteExpiredMetadata(_ context.Context, _ sqlx.ExtContext, value Job, before time.Time) (bool, error) {
	if r.job.ID != value.ID || r.job.Version != value.Version || r.job.Status != StatusExpired || !r.job.UpdatedAt.Before(before) {
		return false, nil
	}
	r.job = Job{}
	return true, nil
}
func (r *fakeRepository) AddOutbox(_ context.Context, _ sqlx.ExtContext, event OutboxEvent) error {
	r.events = append(r.events, event)
	return nil
}

func TestServiceCreateIsIdempotentAndWritesRequestedEvent(t *testing.T) {
	repository := &fakeRepository{}
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	service := &Service{repository: repository, transactor: fakeTransaction{}, now: func() time.Time { return now }, resultTTL: 7 * 24 * time.Hour, applications: allowAllApplications{}}
	ctx := userContext("tenant-1")
	input := CreateInput{TenantID: "tenant-1", ApplicationID: "application-1", DatasetCode: "billing.invoices", ProviderService: "billing-service", Format: "csv", Filename: "账单", QueryJSON: `{"status":"paid"}`, SelectedColumnsJSON: `["id"]`, IdempotencyKey: "request-1"}
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
	service := &Service{repository: repository, transactor: fakeTransaction{}, now: time.Now, applications: allowAllApplications{}}
	ctx := userContext("tenant-1")
	input := CreateInput{TenantID: "tenant-1", ApplicationID: "application-1", DatasetCode: "users", ProviderService: "identity-service", Format: "csv", IdempotencyKey: "same"}
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

func TestServiceIdempotencyIsIsolatedByApplication(t *testing.T) {
	repository := &fakeRepository{}
	service := &Service{repository: repository, transactor: fakeTransaction{}, now: time.Now, applications: allowAllApplications{}}
	input := CreateInput{TenantID: "tenant-1", ApplicationID: "application-1", DatasetCode: "users", ProviderService: "identity-service", Format: "csv", IdempotencyKey: "same"}
	first, duplicate, err := service.Create(userContext("tenant-1"), input)
	if err != nil || duplicate {
		t.Fatalf("first Create() = (%+v, %v, %v)", first, duplicate, err)
	}
	input.ApplicationID = "application-2"
	second, duplicate, err := service.Create(userContext("tenant-1"), input)
	if err != nil || duplicate || first.ID == second.ID {
		t.Fatalf("second Create() = (%+v, %v, %v), first=%+v", second, duplicate, err, first)
	}
}
func TestServiceEnforcesTenantAndOptimisticCancel(t *testing.T) {
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", ApplicationID: "application-1", Status: StatusQueued, Version: 2}}
	service := &Service{repository: repository, transactor: fakeTransaction{}, now: time.Now, applications: allowAllApplications{}}
	if _, err := service.Get(userContext("tenant-2"), "tenant-1", "application-1", "job-1"); err == nil {
		t.Fatal("cross tenant access accepted")
	}
	_, err := service.Cancel(userContext("tenant-1"), "tenant-1", "application-1", "job-1", 1)
	if err == nil {
		t.Fatal("stale cancel accepted")
	}
	job, err := service.Cancel(userContext("tenant-1"), "tenant-1", "application-1", "job-1", 2)
	if err != nil || job.Status != StatusCanceled {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}
func TestServiceValidatesJSONAndFilename(t *testing.T) {
	service := &Service{repository: &fakeRepository{}, transactor: fakeTransaction{}, now: time.Now, applications: allowAllApplications{}}
	ctx := userContext("tenant-1")
	_, _, err := service.Create(ctx, CreateInput{TenantID: "tenant-1", ApplicationID: "application-1", DatasetCode: "users", ProviderService: "identity-service", Format: "csv", Filename: "../../x", QueryJSON: "[]", IdempotencyKey: "key"})
	if err == nil {
		t.Fatal("invalid query accepted")
	}
	if got := safeFilename("../../secret", "users", "csv"); got != "secret.csv" {
		t.Fatalf("filename=%q", got)
	}
}

func TestServiceValidatesExportAgainstProviderDescriptor(t *testing.T) {
	t.Parallel()
	descriptor := DatasetDescriptor{
		Code: "billing.invoices", Formats: []string{"csv"}, Columns: []Column{{Key: "id"}, {Key: "number"}},
		QueryFields: []QueryField{{Key: "status", Type: "string", Options: []string{"draft", "paid"}}, {Key: "created_from", Type: "datetime"}},
	}
	for _, test := range []struct {
		name     string
		format   string
		query    string
		columns  string
		provider *catalogProviderStub
		code     int
	}{
		{name: "unsupported format", format: "xlsx", query: `{}`, columns: `["id"]`, provider: &catalogProviderStub{descriptor: descriptor}, code: apperror.CodeInvalidArgument},
		{name: "unknown column", format: "csv", query: `{}`, columns: `["secret"]`, provider: &catalogProviderStub{descriptor: descriptor}, code: apperror.CodeInvalidArgument},
		{name: "duplicate column", format: "csv", query: `{}`, columns: `["id","id"]`, provider: &catalogProviderStub{descriptor: descriptor}, code: apperror.CodeInvalidArgument},
		{name: "unknown query field", format: "csv", query: `{"secret":"x"}`, columns: `["id"]`, provider: &catalogProviderStub{descriptor: descriptor}, code: apperror.CodeInvalidArgument},
		{name: "invalid query option", format: "csv", query: `{"status":"unknown"}`, columns: `["id"]`, provider: &catalogProviderStub{descriptor: descriptor}, code: apperror.CodeInvalidArgument},
		{name: "invalid query type", format: "csv", query: `{"status":1}`, columns: `["id"]`, provider: &catalogProviderStub{descriptor: descriptor}, code: apperror.CodeInvalidArgument},
		{name: "invalid query datetime", format: "csv", query: `{"created_from":"yesterday"}`, columns: `["id"]`, provider: &catalogProviderStub{descriptor: descriptor}, code: apperror.CodeInvalidArgument},
		{name: "provider unavailable", format: "csv", query: `{}`, columns: `["id"]`, provider: &catalogProviderStub{err: errors.New("offline")}, code: apperror.CodeDependencyUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			service := &Service{applications: allowAllApplications{}, catalog: test.provider, now: time.Now}
			_, _, err := service.Create(userContext("tenant-1"), CreateInput{TenantID: "tenant-1", ApplicationID: "application-1", DatasetCode: "billing.invoices", ProviderService: "billing-service", Format: test.format, QueryJSON: test.query, SelectedColumnsJSON: test.columns, IdempotencyKey: "key"})
			var appErr *apperror.Error
			if !errors.As(err, &appErr) || appErr.Code != test.code {
				t.Fatalf("error = %#v", err)
			}
		})
	}
}

func TestValidateQueryUsesProviderSchema(t *testing.T) {
	t.Parallel()
	fields := []QueryField{
		{Key: "status", Type: "string", Options: []string{"paid"}, Required: true},
		{Key: "created_from", Type: "datetime"},
		{Key: "include_void", Type: "boolean"},
		{Key: "minimum_total", Type: "integer"},
	}
	if err := validateQuery(`{"status":"paid","created_from":"2026-09-02T00:00:00+08:00","include_void":false,"minimum_total":100}`, fields); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}
	if err := validateQuery(`{"include_void":false}`, fields); err == nil {
		t.Fatal("missing required field accepted")
	}
}

func TestServiceRejectsMissingApplicationGrant(t *testing.T) {
	service := NewService(&fakeRepository{}, nil, nil, config.Config{})
	service.applications = applicationVerifier{err: appaccess.ErrNotGranted}
	_, _, err := service.Create(userContext("tenant-1"), CreateInput{TenantID: "tenant-1", ApplicationID: "application-1"})
	var appErr *apperror.Error
	if !errors.As(err, &appErr) || appErr.Code != apperror.CodeForbidden {
		t.Fatalf("Create() error = %v", err)
	}
}
func userContext(tenant string) context.Context {
	return platformprincipal.WithContext(context.Background(), platformprincipal.Principal{ID: "user-1", Type: platformprincipal.TypeUser, TenantID: tenant})
}
