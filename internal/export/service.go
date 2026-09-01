package export

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/data-export-service/internal/apperror"
	"github.com/lihongjie0209/data-export-service/internal/config"
	"github.com/lihongjie0209/data-export-service/internal/database"
	"github.com/lihongjie0209/data-export-service/internal/objectstorage"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	platformprincipal "github.com/lihongjie0209/microservice-platform-go/principal"
)

var codePattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{1,127}$`)

type transactionRunner interface {
	Within(context.Context, *sql.TxOptions, func(*sqlx.Tx) error) error
}
type Service struct {
	repository   Repository
	transactor   transactionRunner
	now          func() time.Time
	resultTTL    time.Duration
	storage      objectstorage.Storage
	presignTTL   time.Duration
	applications appaccess.Verifier
}

type allowAllApplications struct{}

func (allowAllApplications) Verify(context.Context, string, string) error { return nil }

func NewService(repository Repository, transactor *database.Transactor, storage objectstorage.Storage, cfg config.Config) *Service {
	return &Service{repository: repository, transactor: transactor, storage: storage, now: time.Now, resultTTL: cfg.Export.ResultTTL, presignTTL: cfg.ObjectStorage.PresignTTL, applications: allowAllApplications{}}
}

func NewRuntimeService(repository Repository, transactor *database.Transactor, storage objectstorage.Storage, cfg config.Config, applications appaccess.Verifier) (*Service, error) {
	if applications == nil {
		return nil, errors.New("application verifier is required")
	}
	service := NewService(repository, transactor, storage, cfg)
	service.applications = applications
	return service, nil
}

func (s *Service) Create(ctx context.Context, input CreateInput) (Job, bool, error) {
	actor, err := s.authorizeScope(ctx, input.TenantID, input.ApplicationID)
	if err != nil {
		return Job{}, false, err
	}
	input.TenantID, input.ApplicationID, input.DatasetCode, input.ProviderService = normalize(input.TenantID), normalize(input.ApplicationID), strings.ToLower(normalize(input.DatasetCode)), strings.ToLower(normalize(input.ProviderService))
	input.Format, input.IdempotencyKey = strings.ToLower(normalize(input.Format)), normalize(input.IdempotencyKey)
	if input.TenantID == "" || input.ApplicationID == "" || !codePattern.MatchString(input.DatasetCode) || !codePattern.MatchString(input.ProviderService) || !validFormat(input.Format) || input.IdempotencyKey == "" || len(input.IdempotencyKey) > 191 {
		return Job{}, false, apperror.Invalid("tenant_id, application_id, dataset_code, provider_service, format, and idempotency_key are required", nil)
	}
	if !validJSONObject(input.QueryJSON) || !validStringArray(input.SelectedColumnsJSON) {
		return Job{}, false, apperror.Invalid("query_json must be an object and selected_columns_json must be a string array", nil)
	}
	now, id := s.now(), uuid.NewString()
	filename := safeFilename(input.Filename, input.DatasetCode, input.Format)
	value := Job{ID: id, TenantID: input.TenantID, ApplicationID: input.ApplicationID, DatasetCode: input.DatasetCode, ProviderService: input.ProviderService, Format: input.Format, Filename: filename, QueryJSON: defaultJSON(input.QueryJSON, "{}"), SelectedColumnsJSON: defaultJSON(input.SelectedColumnsJSON, "[]"), IdempotencyKey: input.IdempotencyKey, Status: StatusQueued, ObjectKey: fmt.Sprintf("%s/%s/%s/%s", input.TenantID, input.ApplicationID, now.Format("2006/01/02"), id+"."+input.Format), Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: actor, UpdatedBy: actor}
	created := false
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		var e error
		value, created, e = s.repository.Create(ctx, tx, value)
		if e != nil {
			return e
		}
		if !created {
			if value.DatasetCode != input.DatasetCode || value.ProviderService != input.ProviderService || value.Format != input.Format || value.QueryJSON != defaultJSON(input.QueryJSON, "{}") || value.SelectedColumnsJSON != defaultJSON(input.SelectedColumnsJSON, "[]") {
				return ErrConflict
			}
			return nil
		}
		return s.addEvent(ctx, tx, value, "requested", actor, now)
	})
	return value, !created, translate(err)
}
func (s *Service) Get(ctx context.Context, tenantID, applicationID, id string) (Job, error) {
	if _, err := s.authorizeScope(ctx, tenantID, applicationID); err != nil {
		return Job{}, err
	}
	value, err := s.repository.Get(ctx, normalize(tenantID), normalize(applicationID), normalize(id))
	return value, translate(err)
}
func (s *Service) List(ctx context.Context, filter ListFilter) (Page, error) {
	if _, err := s.authorizeScope(ctx, filter.TenantID, filter.ApplicationID); err != nil {
		return Page{}, err
	}
	if filter.Page < 1 {
		filter.Page = 1
	}
	if filter.PageSize < 1 {
		filter.PageSize = 20
	}
	if filter.PageSize > 200 {
		filter.PageSize = 200
	}
	filter.TenantID = normalize(filter.TenantID)
	filter.ApplicationID = normalize(filter.ApplicationID)
	filter.Status = strings.ToLower(normalize(filter.Status))
	filter.DatasetCode = strings.ToLower(normalize(filter.DatasetCode))
	page, err := s.repository.List(ctx, filter)
	return page, translate(err)
}
func (s *Service) Cancel(ctx context.Context, tenantID, applicationID, id string, expected int64) (Job, error) {
	actor, err := s.authorizeScope(ctx, tenantID, applicationID)
	if err != nil {
		return Job{}, err
	}
	if expected < 1 {
		return Job{}, apperror.Invalid("positive version is required", nil)
	}
	now := s.now()
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		if e := s.repository.Cancel(ctx, tx, normalize(tenantID), normalize(applicationID), normalize(id), expected, actor, now); e != nil {
			return e
		}
		value := Job{ID: id, TenantID: tenantID, ApplicationID: applicationID, Status: StatusCanceled, Version: expected + 1, UpdatedAt: now, UpdatedBy: actor}
		return s.addEvent(ctx, tx, value, "canceled", actor, now)
	})
	if err != nil {
		return Job{}, translate(err)
	}
	return s.Get(ctx, tenantID, applicationID, id)
}
func (s *Service) Retry(ctx context.Context, tenantID, applicationID, id string, expected int64, key string) (Job, bool, error) {
	actor, err := s.authorizeScope(ctx, tenantID, applicationID)
	if err != nil {
		return Job{}, false, err
	}
	if expected < 1 || normalize(key) == "" {
		return Job{}, false, apperror.Invalid("positive version and idempotency_key are required", nil)
	}
	current, err := s.repository.Get(ctx, normalize(tenantID), normalize(applicationID), normalize(id))
	if err != nil {
		return Job{}, false, translate(err)
	}
	if current.Status != StatusFailed && current.Status != StatusCanceled {
		return Job{}, false, apperror.Conflict("only failed or canceled jobs can be retried", nil)
	}
	if current.IdempotencyKey == normalize(key) {
		return current, true, nil
	}
	now := s.now()
	current.IdempotencyKey, current.UpdatedAt, current.UpdatedBy = normalize(key), now, actor
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		if e := s.repository.Retry(ctx, tx, current, expected); e != nil {
			return e
		}
		current.Status = StatusQueued
		current.Version = expected + 1
		return s.addEvent(ctx, tx, current, "requested", actor, now)
	})
	if err != nil {
		return Job{}, false, translate(err)
	}
	value, err := s.Get(ctx, tenantID, applicationID, id)
	return value, false, err
}

func (s *Service) CreateDownloadURL(ctx context.Context, tenantID, applicationID, id string, ttl time.Duration) (*url.URL, time.Time, Job, error) {
	job, err := s.Get(ctx, tenantID, applicationID, id)
	if err != nil {
		return nil, time.Time{}, Job{}, err
	}
	now := s.now()
	if job.Status != StatusSucceeded || job.ExpiresAt == nil || !job.ExpiresAt.After(now) {
		return nil, time.Time{}, Job{}, apperror.Conflict("export result is not available", nil)
	}
	if ttl <= 0 || ttl > s.presignTTL {
		ttl = s.presignTTL
	}
	if remaining := job.ExpiresAt.Sub(now); ttl > remaining {
		ttl = remaining
	}
	value, err := s.storage.PresignDownload(ctx, job.ObjectKey, ttl)
	if err != nil {
		return nil, time.Time{}, Job{}, apperror.Unavailable("object storage unavailable", err)
	}
	return value, now.Add(ttl), job, nil
}
func (s *Service) addEvent(ctx context.Context, tx *sqlx.Tx, job Job, change, actor string, at time.Time) error {
	event, err := jobChangedEvent(job, change, actor, at)
	if err != nil {
		return err
	}
	return s.repository.AddOutbox(ctx, tx, event)
}
func authorize(ctx context.Context, tenantID string) (string, error) {
	p, ok := platformprincipal.FromContext(ctx)
	if !ok {
		return "", apperror.Unauthorized("authenticated actor is required")
	}
	if p.Type != platformprincipal.TypeServiceAccount && p.Type != platformprincipal.TypeSystem && (p.TenantID == "" || p.TenantID != normalize(tenantID)) {
		return "", apperror.New(apperror.CodeForbidden, "tenant access denied", 403, nil)
	}
	return p.ID, nil
}

func (s *Service) authorizeScope(ctx context.Context, tenantID, applicationID string) (string, error) {
	actor, err := authorize(ctx, tenantID)
	if err != nil {
		return "", err
	}
	tenantID, applicationID = normalize(tenantID), normalize(applicationID)
	if tenantID == "" || applicationID == "" {
		return "", apperror.Invalid("tenant_id and application_id are required", nil)
	}
	if err := s.applications.Verify(ctx, tenantID, applicationID); err != nil {
		if errors.Is(err, appaccess.ErrNotGranted) {
			return "", apperror.New(apperror.CodeForbidden, "application access denied", 403, err)
		}
		return "", apperror.Unavailable("application authorization unavailable", err)
	}
	return actor, nil
}
func validFormat(value string) bool {
	return value == FormatCSV || value == FormatJSONL || value == FormatXLSX
}
func validJSONObject(value string) bool {
	if normalize(value) == "" {
		return true
	}
	var v map[string]any
	return json.Unmarshal([]byte(value), &v) == nil
}
func validStringArray(value string) bool {
	if normalize(value) == "" {
		return true
	}
	var v []string
	return json.Unmarshal([]byte(value), &v) == nil
}
func defaultJSON(value, fallback string) string {
	if normalize(value) == "" {
		return fallback
	}
	return value
}
func safeFilename(value, dataset, format string) string {
	base := filepath.Base(strings.ReplaceAll(normalize(value), "\\", "/"))
	base = strings.Map(func(r rune) rune {
		if r < ' ' || strings.ContainsRune(`/:*?"<>|`, r) {
			return '_'
		}
		return r
	}, base)
	if base == "" || base == "." {
		base = dataset
	}
	ext := "." + format
	if !strings.HasSuffix(strings.ToLower(base), ext) {
		base += ext
	}
	if len(base) > 240 {
		base = base[:240-len(ext)] + ext
	}
	return base
}
func translate(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNotFound):
		return apperror.NotFound("export job not found")
	case errors.Is(err, ErrStaleVersion):
		return apperror.Conflict("export job version or state changed", err)
	case errors.Is(err, ErrConflict):
		return apperror.Conflict("idempotency key was reused with different export parameters", err)
	default:
		return apperror.Internal(err)
	}
}
