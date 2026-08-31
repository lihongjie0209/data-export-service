package export

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/data-export-service/internal/objectstorage"
)

type Worker struct {
	repository         Repository
	transactor         transactionRunner
	pipeline           *Pipeline
	storage            objectstorage.Storage
	timeout, resultTTL time.Duration
	now                func() time.Time
}

func NewWorker(repository Repository, transactor transactionRunner, pipeline *Pipeline, storage objectstorage.Storage, timeout, resultTTL time.Duration) *Worker {
	return &Worker{repository: repository, transactor: transactor, pipeline: pipeline, storage: storage, timeout: timeout, resultTTL: resultTTL, now: time.Now}
}
func (w *Worker) Process(ctx context.Context, tenantID, id string) error {
	now := w.now()
	job, claimed, err := w.repository.Claim(ctx, tenantID, id, now)
	if err != nil || !claimed {
		return err
	}
	selected := []string{}
	if err := jsonStrings(job.SelectedColumnsJSON, &selected); err != nil {
		return w.fail(ctx, job, "invalid_definition", err)
	}
	runCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	result, err := w.pipeline.Run(runCtx, job, selected, func(progress Progress) error {
		if !progress.Checkpoint {
			return w.repository.EnsureRunning(runCtx, job.TenantID, job.ID)
		}
		percent := int32(0)
		if progress.EstimatedRows > 0 {
			percent = int32(min(int64(99), progress.Rows*100/progress.EstimatedRows))
		}
		return w.repository.Progress(runCtx, job.TenantID, job.ID, progress.Rows, progress.Bytes, percent, w.now())
	})
	if err != nil {
		return w.fail(ctx, job, errorCode(err), err)
	}
	completed := w.now()
	job.Status = StatusSucceeded
	job.RowsExported = result.Rows
	job.BytesWritten = result.Bytes
	job.ContentType = result.ContentType
	job.Checksum = result.Checksum
	job.CompletedAt = &completed
	expires := completed.Add(w.resultTTL)
	job.ExpiresAt = &expires
	job.UpdatedAt = completed
	job.UpdatedBy = "data-export-worker"
	return w.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		persisted, err := w.repository.Succeed(ctx, tx, job)
		if err != nil {
			_ = w.storage.Delete(context.WithoutCancel(ctx), job.ObjectKey)
			return err
		}
		event, err := jobChangedEvent(persisted, "succeeded", persisted.UpdatedBy, completed)
		if err != nil {
			return err
		}
		return w.repository.AddOutbox(ctx, tx, event)
	})
}
func (w *Worker) fail(ctx context.Context, job Job, code string, cause error) error {
	_ = w.storage.Delete(context.WithoutCancel(ctx), job.ObjectKey)
	now := w.now()
	job.Status = StatusFailed
	job.ErrorCode = code
	job.ErrorMessage = truncate(cause.Error(), 2000)
	job.CompletedAt = &now
	job.UpdatedAt = now
	job.UpdatedBy = "data-export-worker"
	persistCtx := context.WithoutCancel(ctx)
	persistErr := w.transactor.Within(persistCtx, nil, func(tx *sqlx.Tx) error {
		persisted, err := w.repository.Fail(persistCtx, tx, job)
		if err != nil {
			return err
		}
		event, err := jobChangedEvent(persisted, "failed", persisted.UpdatedBy, now)
		if err != nil {
			return err
		}
		return w.repository.AddOutbox(persistCtx, tx, event)
	})
	if persistErr != nil {
		return errors.Join(cause, persistErr)
	}
	return cause
}

// CleanupExpired removes expired result objects before atomically making them unavailable.
// Object deletion is idempotent, so a database failure is safely retried on the next run.
func (w *Worker) CleanupExpired(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, errors.New("cleanup limit must be positive")
	}
	now := w.now()
	jobs, err := w.repository.ListExpired(ctx, now, limit)
	if err != nil {
		return 0, err
	}
	cleaned := 0
	for _, job := range jobs {
		if err := w.storage.Delete(ctx, job.ObjectKey); err != nil {
			return cleaned, err
		}
		job.Status = StatusExpired
		job.UpdatedAt = now
		job.UpdatedBy = "data-export-cleaner"
		err := w.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
			persisted, err := w.repository.Expire(ctx, tx, job, now)
			if err != nil {
				return err
			}
			event, err := jobChangedEvent(persisted, "expired", persisted.UpdatedBy, now)
			if err != nil {
				return err
			}
			return w.repository.AddOutbox(ctx, tx, event)
		})
		if errors.Is(err, ErrStaleVersion) {
			continue
		}
		if err != nil {
			return cleaned, err
		}
		cleaned++
	}
	return cleaned, nil
}

// PurgeExpiredMetadata removes old terminal job metadata after the result was
// already deleted and the expired event has remained queryable for retention.
func (w *Worker) PurgeExpiredMetadata(ctx context.Context, retention time.Duration, limit int) (int, error) {
	if retention <= 0 || limit <= 0 {
		return 0, errors.New("metadata retention and cleanup limit must be positive")
	}
	before := w.now().Add(-retention)
	jobs, err := w.repository.ListExpiredMetadataBefore(ctx, before, limit)
	if err != nil {
		return 0, err
	}
	purged := 0
	for _, job := range jobs {
		deleted := false
		err := w.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
			var deleteErr error
			deleted, deleteErr = w.repository.DeleteExpiredMetadata(ctx, tx, job, before)
			return deleteErr
		})
		if err != nil {
			return purged, err
		}
		if deleted {
			purged++
		}
	}
	return purged, nil
}
func errorCode(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, ErrRowLimitExceeded):
		return "row_limit_exceeded"
	case errors.Is(err, ErrByteLimitExceeded):
		return "byte_limit_exceeded"
	case errors.Is(err, ErrNoColumns):
		return "invalid_provider_response"
	default:
		return "export_failed"
	}
}
func jsonStrings(value string, target *[]string) error {
	decoder := json.NewDecoder(strings.NewReader(value))
	return decoder.Decode(target)
}
func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}
