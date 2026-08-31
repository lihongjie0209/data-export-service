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
		percent := int32(0)
		if progress.EstimatedRows > 0 {
			percent = int32(min(int64(99), progress.Rows*100/progress.EstimatedRows))
		}
		return w.repository.Progress(runCtx, job.ID, progress.Rows, progress.Bytes, percent, w.now())
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
		if err := w.repository.Succeed(ctx, tx, job); err != nil {
			_ = w.storage.Delete(context.WithoutCancel(ctx), job.ObjectKey)
			return err
		}
		return nil
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
	persistErr := w.transactor.Within(context.WithoutCancel(ctx), nil, func(tx *sqlx.Tx) error { return w.repository.Fail(context.WithoutCancel(ctx), tx, job) })
	if persistErr != nil {
		return errors.Join(cause, persistErr)
	}
	return cause
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
