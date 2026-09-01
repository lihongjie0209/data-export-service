package export

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/lihongjie0209/data-export-service/internal/objectstorage"
	"golang.org/x/sync/errgroup"
)

var (
	ErrRowLimitExceeded  = errors.New("export row limit exceeded")
	ErrByteLimitExceeded = errors.New("export byte limit exceeded")
	ErrNoColumns         = errors.New("export provider returned no columns")
)

type StreamRequest struct {
	TenantID, ApplicationID, DatasetCode, QueryJSON, Cursor, SnapshotToken string
	SelectedColumns                                                        []string
	BatchSize                                                              int
}

type Provider interface {
	Stream(context.Context, string, StreamRequest, func(Batch) error) error
}

type Progress struct {
	Rows, Bytes   int64
	EstimatedRows int64
	Checkpoint    bool
}

type PipelineResult struct {
	Rows, Bytes int64
	Checksum    string
	ContentType string
}

type Pipeline struct {
	provider      Provider
	storage       objectstorage.Storage
	batchSize     int
	maxRows       int64
	maxBytes      int64
	progressEvery int64
}

func NewPipeline(provider Provider, storage objectstorage.Storage, batchSize int, maxRows, maxBytes, progressEvery int64) *Pipeline {
	return &Pipeline{provider: provider, storage: storage, batchSize: batchSize, maxRows: maxRows, maxBytes: maxBytes, progressEvery: progressEvery}
}

func (p *Pipeline) Run(ctx context.Context, job Job, selected []string, onProgress func(Progress) error) (PipelineResult, error) {
	reader, writer := io.Pipe()
	hash := sha256.New()
	limited := &limitWriter{writer: io.MultiWriter(writer, hash), max: p.maxBytes}
	group, groupCtx := errgroup.WithContext(ctx)

	var stored objectstorage.StoredObject
	group.Go(func() error {
		var err error
		stored, err = p.storage.Put(groupCtx, job.ObjectKey, reader, contentTypeFor(job.Format))
		if err != nil {
			_ = reader.CloseWithError(err)
		}
		return err
	})

	var rows int64
	group.Go(func() (err error) {
		defer func() { _ = writer.CloseWithError(err) }()
		formatter, _, err := NewFormatter(job.Format, limited)
		if err != nil {
			return err
		}
		var columns []Column
		wroteHeader := false
		lastReported := int64(0)
		err = p.provider.Stream(groupCtx, job.ProviderService, StreamRequest{TenantID: job.TenantID, ApplicationID: job.ApplicationID, DatasetCode: job.DatasetCode, QueryJSON: job.QueryJSON, SelectedColumns: selected, BatchSize: p.batchSize}, func(batch Batch) error {
			if !wroteHeader {
				columns = batch.Columns
				if len(columns) == 0 {
					return ErrNoColumns
				}
				if err := formatter.WriteHeader(columns); err != nil {
					return err
				}
				wroteHeader = true
			}
			if rows+int64(len(batch.Rows)) > p.maxRows {
				return ErrRowLimitExceeded
			}
			if err := formatter.WriteRows(columns, batch.Rows); err != nil {
				return err
			}
			rows += int64(len(batch.Rows))
			if onProgress != nil {
				checkpoint := rows-lastReported >= p.progressEvery || batch.Done
				if err := onProgress(Progress{Rows: rows, Bytes: limited.written, EstimatedRows: batch.EstimatedTotalRows, Checkpoint: checkpoint}); err != nil {
					return err
				}
				if checkpoint {
					lastReported = rows
				}
			}
			return nil
		})
		if err != nil {
			_ = formatter.Close()
			return err
		}
		if !wroteHeader {
			_ = formatter.Close()
			return ErrNoColumns
		}
		return formatter.Close()
	})

	if err := group.Wait(); err != nil {
		return PipelineResult{}, err
	}
	if stored.Size != limited.written {
		return PipelineResult{}, fmt.Errorf("stored size %d differs from streamed size %d", stored.Size, limited.written)
	}
	return PipelineResult{Rows: rows, Bytes: limited.written, Checksum: hex.EncodeToString(hash.Sum(nil)), ContentType: contentTypeFor(job.Format)}, nil
}

func contentTypeFor(format string) string {
	switch format {
	case FormatCSV:
		return "text/csv; charset=utf-8"
	case FormatJSONL:
		return "application/x-ndjson"
	case FormatXLSX:
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	default:
		return "application/octet-stream"
	}
}

type limitWriter struct {
	writer  io.Writer
	max     int64
	written int64
}

func (w *limitWriter) Write(value []byte) (int, error) {
	if int64(len(value)) > w.max-w.written {
		return 0, ErrByteLimitExceeded
	}
	n, err := w.writer.Write(value)
	w.written += int64(n)
	return n, err
}
