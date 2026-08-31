package export

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/lihongjie0209/data-export-service/internal/objectstorage"
)

func TestPipelineStreamsBatchesAndReportsProgress(t *testing.T) {
	storage := &memoryStorage{}
	provider := providerFunc(func(_ context.Context, _ string, _ StreamRequest, receive func(Batch) error) error {
		columns := []Column{{Key: "id", Title: "ID"}}
		if err := receive(Batch{Columns: columns, Rows: []map[string]any{{"id": 1}}, EstimatedTotalRows: 2}); err != nil {
			return err
		}
		return receive(Batch{Rows: []map[string]any{{"id": 2}}, EstimatedTotalRows: 2, Done: true})
	})
	pipeline := NewPipeline(provider, storage, 100, 10, 1024, 1)
	var progress []Progress
	result, err := pipeline.Run(context.Background(), Job{Format: FormatCSV, ObjectKey: "tenant/job.csv"}, nil, func(value Progress) error { progress = append(progress, value); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 2 || result.Bytes == 0 || len(result.Checksum) != 64 {
		t.Fatalf("result = %#v", result)
	}
	if got := storage.String(); got != "ID\n1\n2\n" {
		t.Fatalf("object = %q", got)
	}
	if len(progress) != 2 || progress[1].Rows != 2 {
		t.Fatalf("progress = %#v", progress)
	}
}

func TestPipelineCancelsProviderWhenUploadFails(t *testing.T) {
	uploadErr := errors.New("s3 unavailable")
	storage := &memoryStorage{putErr: uploadErr}
	canceled := make(chan struct{})
	provider := providerFunc(func(ctx context.Context, _ string, _ StreamRequest, receive func(Batch) error) error {
		<-ctx.Done()
		close(canceled)
		return ctx.Err()
	})
	_, err := NewPipeline(provider, storage, 1, 10, 100, 1).Run(context.Background(), Job{Format: FormatCSV}, nil, nil)
	if !errors.Is(err, uploadErr) {
		t.Fatalf("error = %v", err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("provider was not canceled")
	}
}

func TestPipelineEnforcesRowAndByteLimits(t *testing.T) {
	provider := providerFunc(func(_ context.Context, _ string, _ StreamRequest, receive func(Batch) error) error {
		return receive(Batch{Columns: []Column{{Key: "value", Title: "Value"}}, Rows: []map[string]any{{"value": "one"}, {"value": "two"}}, Done: true})
	})
	_, err := NewPipeline(provider, &memoryStorage{}, 10, 1, 1024, 1).Run(context.Background(), Job{Format: FormatCSV}, nil, nil)
	if !errors.Is(err, ErrRowLimitExceeded) {
		t.Fatalf("row error = %v", err)
	}
	_, err = NewPipeline(provider, &memoryStorage{}, 10, 10, 3, 1).Run(context.Background(), Job{Format: FormatCSV}, nil, nil)
	if !errors.Is(err, ErrByteLimitExceeded) {
		t.Fatalf("byte error = %v", err)
	}
}

type providerFunc func(context.Context, string, StreamRequest, func(Batch) error) error

func (f providerFunc) Stream(ctx context.Context, service string, request StreamRequest, receive func(Batch) error) error {
	return f(ctx, service, request, receive)
}

type memoryStorage struct {
	mu      sync.Mutex
	data    bytes.Buffer
	putErr  error
	deleted bool
}

func (s *memoryStorage) Put(ctx context.Context, _ string, source io.Reader, _ string) (objectstorage.StoredObject, error) {
	if s.putErr != nil {
		return objectstorage.StoredObject{}, s.putErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := io.Copy(&s.data, source)
	return objectstorage.StoredObject{Size: n}, err
}
func (s *memoryStorage) Delete(context.Context, string) error {
	s.mu.Lock()
	s.deleted = true
	s.mu.Unlock()
	return nil
}
func (*memoryStorage) PresignDownload(context.Context, string, time.Duration) (*url.URL, error) {
	return &url.URL{}, nil
}
func (*memoryStorage) Bucket() string   { return "test" }
func (*memoryStorage) Enabled() bool    { return true }
func (s *memoryStorage) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.data.String() }
