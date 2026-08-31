package export

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWorkerCompletesClaimedJob(t *testing.T) {
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", DatasetCode: "users", ProviderService: "identity-service", Format: FormatJSONL, SelectedColumnsJSON: `["id"]`, ObjectKey: "tenant-1/job-1.jsonl", Status: StatusQueued, Version: 1}}
	storage := &memoryStorage{}
	provider := providerFunc(func(_ context.Context, _ string, request StreamRequest, receive func(Batch) error) error {
		if request.BatchSize != 100 {
			t.Fatalf("batch size=%d", request.BatchSize)
		}
		return receive(Batch{Columns: []Column{{Key: "id", Title: "ID"}}, Rows: []map[string]any{{"id": "u1"}}, EstimatedTotalRows: 1, Done: true})
	})
	pipeline := NewPipeline(provider, storage, 100, 1000, 1<<20, 1)
	worker := NewWorker(repository, fakeTransaction{}, pipeline, storage, time.Minute, time.Hour)
	fixed := time.Date(2026, 8, 31, 10, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	worker.now = func() time.Time { return fixed }
	if err := worker.Process(context.Background(), "tenant-1", "job-1"); err != nil {
		t.Fatal(err)
	}
	if repository.job.Status != StatusSucceeded || repository.job.RowsExported != 1 || repository.job.Checksum == "" || repository.job.ExpiresAt == nil {
		t.Fatalf("job=%+v", repository.job)
	}
}

func TestWorkerPersistsFailureAndDeletesPartialObject(t *testing.T) {
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", ProviderService: "source", Format: FormatCSV, SelectedColumnsJSON: `[]`, ObjectKey: "partial", Status: StatusQueued, Version: 1}}
	storage := &memoryStorage{}
	sourceErr := errors.New("provider unavailable")
	provider := providerFunc(func(context.Context, string, StreamRequest, func(Batch) error) error { return sourceErr })
	worker := NewWorker(repository, fakeTransaction{}, NewPipeline(provider, storage, 10, 100, 1024, 10), storage, time.Minute, time.Hour)
	err := worker.Process(context.Background(), "tenant-1", "job-1")
	if !errors.Is(err, sourceErr) {
		t.Fatalf("error=%v", err)
	}
	if repository.job.Status != StatusFailed || repository.job.ErrorCode != "export_failed" {
		t.Fatalf("job=%+v", repository.job)
	}
	storage.mu.Lock()
	deleted := storage.deleted
	storage.mu.Unlock()
	if !deleted {
		t.Fatal("partial object was not deleted")
	}
}

func TestWorkerDoesNotRunUnclaimedJob(t *testing.T) {
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", Status: StatusCanceled}}
	called := false
	provider := providerFunc(func(context.Context, string, StreamRequest, func(Batch) error) error { called = true; return nil })
	storage := &memoryStorage{}
	worker := NewWorker(repository, fakeTransaction{}, NewPipeline(provider, storage, 1, 1, 1, 1), storage, time.Second, time.Hour)
	if err := worker.Process(context.Background(), "tenant-1", "job-1"); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Fatal("provider called for unclaimed job")
	}
}
