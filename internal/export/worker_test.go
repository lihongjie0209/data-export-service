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
	if len(repository.events) != 1 || repository.events[0].Subject != "platform.export.job.succeeded.v1" {
		t.Fatalf("events=%+v", repository.events)
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
	if len(repository.events) != 1 || repository.events[0].Subject != "platform.export.job.failed.v1" {
		t.Fatalf("events=%+v", repository.events)
	}
}

func TestWorkerCleansExpiredResultAndEmitsEvent(t *testing.T) {
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.FixedZone("UTC+8", 8*3600))
	expires := now.Add(-time.Minute)
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", ObjectKey: "tenant-1/job-1.csv", Status: StatusSucceeded, ExpiresAt: &expires, Version: 3}}
	storage := &memoryStorage{}
	worker := NewWorker(repository, fakeTransaction{}, nil, storage, time.Minute, time.Hour)
	worker.now = func() time.Time { return now }
	count, err := worker.CleanupExpired(context.Background(), 10)
	if err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if repository.job.Status != StatusExpired || len(repository.events) != 1 || repository.events[0].Subject != "platform.export.job.expired.v1" {
		t.Fatalf("job=%+v events=%+v", repository.job, repository.events)
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

func TestWorkerStopsAndKeepsCanceledStateWhenProgressUpdateLosesRunningState(t *testing.T) {
	repository := &fakeRepository{job: Job{ID: "job-1", TenantID: "tenant-1", DatasetCode: "users", ProviderService: "identity-service", Format: FormatJSONL, SelectedColumnsJSON: `["id"]`, ObjectKey: "partial", Status: StatusQueued, Version: 1}}
	storage := &memoryStorage{}
	provider := providerFunc(func(_ context.Context, _ string, _ StreamRequest, receive func(Batch) error) error {
		repository.job.Status = StatusCanceled
		return receive(Batch{Columns: []Column{{Key: "id", Title: "ID"}}, Rows: []map[string]any{{"id": "u1"}}, Done: true})
	})
	worker := NewWorker(repository, fakeTransaction{}, NewPipeline(provider, storage, 10, 100, 1024, 1), storage, time.Minute, time.Hour)
	err := worker.Process(context.Background(), "tenant-1", "job-1")
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("error=%v", err)
	}
	if repository.job.Status != StatusCanceled {
		t.Fatalf("status=%s", repository.job.Status)
	}
	if len(repository.events) != 0 {
		t.Fatalf("unexpected events=%+v", repository.events)
	}
	storage.mu.Lock()
	deleted := storage.deleted
	storage.mu.Unlock()
	if !deleted {
		t.Fatal("partial object was not deleted after cancellation")
	}
}
