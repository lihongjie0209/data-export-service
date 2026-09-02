package export

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/lihongjie0209/data-export-service/internal/config"
	exportv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/export/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeRowStream struct {
	responses []*exportv1.StreamRowsResponse
	err       error
}

func (s *fakeRowStream) Recv() (*exportv1.StreamRowsResponse, error) {
	if len(s.responses) == 0 {
		if s.err != nil {
			err := s.err
			s.err = nil
			return nil, err
		}
		return nil, io.EOF
	}
	response := s.responses[0]
	s.responses = s.responses[1:]
	return response, nil
}

func TestSupportsDatasetMetadataForms(t *testing.T) {
	for _, metadata := range []map[string]string{{"platform.export.provider": "true", "platform.export.datasets": `[{"code":"billing.invoices","title":"Invoices","formats":["csv"]}]`}} {
		if !supportsDataset(metadata, "billing.invoices") {
			t.Fatalf("not supported: %v", metadata)
		}
	}
	if supportsDataset(map[string]string{"platform.export.provider": "true", "platform.export.datasets": `[{"code":"users","title":"Users","formats":["csv"]}]`}, "billing.invoices") {
		t.Fatal("unregistered dataset accepted")
	}
}
func TestValidateProviderTargetUsesDNSAllowlist(t *testing.T) {
	if err := validateProviderTarget("billing-service.platform.svc.cluster.local:9090", []string{".platform.svc.cluster.local"}); err != nil {
		t.Fatal(err)
	}
	for _, target := range []string{"127.0.0.1:9090", "localhost:9090", "evil.example.com:9090", "missing-port"} {
		if err := validateProviderTarget(target, []string{".platform.svc.cluster.local"}); err == nil {
			t.Fatalf("target %q accepted", target)
		}
	}
}

func TestStreamRowsResumesFromAcceptedBatchWithoutEjectingProvider(t *testing.T) {
	t.Parallel()
	var opened int
	var cursors []string
	var received []string
	failed, err := streamRows(context.Background(), StreamRequest{DatasetCode: "billing.invoices"}, config.Retry{MaxAttempts: 2}, func(_ context.Context, request *exportv1.StreamRowsRequest) (rowStream, error) {
		opened++
		cursors = append(cursors, request.GetCursor())
		if opened == 1 {
			return &fakeRowStream{responses: []*exportv1.StreamRowsResponse{{NextCursor: "cursor-1", SnapshotToken: "snapshot-1"}}, err: status.Error(codes.Unavailable, "connection reset")}, nil
		}
		if request.GetSnapshotToken() != "snapshot-1" {
			t.Fatalf("snapshot token = %q", request.GetSnapshotToken())
		}
		return &fakeRowStream{responses: []*exportv1.StreamRowsResponse{{NextCursor: "cursor-2", SnapshotToken: "snapshot-1", Done: true}}}, nil
	}, func(batch Batch) error {
		received = append(received, batch.NextCursor)
		return nil
	})
	if err != nil || failed {
		t.Fatalf("streamRows() failed = %v, err = %v", failed, err)
	}
	if opened != 2 || len(received) != 2 || cursors[0] != "" || cursors[1] != "cursor-1" {
		t.Fatalf("opened = %d, cursors = %v, received = %v", opened, cursors, received)
	}
}

func TestStreamRowsReportsProviderFailureOnlyAfterRetryExhaustion(t *testing.T) {
	t.Parallel()
	var opened int
	failed, err := streamRows(context.Background(), StreamRequest{}, config.Retry{MaxAttempts: 3}, func(context.Context, *exportv1.StreamRowsRequest) (rowStream, error) {
		opened++
		return nil, status.Error(codes.Unavailable, "offline")
	}, func(Batch) error { return nil })
	if !failed || status.Code(err) != codes.Unavailable || opened != 3 {
		t.Fatalf("failed = %v, code = %v, opened = %d", failed, status.Code(err), opened)
	}
}

func TestStreamRowsTreatsEOFBeforeDoneAsRetryableTruncation(t *testing.T) {
	t.Parallel()
	var opened int
	failed, err := streamRows(context.Background(), StreamRequest{}, config.Retry{MaxAttempts: 2}, func(_ context.Context, request *exportv1.StreamRowsRequest) (rowStream, error) {
		opened++
		if opened == 1 {
			return &fakeRowStream{responses: []*exportv1.StreamRowsResponse{{NextCursor: "cursor-1"}}}, nil
		}
		if request.GetCursor() != "cursor-1" {
			t.Fatalf("resume cursor = %q", request.GetCursor())
		}
		return &fakeRowStream{responses: []*exportv1.StreamRowsResponse{{Done: true}}}, nil
	}, func(Batch) error { return nil })
	if err != nil || failed || opened != 2 {
		t.Fatalf("failed = %v, err = %v, opened = %d", failed, err, opened)
	}
}

func TestStreamRowsRejectsTruncationWithoutResumeCursor(t *testing.T) {
	t.Parallel()
	failed, err := streamRows(context.Background(), StreamRequest{}, config.Retry{MaxAttempts: 3}, func(context.Context, *exportv1.StreamRowsRequest) (rowStream, error) {
		return &fakeRowStream{responses: []*exportv1.StreamRowsResponse{{}}}, nil
	}, func(Batch) error { return nil })
	if !failed || !errors.Is(err, errProviderStreamIncomplete) {
		t.Fatalf("failed = %v, err = %v", failed, err)
	}
}

func TestStreamRowsDoesNotRetryConsumerFailure(t *testing.T) {
	t.Parallel()
	want := errors.New("storage unavailable")
	var opened int
	failed, err := streamRows(context.Background(), StreamRequest{}, config.Retry{MaxAttempts: 3}, func(context.Context, *exportv1.StreamRowsRequest) (rowStream, error) {
		opened++
		return &fakeRowStream{responses: []*exportv1.StreamRowsResponse{{NextCursor: "cursor-1"}}}, nil
	}, func(Batch) error { return want })
	if failed || !errors.Is(err, want) || opened != 1 {
		t.Fatalf("failed = %v, err = %v, opened = %d", failed, err, opened)
	}
}
