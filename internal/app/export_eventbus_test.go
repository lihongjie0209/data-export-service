package app

import (
	"context"
	"errors"
	"testing"

	platformeventbus "github.com/lihongjie0209/microservice-platform-go/eventbus"
	commonv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/common/v1"
	exportv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/export/v1"
	"google.golang.org/protobuf/proto"
)

type processorStub struct {
	tenant, application, id string
	err                     error
}

func (p *processorStub) Process(_ context.Context, tenant, application, id string) error {
	p.tenant, p.application, p.id = tenant, application, id
	return p.err
}
func TestExportEventRuntimeHandlesRequestedEvent(t *testing.T) {
	processor := &processorStub{}
	runtime := &exportEventRuntime{worker: processor}
	payload, _ := proto.Marshal(&exportv1.ExportJobChangedEvent{Job: &exportv1.ExportJob{Id: "job-1", TenantId: "tenant-1", ApplicationId: "application-1"}, ChangeType: "requested"})
	envelope, _ := platformeventbus.NewEnvelope(platformeventbus.Metadata{EventID: "event-1", EventType: "platform.export.v1.ExportJobChanged", AggregateID: "job-1", AggregateType: "export_job", TenantID: "tenant-1", ApplicationID: "application-1", SchemaVersion: 1}, &exportv1.ExportJobChangedEvent{})
	envelope.Payload = payload
	if err := runtime.handleRequested(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if processor.tenant != "tenant-1" || processor.application != "application-1" || processor.id != "job-1" {
		t.Fatalf("processor=%+v", processor)
	}
}
func TestExportEventRuntimeRejectsMalformedAndPropagatesWorkerFailure(t *testing.T) {
	runtime := &exportEventRuntime{worker: &processorStub{err: errors.New("failed")}}
	if err := runtime.handleRequested(context.Background(), &commonv1.EventEnvelope{Payload: []byte("bad")}); err == nil {
		t.Fatal("malformed payload accepted")
	}
	payload, _ := proto.Marshal(&exportv1.ExportJobChangedEvent{Job: &exportv1.ExportJob{Id: "job", TenantId: "tenant", ApplicationId: "application"}, ChangeType: "requested"})
	err := runtime.handleRequested(context.Background(), &commonv1.EventEnvelope{TenantId: "tenant", ApplicationId: "application", Payload: payload})
	if err == nil || err.Error() != "failed" {
		t.Fatalf("error=%v", err)
	}
}
