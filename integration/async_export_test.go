//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/lihongjie0209/data-export-service/internal/app"
	"github.com/lihongjie0209/data-export-service/internal/config"
	exportv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/export/v1"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestAsynchronousExportThroughJetStreamAndMinIO(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	postgresContainer, err := postgres.Run(ctx, "postgres:17-alpine", postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("app"), postgres.BasicWaitStrategies(), postgres.WithSQLDriver("pgx"))
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, postgresContainer)
	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	natsURL := startNATS(t, ctx)
	endpoint, accessKey, secretKey := startMinIO(t, ctx)
	admin, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	if err := admin.MakeBucket(ctx, "platform-exports", minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	providerAddress := startExportProvider(t)
	httpAddress, grpcAddress := freeAddress(t), freeAddress(t)
	migrationPath, _ := filepath.Abs(filepath.Join("..", "migrations", "postgres"))
	const psk = "integration-export-psk-0000000000000000"
	cfg := config.Config{Runtime: config.Runtime{ActiveProfile: "integration"}, App: config.App{Name: "data-export-service", Env: "integration", ShutdownTimeout: 10 * time.Second}, HTTP: config.HTTP{Address: httpAddress, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: time.Minute, RequestTimeout: 5 * time.Second, MaxBodyBytes: 1 << 20}, GRPC: config.GRPC{Enabled: true, Address: grpcAddress, MaxReceiveBytes: 4 << 20}, Log: config.Log{Level: "error", Format: "json", File: filepath.Join(t.TempDir(), "app.log"), MaxSizeMB: 1, MaxBackups: 1, MaxAgeDays: 1}, Database: config.Database{Enabled: true, Type: "postgres", DSN: dsn, MaxOpenConns: 5, MaxIdleConns: 2, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute, PingTimeout: 10 * time.Second}, Migration: config.Migration{AutoUp: true, Path: migrationPath, DatabaseURL: dsn, Table: "async_export_schema_migrations"}, Health: config.Health{DatabaseTimeout: 2 * time.Second, RedisTimeout: 2 * time.Second}, JWT: config.JWT{Issuer: "integration", Secret: psk, TTL: time.Hour}, Auth: config.Auth{PSK: config.PSK{Enabled: true, Key: psk, HTTPPaths: []string{"/api/v1/exports/*"}}, SkipHTTPPaths: []string{"/api/v1/version"}, SkipGRPCMethods: []string{"/grpc.health.v1.Health/*"}}, Cron: config.Cron{Enabled: false, Timezone: "UTC"}, EventBus: config.EventBus{Enabled: true, URLs: []string{natsURL}, StreamName: "PLATFORM_EVENTS", Subjects: []string{"platform.>"}, Storage: "memory", MaxAge: time.Hour, DuplicateWindow: time.Minute, ConnectTimeout: 10 * time.Second, ReconnectWait: time.Second, PublishTimeout: 5 * time.Second, ConsumerAckWait: 2 * time.Minute, ConsumerMaxDeliver: 3, DispatchInterval: 20 * time.Millisecond, DispatchBatchSize: 10, DispatchLease: time.Minute, DispatchRetryDelay: 100 * time.Millisecond, PublishedRetention: time.Hour, CleanupInterval: time.Hour, CleanupBatchSize: 10}, Export: config.Export{BatchSize: 2, MaxRows: 100, MaxBytes: 1 << 20, JobTimeout: time.Minute, ResultTTL: time.Hour, WorkerCount: 1, ProgressEvery: 1}, ObjectStorage: config.ObjectStorage{Enabled: true, Endpoint: endpoint, AccessKey: accessKey, SecretKey: secretKey, Bucket: "platform-exports", PresignTTL: time.Minute}, Outbound: config.Outbound{GRPC: map[string]config.GRPCUpstream{"test-provider": {Target: providerAddress, Timeout: 5 * time.Second, Retry: config.Retry{MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}}}}}
	application := app.New(cfg)
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = application.Stop(stopCtx)
	})
	baseURL := "http://" + httpAddress
	body, _ := postJSONBody(t, baseURL+"/api/v1/exports/create", "PSK "+psk, "", `{"tenant_id":"tenant-1","dataset_code":"test.rows","provider_service":"test-provider","format":"csv","filename":"rows","selected_columns":["id","name"],"idempotency_key":"async-1"}`)
	var created envelopeBody[struct {
		Job struct {
			ID string `json:"id"`
		} `json:"job"`
	}]
	if err := json.Unmarshal(body, &created); err != nil {
		t.Fatal(err)
	}
	if created.Body.Job.ID == "" {
		t.Fatalf("create response=%s", body)
	}
	var succeeded map[string]any
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		payload, _ := postJSONBody(t, baseURL+"/api/v1/exports/get", "PSK "+psk, "", `{"tenant_id":"tenant-1","id":"`+created.Body.Job.ID+`"}`)
		var response envelopeBody[map[string]any]
		if json.Unmarshal(payload, &response) == nil && response.Body["status"] == "succeeded" {
			succeeded = response.Body
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if succeeded == nil {
		t.Fatal("export job did not succeed")
	}
	downloadBody, statusCode := postJSONBody(t, baseURL+"/api/v1/exports/download", "PSK "+psk, "", `{"tenant_id":"tenant-1","id":"`+created.Body.Job.ID+`","ttl_seconds":30}`)
	if statusCode != http.StatusOK {
		t.Fatalf("download response=%s", downloadBody)
	}
	var download envelopeBody[struct {
		URL string `json:"url"`
	}]
	if err := json.Unmarshal(downloadBody, &download); err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, download.Body.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if string(data) != "ID,Name\n1,Alice\n2,Bob\n" {
		t.Fatalf("file=%q", data)
	}
}

type envelopeBody[T any] struct {
	Code int `json:"code"`
	Body T   `json:"body"`
}
type testExportProvider struct {
	exportv1.UnimplementedExportProviderServiceServer
}

func (*testExportProvider) DescribeDataset(context.Context, *exportv1.DescribeDatasetRequest) (*exportv1.DescribeDatasetResponse, error) {
	return &exportv1.DescribeDatasetResponse{Dataset: &exportv1.DatasetDescriptor{Code: "test.rows"}}, nil
}
func (*testExportProvider) StreamRows(_ *exportv1.StreamRowsRequest, stream exportv1.ExportProviderService_StreamRowsServer) error {
	one, _ := structpb.NewStruct(map[string]any{"id": "1", "name": "Alice"})
	two, _ := structpb.NewStruct(map[string]any{"id": "2", "name": "Bob"})
	return stream.Send(&exportv1.StreamRowsResponse{Columns: []*exportv1.ExportColumn{{Key: "id", Title: "ID"}, {Key: "name", Title: "Name"}}, Rows: []*structpb.Struct{one, two}, EstimatedTotalRows: 2, Done: true})
}
func startExportProvider(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	exportv1.RegisterExportProviderServiceServer(server, &testExportProvider{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return listener.Addr().String()
}
func startNATS(t *testing.T, ctx context.Context) string {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "nats:2.11.11-alpine", ExposedPorts: []string{"4222/tcp", "8222/tcp"}, Cmd: []string{"-js", "-m", "8222"}, WaitingFor: wait.ForHTTP("/healthz").WithPort("8222/tcp")}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "4222/tcp")
	return "nats://" + host + ":" + port.Port()
}
func startMinIO(t *testing.T, ctx context.Context) (string, string, string) {
	t.Helper()
	const access = "integration-access"
	const secret = "integration-secret-key"
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "minio/minio:RELEASE.2025-09-07T16-13-09Z", ExposedPorts: []string{"9000/tcp"}, Env: map[string]string{"MINIO_ROOT_USER": access, "MINIO_ROOT_PASSWORD": secret}, Cmd: []string{"server", "/data"}, WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp")}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "9000/tcp")
	return host + ":" + port.Port(), access, secret
}
