//go:build integration

package integration

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lihongjie0209/data-export-service/internal/config"
	"github.com/lihongjie0209/data-export-service/internal/objectstorage"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestS3StreamingUploadPresignAndDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	const accessKey = "integration-access"
	const secretKey = "integration-secret-key"
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Image: "minio/minio:RELEASE.2025-09-07T16-13-09Z", ExposedPorts: []string{"9000/tcp"}, Env: map[string]string{"MINIO_ROOT_USER": accessKey, "MINIO_ROOT_PASSWORD": secretKey}, Cmd: []string{"server", "/data"}, WaitingFor: wait.ForHTTP("/minio/health/live").WithPort("9000/tcp").WithStartupTimeout(time.Minute)}, Started: true})
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	host, err := container.Host(ctx)
	if err != nil {
		t.Fatal(err)
	}
	port, err := container.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := host + ":" + port.Port()
	admin, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: false})
	if err != nil {
		t.Fatal(err)
	}
	const bucket = "platform-exports"
	if err := admin.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
		t.Fatal(err)
	}
	storage, err := objectstorage.New(config.Config{ObjectStorage: config.ObjectStorage{Enabled: true, Endpoint: endpoint, AccessKey: accessKey, SecretKey: secretKey, Bucket: bucket, PresignTTL: time.Minute}})
	if err != nil {
		t.Fatal(err)
	}
	payload := strings.Repeat("streamed-row\n", 10000)
	stored, err := storage.Put(ctx, "tenant/job.csv", strings.NewReader(payload), "text/csv")
	if err != nil {
		t.Fatal(err)
	}
	if stored.Size != int64(len(payload)) {
		t.Fatalf("size=%d want=%d", stored.Size, len(payload))
	}
	download, err := storage.PresignDownload(ctx, "tenant/job.csv", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, download.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || string(body) != payload {
		t.Fatalf("download status=%d bytes=%d err=%v", response.StatusCode, len(body), err)
	}
	if err := storage.Delete(ctx, "tenant/job.csv"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.StatObject(ctx, bucket, "tenant/job.csv", minio.StatObjectOptions{}); err == nil {
		t.Fatal("deleted object still exists")
	}
}
