package objectstorage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lihongjie0209/data-export-service/internal/config"
)

func TestDisabledStorageFailsClosed(t *testing.T) {
	storage, err := New(config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if storage.Enabled() {
		t.Fatal("disabled storage reported enabled")
	}
	_, err = storage.Put(context.Background(), "x", strings.NewReader("x"), "text/plain")
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("put error = %v", err)
	}
	if err := storage.Delete(context.Background(), "x"); !errors.Is(err, ErrDisabled) {
		t.Fatalf("delete error = %v", err)
	}
}

func TestPresignUsesPublicEndpointWithoutChangingInternalClient(t *testing.T) {
	storage, err := New(config.Config{ObjectStorage: config.ObjectStorage{
		Enabled:         true,
		Endpoint:        "minio:9000",
		PresignEndpoint: "127.0.0.1:9000",
		AccessKey:       "access",
		SecretKey:       "secret",
		Bucket:          "exports",
		Region:          "us-east-1",
		PresignTTL:      15 * time.Minute,
	}})
	if err != nil {
		t.Fatal(err)
	}
	s3 := storage.(*S3)
	if got := s3.client.EndpointURL().Host; got != "minio:9000" {
		t.Fatalf("internal endpoint = %q", got)
	}
	value, err := s3.PresignDownload(t.Context(), "tenant/job/export.csv", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if value.Host != "127.0.0.1:9000" {
		t.Fatalf("presigned host = %q", value.Host)
	}
}
