package objectstorage

import (
	"context"
	"errors"
	"io"
	"net/url"
	"time"

	"github.com/lihongjie0209/data-export-service/internal/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var ErrDisabled = errors.New("object storage is disabled")

type StoredObject struct {
	Size int64
	ETag string
}

type Storage interface {
	Put(context.Context, string, io.Reader, string) (StoredObject, error)
	Delete(context.Context, string) error
	PresignDownload(context.Context, string, time.Duration) (*url.URL, error)
	Bucket() string
	Enabled() bool
}

type S3 struct {
	client        *minio.Client
	presignClient *minio.Client
	cfg           config.ObjectStorage
}

func New(cfg config.Config) (Storage, error) {
	c := cfg.ObjectStorage
	if !c.Enabled {
		return &S3{cfg: c}, nil
	}
	client, err := minio.New(c.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
		Secure: c.UseSSL,
		Region: c.Region,
	})
	if err != nil {
		return nil, err
	}
	presignClient := client
	if c.PresignEndpoint != "" && (c.PresignEndpoint != c.Endpoint || c.PresignUseSSL != c.UseSSL) {
		presignClient, err = minio.New(c.PresignEndpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(c.AccessKey, c.SecretKey, ""),
			Secure: c.PresignUseSSL,
			Region: c.Region,
		})
		if err != nil {
			return nil, err
		}
	}
	return &S3{client: client, presignClient: presignClient, cfg: c}, nil
}

func (s *S3) Enabled() bool  { return s != nil && s.cfg.Enabled }
func (s *S3) Bucket() string { return s.cfg.Bucket }

func (s *S3) Put(ctx context.Context, key string, source io.Reader, contentType string) (StoredObject, error) {
	if !s.Enabled() {
		return StoredObject{}, ErrDisabled
	}
	// A size of -1 makes minio-go stream a multipart upload. Cancellation
	// aborts the in-flight request, so a failed export never exposes a partial object.
	info, err := s.client.PutObject(ctx, s.cfg.Bucket, key, source, -1, minio.PutObjectOptions{ContentType: contentType})
	return StoredObject{Size: info.Size, ETag: info.ETag}, err
}

func (s *S3) Delete(ctx context.Context, key string) error {
	if !s.Enabled() {
		return ErrDisabled
	}
	return s.client.RemoveObject(ctx, s.cfg.Bucket, key, minio.RemoveObjectOptions{})
}

func (s *S3) PresignDownload(ctx context.Context, key string, ttl time.Duration) (*url.URL, error) {
	if !s.Enabled() {
		return nil, ErrDisabled
	}
	if ttl <= 0 || ttl > s.cfg.PresignTTL {
		ttl = s.cfg.PresignTTL
	}
	return s.presignClient.PresignedGetObject(ctx, s.cfg.Bucket, key, ttl, nil)
}
