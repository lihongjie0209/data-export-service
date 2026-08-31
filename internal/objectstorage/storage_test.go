package objectstorage

import (
	"context"
	"errors"
	"strings"
	"testing"

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
