package azure_datalake

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/storage"
)

func TestRegisterRejectsInvalidAuthorizationScheme(t *testing.T) {
	mux, _, cleanup := newAzureDataLakeTestMux(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodHead, "/demo", nil)
	req.Header.Set("Authorization", "Bearer invalid")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestRegisterMissingPathReturnsNotFound(t *testing.T) {
	mux, _, cleanup := newAzureDataLakeTestMux(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/missing/path.txt", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestRegisterDeleteFilesystemConflictWhenNotEmpty(t *testing.T) {
	mux, metadata, cleanup := newAzureDataLakeTestMux(t)
	defer cleanup()
	if err := metadata.CreateBucket(context.Background(), "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
	if err := metadata.PutObject(context.Background(), core.ObjectMetadata{
		Bucket:     "demo",
		Key:        "file.txt",
		Path:       "/tmp/file.txt",
		Size:       int64(len("hello")),
		ETag:       "etag",
		CreatedAt:  time.Now().UTC(),
		ModifiedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/demo", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
	if got := rec.Header().Get("x-ms-error-code"); got != "FilesystemNotEmpty" {
		t.Fatalf("x-ms-error-code = %q, want %q", got, "FilesystemNotEmpty")
	}
}

func TestRegisterRejectsBlobOperationHeader(t *testing.T) {
	mux, _, cleanup := newAzureDataLakeTestMux(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/demo/file.txt", nil)
	req.Header.Set("X-Ms-Blob-Type", "BlockBlob")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	if got := rec.Header().Get("x-ms-error-code"); got != "InvalidHeader" {
		t.Fatalf("x-ms-error-code = %q, want %q", got, "InvalidHeader")
	}
}

func newAzureDataLakeTestMux(t *testing.T) (*http.ServeMux, *storage.SQLiteStore, func()) {
	t.Helper()
	dir := t.TempDir()
	metadata, err := storage.OpenSQLite(filepath.Join(dir, "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	objects, err := storage.NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		_ = metadata.Close()
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}
	deps := common.Dependencies{
		Metadata: metadata,
		Objects:  objects,
	}
	cfg := config.Default()
	cfg.Azure.Account = "mockstorage"
	cfg.Azure.Key = base64.StdEncoding.EncodeToString([]byte("mockstorage-key-32bytes!!"))

	mux := http.NewServeMux()
	Register(mux, cfg, deps)
	return mux, metadata, func() { _ = metadata.Close() }
}
