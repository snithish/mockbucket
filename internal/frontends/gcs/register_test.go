package gcs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/iam"
	"github.com/snithish/mockbucket/internal/storage"
)

func TestRegisterRejectsUnauthenticatedBucketList(t *testing.T) {
	mux, _, cleanup := newGCSTestMux(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestRegisterRejectsInvalidBearerToken(t *testing.T) {
	mux, _, cleanup := newGCSTestMux(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b", nil)
	req.Header.Set("Authorization", "Bearer bad-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestRegisterCreateBucketConflict(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	if err := metadata.CreateBucket(context.Background(), "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/storage/v1/b", strings.NewReader(`{"name":"demo"}`))
	req.Header.Set("Authorization", "Bearer gcs-token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestRegisterGetMissingObjectReturnsNotFound(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	if err := metadata.CreateBucket(context.Background(), "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b/demo/o/missing.txt?alt=media", nil)
	req.Header.Set("Authorization", "Bearer gcs-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestRegisterUploadTypeResumableReturnsBadRequest(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	if err := metadata.CreateBucket(context.Background(), "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=resumable&name=file.txt", strings.NewReader("body"))
	req.Header.Set("Authorization", "Bearer gcs-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func newGCSTestMux(t *testing.T) (*http.ServeMux, *storage.SQLiteStore, func()) {
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
	if err := metadata.UpsertServiceAccount(context.Background(), core.ServiceAccount{
		ClientEmail: "gcs@mock.iam.gserviceaccount.com",
		Principal:   "gcs-user",
		Token:       "gcs-token",
	}); err != nil {
		_ = metadata.Close()
		t.Fatalf("UpsertServiceAccount() error = %v", err)
	}
	resolver := iam.Resolver{
		Store:          metadata,
		SessionManager: iam.SessionManager{Store: metadata, DefaultDuration: time.Hour},
	}
	deps := common.Dependencies{
		Metadata:       metadata,
		Objects:        objects,
		AuthResolver:   resolver,
		SessionManager: resolver.SessionManager,
	}

	mux := http.NewServeMux()
	Register(mux, config.Default(), deps, nil)
	return mux, metadata, func() { _ = metadata.Close() }
}
