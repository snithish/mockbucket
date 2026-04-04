package gcs

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/iam"
	"github.com/snithish/mockbucket/internal/storage"
)

type gcsTestFixture struct {
	deps     common.Dependencies
	metadata *storage.SQLiteStore
	objects  *storage.FilesystemObjectStore
	mux      *http.ServeMux
}

func newGCSTestFixture(t *testing.T) gcsTestFixture {
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
	t.Cleanup(func() { _ = metadata.Close() })
	if err := metadata.UpsertServiceAccount(context.Background(), core.ServiceAccount{
		ClientEmail: "gcs@mock.iam.gserviceaccount.com",
		Principal:   "gcs-user",
		Token:       "gcs-token",
	}); err != nil {
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
	return gcsTestFixture{
		deps:     deps,
		metadata: metadata,
		objects:  objects,
		mux:      mux,
	}
}

func (fixture gcsTestFixture) authedRequest(method, target string, body io.Reader) *http.Request {
	return fixture.requestWithToken(method, target, "gcs-token", body)
}

func (fixture gcsTestFixture) requestWithToken(method, target, token string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, target, body)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func (fixture gcsTestFixture) serve(req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	fixture.mux.ServeHTTP(rec, req)
	return rec
}

func (fixture gcsTestFixture) mustCreateBucket(t *testing.T, name string) {
	t.Helper()
	if err := fixture.metadata.CreateBucket(context.Background(), name); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
}

func (fixture gcsTestFixture) seedServiceAccount(t *testing.T, email, principal, token string) {
	t.Helper()
	if err := fixture.metadata.UpsertServiceAccount(context.Background(), core.ServiceAccount{
		ClientEmail: email,
		Principal:   principal,
		Token:       token,
	}); err != nil {
		t.Fatalf("UpsertServiceAccount() error = %v", err)
	}
}
