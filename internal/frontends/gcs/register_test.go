package gcs

import (
	"context"
	"encoding/json"
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
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if !strings.Contains(rec.Body.String(), `"reason":"notFound"`) {
		t.Fatalf("body = %q, want notFound reason", rec.Body.String())
	}
}

func TestRegisterUploadTypeResumableReturnsBadRequest(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	if err := metadata.CreateBucket(context.Background(), "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=resumable&name=file.txt", strings.NewReader(`{"name":"file.txt"}`))
	req.Header.Set("Authorization", "Bearer gcs-token")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	location := rec.Header().Get("Location")
	if location == "" {
		t.Fatal("Location header is empty")
	}

	statusReq := httptest.NewRequest(http.MethodPut, location, nil)
	statusReq.Header.Set("Content-Range", "bytes */1")
	statusRec := httptest.NewRecorder()
	mux.ServeHTTP(statusRec, statusReq)

	if statusRec.Code != http.StatusPermanentRedirect {
		t.Fatalf("status probe code = %d, want %d", statusRec.Code, http.StatusPermanentRedirect)
	}

	putReq := httptest.NewRequest(http.MethodPut, location, strings.NewReader("payload"))
	putRec := httptest.NewRecorder()
	mux.ServeHTTP(putRec, putReq)

	if putRec.Code != http.StatusOK {
		t.Fatalf("put status = %d, want %d, body = %q", putRec.Code, http.StatusOK, putRec.Body.String())
	}
}

func TestRegisterResumableUploadSupportsChunkedCompletion(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	ctx := context.Background()
	if err := metadata.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	initReq := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=resumable&name=chunks.txt", strings.NewReader(`{"name":"chunks.txt"}`))
	initReq.Header.Set("Authorization", "Bearer gcs-token")
	initReq.Header.Set("Content-Type", "application/json")
	initRec := httptest.NewRecorder()
	mux.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d", initRec.Code, http.StatusOK)
	}
	location := initRec.Header().Get("Location")
	if location == "" {
		t.Fatal("Location header is empty")
	}

	firstReq := httptest.NewRequest(http.MethodPut, location, strings.NewReader("abc"))
	firstReq.Header.Set("Content-Range", "bytes 0-2/6")
	firstRec := httptest.NewRecorder()
	mux.ServeHTTP(firstRec, firstReq)
	if firstRec.Code != http.StatusPermanentRedirect {
		t.Fatalf("first chunk status = %d, want %d, body = %q", firstRec.Code, http.StatusPermanentRedirect, firstRec.Body.String())
	}
	if got := firstRec.Header().Get("Range"); got != "bytes=0-2" {
		t.Fatalf("first chunk Range = %q, want %q", got, "bytes=0-2")
	}

	secondReq := httptest.NewRequest(http.MethodPut, location, strings.NewReader("def"))
	secondReq.Header.Set("Content-Range", "bytes 3-5/6")
	secondRec := httptest.NewRecorder()
	mux.ServeHTTP(secondRec, secondReq)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second chunk status = %d, want %d, body = %q", secondRec.Code, http.StatusOK, secondRec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/storage/v1/b/demo/o/chunks.txt?alt=media", nil)
	getReq.Header.Set("Authorization", "Bearer gcs-token")
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", getRec.Code, http.StatusOK)
	}
	if got := getRec.Body.String(); got != "abcdef" {
		t.Fatalf("body = %q, want %q", got, "abcdef")
	}
}

func TestRegisterResumableUploadSupportsZeroByteFinalize(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	ctx := context.Background()
	if err := metadata.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	initReq := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=resumable&name=_SUCCESS", strings.NewReader(`{"name":"_SUCCESS"}`))
	initReq.Header.Set("Authorization", "Bearer gcs-token")
	initReq.Header.Set("Content-Type", "application/json")
	initRec := httptest.NewRecorder()
	mux.ServeHTTP(initRec, initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d", initRec.Code, http.StatusOK)
	}
	location := initRec.Header().Get("Location")
	if location == "" {
		t.Fatal("Location header is empty")
	}

	finalizeReq := httptest.NewRequest(http.MethodPut, location, nil)
	finalizeReq.Header.Set("Content-Range", "bytes */0")
	finalizeRec := httptest.NewRecorder()
	mux.ServeHTTP(finalizeRec, finalizeReq)
	if finalizeRec.Code != http.StatusOK {
		t.Fatalf("finalize status = %d, want %d, body = %q", finalizeRec.Code, http.StatusOK, finalizeRec.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/storage/v1/b/demo/o/_SUCCESS?alt=media", nil)
	getReq.Header.Set("Authorization", "Bearer gcs-token")
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", getRec.Code, http.StatusOK)
	}
	if got := getRec.Body.String(); got != "" {
		t.Fatalf("body = %q, want empty", got)
	}
}

func TestRegisterUploadTypeUnsupportedReturnsBadRequest(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	if err := metadata.CreateBucket(context.Background(), "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=unknown&name=file.txt", strings.NewReader("body"))
	req.Header.Set("Authorization", "Bearer gcs-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestRegisterRewriteObjectCopiesSource(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	ctx := context.Background()
	if err := metadata.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	putReq := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name=src.txt", strings.NewReader("payload"))
	putReq.Header.Set("Authorization", "Bearer gcs-token")
	putRec := httptest.NewRecorder()
	mux.ServeHTTP(putRec, putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d, body = %q", putRec.Code, http.StatusOK, putRec.Body.String())
	}

	rewriteReq := httptest.NewRequest(http.MethodPost, "/storage/v1/b/demo/o/src.txt/rewriteTo/b/demo/o/dst.txt", nil)
	rewriteReq.Header.Set("Authorization", "Bearer gcs-token")
	rewriteRec := httptest.NewRecorder()
	mux.ServeHTTP(rewriteRec, rewriteReq)

	if rewriteRec.Code != http.StatusOK {
		t.Fatalf("rewrite status = %d, want %d, body = %q", rewriteRec.Code, http.StatusOK, rewriteRec.Body.String())
	}
	var resp struct {
		Done     bool `json:"done"`
		Resource struct {
			Name string `json:"name"`
			Size string `json:"size"`
		} `json:"resource"`
	}
	if err := json.NewDecoder(rewriteRec.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if !resp.Done {
		t.Fatal("done = false, want true")
	}
	if resp.Resource.Name != "dst.txt" {
		t.Fatalf("resource.name = %q, want %q", resp.Resource.Name, "dst.txt")
	}
	if resp.Resource.Size != "7" {
		t.Fatalf("resource.size = %q, want %q", resp.Resource.Size, "7")
	}

	getReq := httptest.NewRequest(http.MethodGet, "/storage/v1/b/demo/o/dst.txt?alt=media", nil)
	getReq.Header.Set("Authorization", "Bearer gcs-token")
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", getRec.Code, http.StatusOK)
	}
	if got := getRec.Body.String(); got != "payload" {
		t.Fatalf("body = %q, want %q", got, "payload")
	}
}

func TestRegisterDeleteMissingObjectIsIdempotent(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	if err := metadata.CreateBucket(context.Background(), "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodDelete, "/storage/v1/b/demo/o/missing/", nil)
	req.Header.Set("Authorization", "Bearer gcs-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d, body = %q", rec.Code, http.StatusNoContent, rec.Body.String())
	}
}

func TestRegisterDeleteObjectRemovesPrefixDescendants(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	ctx := context.Background()
	if err := metadata.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	items := []struct {
		key  string
		body string
	}{
		{key: "tree", body: ""},
		{key: "tree/child.txt", body: "payload"},
		{key: "tree/nested/grandchild.txt", body: "payload"},
	}
	for _, item := range items {
		putReq := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name="+item.key, strings.NewReader(item.body))
		putReq.Header.Set("Authorization", "Bearer gcs-token")
		putRec := httptest.NewRecorder()
		mux.ServeHTTP(putRec, putReq)
		if putRec.Code != http.StatusOK {
			t.Fatalf("upload %q status = %d, want %d, body = %q", item.key, putRec.Code, http.StatusOK, putRec.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodDelete, "/storage/v1/b/demo/o/tree", nil)
	req.Header.Set("Authorization", "Bearer gcs-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d, body = %q", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/storage/v1/b/demo/o?prefix=tree", nil)
	listReq.Header.Set("Authorization", "Bearer gcs-token")
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", listRec.Code, http.StatusOK)
	}
	var resp struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	if err := json.NewDecoder(listRec.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(resp.Items) != 0 {
		t.Fatalf("items = %+v, want empty", resp.Items)
	}
}

func TestRegisterZeroByteDirectoryMarkerAllowsChildren(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	ctx := context.Background()
	if err := metadata.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	markerReq := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name=tree", strings.NewReader(""))
	markerReq.Header.Set("Authorization", "Bearer gcs-token")
	markerRec := httptest.NewRecorder()
	mux.ServeHTTP(markerRec, markerReq)
	if markerRec.Code != http.StatusOK {
		t.Fatalf("marker status = %d, want %d, body = %q", markerRec.Code, http.StatusOK, markerRec.Body.String())
	}

	childReq := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name=tree/child.txt", strings.NewReader("payload"))
	childReq.Header.Set("Authorization", "Bearer gcs-token")
	childRec := httptest.NewRecorder()
	mux.ServeHTTP(childRec, childReq)
	if childRec.Code != http.StatusOK {
		t.Fatalf("child status = %d, want %d, body = %q", childRec.Code, http.StatusOK, childRec.Body.String())
	}
}

func TestRegisterListObjectsDelimiterReturnsPrefixes(t *testing.T) {
	mux, metadata, cleanup := newGCSTestMux(t)
	defer cleanup()
	ctx := context.Background()
	if err := metadata.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	for _, key := range []string{"dir/file.txt", "dir/sub/nested.txt"} {
		putReq := httptest.NewRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name="+key, strings.NewReader("payload"))
		putReq.Header.Set("Authorization", "Bearer gcs-token")
		putRec := httptest.NewRecorder()
		mux.ServeHTTP(putRec, putReq)
		if putRec.Code != http.StatusOK {
			t.Fatalf("upload %q status = %d, want %d, body = %q", key, putRec.Code, http.StatusOK, putRec.Body.String())
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b/demo/o?prefix=dir/&delimiter=/", nil)
	req.Header.Set("Authorization", "Bearer gcs-token")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %q", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp struct {
		Prefixes []string `json:"prefixes"`
		Items    []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].Name != "dir/file.txt" {
		t.Fatalf("items = %+v, want dir/file.txt", resp.Items)
	}
	if len(resp.Prefixes) != 1 || resp.Prefixes[0] != "dir/sub/" {
		t.Fatalf("prefixes = %v, want %v", resp.Prefixes, []string{"dir/sub/"})
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
