package azure_blob

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	azauth "github.com/snithish/mockbucket/internal/auth/azure"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/storage"
)

func TestListContainers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	objects, err := storage.NewFilesystemObjectStore(dir + "/objects")
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	meta, err := storage.OpenSQLite(dir + "/meta.db")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { meta.Close() })

	if err := meta.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	deps := common.Dependencies{
		Metadata: meta,
		Objects:  objects,
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()

	handleListContainers(rec, req, deps)

	if rec.Code != http.StatusOK {
		t.Fatalf("handleListContainers() status = %d, want 200", rec.Code)
	}

	var resp ListContainersResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("xml.Unmarshal() error = %v", err)
	}

	if len(resp.Containers.Container) != 1 {
		t.Fatalf("len(Containers) = %d, want 1", len(resp.Containers.Container))
	}

	if resp.Containers.Container[0].Name != "demo" {
		t.Fatalf("Container[0].Name = %q, want demo", resp.Containers.Container[0].Name)
	}
}

func TestCreateContainer(t *testing.T) {
	dir := t.TempDir()

	objects, err := storage.NewFilesystemObjectStore(dir + "/objects")
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	meta, err := storage.OpenSQLite(dir + "/meta.db")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { meta.Close() })

	deps := common.Dependencies{
		Metadata: meta,
		Objects:  objects,
	}

	req := httptest.NewRequest(http.MethodPut, "/test-container?restype=container", nil)
	rec := httptest.NewRecorder()

	handleCreateContainer(rec, req, deps, "test-container")

	if rec.Code != http.StatusCreated {
		t.Fatalf("handleCreateContainer() status = %d, want 201", rec.Code)
	}

	if rec.Header().Get("ETag") == "" {
		t.Fatal("ETag header is missing")
	}
}

func TestCreateContainerConflict(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	objects, err := storage.NewFilesystemObjectStore(dir + "/objects")
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	meta, err := storage.OpenSQLite(dir + "/meta.db")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { meta.Close() })

	if err := meta.CreateBucket(ctx, "existing"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	deps := common.Dependencies{
		Metadata: meta,
		Objects:  objects,
	}

	req := httptest.NewRequest(http.MethodPut, "/existing?restype=container", nil)
	rec := httptest.NewRecorder()

	handleCreateContainer(rec, req, deps, "existing")

	if rec.Code != http.StatusConflict {
		t.Fatalf("handleCreateContainer() status = %d, want 409", rec.Code)
	}
}

func TestGetContainerProperties(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	objects, err := storage.NewFilesystemObjectStore(dir + "/objects")
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	meta, err := storage.OpenSQLite(dir + "/meta.db")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { meta.Close() })

	if err := meta.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	deps := common.Dependencies{
		Metadata: meta,
		Objects:  objects,
	}

	req := httptest.NewRequest(http.MethodHead, "/demo?restype=container", nil)
	rec := httptest.NewRecorder()

	handleGetContainerProperties(rec, req, deps, "demo")

	if rec.Code != http.StatusOK {
		t.Fatalf("handleGetContainerProperties() status = %d, want 200", rec.Code)
	}
}

func TestGetContainerPropertiesNotFound(t *testing.T) {
	dir := t.TempDir()

	objects, err := storage.NewFilesystemObjectStore(dir + "/objects")
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	meta, err := storage.OpenSQLite(dir + "/meta.db")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { meta.Close() })

	deps := common.Dependencies{
		Metadata: meta,
		Objects:  objects,
	}

	req := httptest.NewRequest(http.MethodHead, "/nonexistent?restype=container", nil)
	rec := httptest.NewRecorder()

	handleGetContainerProperties(rec, req, deps, "nonexistent")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("handleGetContainerProperties() status = %d, want 404", rec.Code)
	}
}

func TestDeleteContainerDeletesBucket(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	objects, err := storage.NewFilesystemObjectStore(dir + "/objects")
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	meta, err := storage.OpenSQLite(dir + "/meta.db")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = meta.Close() })

	if err := meta.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	deps := common.Dependencies{
		Metadata: meta,
		Objects:  objects,
	}

	req := httptest.NewRequest(http.MethodDelete, "/demo?restype=container", nil)
	rec := httptest.NewRecorder()

	handleDeleteContainer(rec, req, deps, "demo")

	if rec.Code != http.StatusAccepted {
		t.Fatalf("handleDeleteContainer() status = %d, want 202", rec.Code)
	}
	if _, err := meta.GetBucket(ctx, "demo"); err != core.ErrNotFound {
		t.Fatalf("GetBucket() error = %v, want %v", err, core.ErrNotFound)
	}
}

func TestDeleteContainerNonEmptyReturnsConflict(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	objects, err := storage.NewFilesystemObjectStore(dir + "/objects")
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	meta, err := storage.OpenSQLite(dir + "/meta.db")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = meta.Close() })

	if err := meta.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
	objMeta, err := objects.PutObject(ctx, "demo", "file.txt", bytes.NewBufferString("hello"))
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	if err := meta.PutObject(ctx, objMeta); err != nil {
		t.Fatalf("PutObject(metadata) error = %v", err)
	}

	deps := common.Dependencies{
		Metadata: meta,
		Objects:  objects,
	}

	req := httptest.NewRequest(http.MethodDelete, "/demo?restype=container", nil)
	rec := httptest.NewRecorder()

	handleDeleteContainer(rec, req, deps, "demo")

	if rec.Code != http.StatusConflict {
		t.Fatalf("handleDeleteContainer() status = %d, want 409", rec.Code)
	}
}

func TestPutAndGetBlob(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	objects, err := storage.NewFilesystemObjectStore(dir + "/objects")
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	meta, err := storage.OpenSQLite(dir + "/meta.db")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { meta.Close() })

	if err := meta.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	deps := common.Dependencies{
		Metadata: meta,
		Objects:  objects,
	}

	body := bytes.NewBufferString("hello world")
	req := httptest.NewRequest(http.MethodPut, "/demo/test.txt", body)
	rec := httptest.NewRecorder()

	handlePutBlob(rec, req, deps, "demo", "test.txt")

	if rec.Code != http.StatusCreated {
		t.Fatalf("handlePutBlob() status = %d, want 201", rec.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/demo/test.txt", nil)
	getRec := httptest.NewRecorder()

	handleGetBlob(getRec, getReq, deps, "demo", "test.txt")

	if getRec.Code != http.StatusOK {
		t.Fatalf("handleGetBlob() status = %d, want 200", getRec.Code)
	}

	if getRec.Body.String() != "hello world" {
		t.Fatalf("handleGetBlob() body = %q, want hello world", getRec.Body.String())
	}
}

func TestDeleteBlob(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	objects, err := storage.NewFilesystemObjectStore(dir + "/objects")
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	meta, err := storage.OpenSQLite(dir + "/meta.db")
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { meta.Close() })

	if err := meta.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}

	metaObj := core.ObjectMetadata{
		Bucket: "demo",
		Key:    "delete-me.txt",
	}
	if err := meta.PutObject(ctx, metaObj); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	deps := common.Dependencies{
		Metadata: meta,
		Objects:  objects,
	}

	req := httptest.NewRequest(http.MethodDelete, "/demo/delete-me.txt", nil)
	rec := httptest.NewRecorder()

	handleDeleteBlob(rec, req, deps, "demo", "delete-me.txt")

	if rec.Code != http.StatusAccepted {
		t.Fatalf("handleDeleteBlob() status = %d, want 202", rec.Code)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/demo/delete-me.txt", nil)
	getRec := httptest.NewRecorder()

	handleGetBlob(getRec, getReq, deps, "demo", "delete-me.txt")

	if getRec.Code != http.StatusNotFound {
		t.Fatalf("handleGetBlob() after delete status = %d, want 404", getRec.Code)
	}
}

func TestAuthenticateAnonymousOrSharedKey(t *testing.T) {
	key := []byte("test-key-32-characters-long!!")
	accounts := []azauth.AccountConfig{
		{Name: "testaccount", Key: key},
	}
	resolver := azauth.NewAuthResolver(accounts)

	handler := azauth.AuthenticateAnonymousOrSharedKey(resolver)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		account := azauth.GetAccountFromContext(r.Context())
		w.Write([]byte(account))
	}))

	t.Run("anonymous request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if rec.Body.String() != "anonymous" {
			t.Fatalf("account = %q, want anonymous", rec.Body.String())
		}
	})

	t.Run("shared key valid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("Authorization", "SharedKey testaccount:signature")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if rec.Body.String() != "testaccount" {
			t.Fatalf("account = %q, want testaccount", rec.Body.String())
		}
	})
}

func TestAuthResolver(t *testing.T) {
	key := []byte("test-key")
	accounts := []azauth.AccountConfig{
		{Name: "account1", Key: key},
	}

	resolver := azauth.NewAuthResolver(accounts)

	t.Run("found", func(t *testing.T) {
		acc, ok := resolver.GetAccount("account1")
		if !ok {
			t.Fatal("GetAccount() returned false, want true")
		}
		if acc.Name != "account1" {
			t.Fatalf("Name = %q, want account1", acc.Name)
		}
	})

	t.Run("not found", func(t *testing.T) {
		_, ok := resolver.GetAccount("nonexistent")
		if ok {
			t.Fatal("GetAccount() returned true, want false")
		}
	})
}

func TestComputeSharedKeySignature(t *testing.T) {
	key := []byte("test-key-32-characters-long!!")
	accountName := "testaccount"

	sig := computeTestSignature(accountName, key, "GET", "/test", "", "")

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Authorization", "SharedKey "+accountName+":"+sig)

	if req.Header.Get("Authorization") == "" {
		t.Fatal("Authorization header not set")
	}
}

func computeTestSignature(accountName string, accountKey []byte, method, path, query, body string) string {
	stringToSign := method + "\n\n" + "\n\n\n\n\n\n\n\n\n" + query + "\n" + path + "\n" + accountName

	h := hmac.New(sha256.New, accountKey)
	h.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
