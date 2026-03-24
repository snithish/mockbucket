package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/httpx"
	"github.com/snithish/mockbucket/internal/iam"
	"github.com/snithish/mockbucket/internal/storage"
)

func TestPutObjectRollsBackOnMetadataFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	objects, err := storage.NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}
	meta := &failingMetadataStore{bucket: "demo", putErr: errors.New("db down")}
	deps := common.Dependencies{Metadata: meta, Objects: objects, Policy: iam.Evaluator{}}

	req := httptest.NewRequest(http.MethodPut, "/demo/file.txt", bytes.NewBufferString("payload"))
	req = req.WithContext(httpx.ContextWithSubject(req.Context(), allowSubject("s3:PutObject", "arn:mockbucket:s3:::demo/file.txt")))
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "file.txt")
	rec := httptest.NewRecorder()

	handlePutObject(rec, req, deps, "demo", "file.txt")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if _, _, err := objects.OpenObject(ctx, "demo", "file.txt"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expected object rollback, got %v", err)
	}
}

func TestDeleteObjectUsesMetadataTruth(t *testing.T) {
	dir := t.TempDir()
	objects, err := storage.NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}
	if _, err := objects.PutObject(context.Background(), "demo", "file.txt", bytes.NewBufferString("payload")); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	meta := &failingMetadataStore{bucket: "demo", deleteErr: core.ErrNotFound}
	deps := common.Dependencies{Metadata: meta, Objects: objects, Policy: iam.Evaluator{}}

	req := httptest.NewRequest(http.MethodDelete, "/demo/file.txt", nil)
	req = req.WithContext(httpx.ContextWithSubject(req.Context(), allowSubject("s3:DeleteObject", "arn:mockbucket:s3:::demo/file.txt")))
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "file.txt")
	rec := httptest.NewRecorder()

	handleDeleteObject(rec, req, deps, "demo", "file.txt")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if _, _, err := objects.OpenObject(context.Background(), "demo", "file.txt"); err != nil {
		t.Fatalf("expected object to remain, got %v", err)
	}
}

func TestCompleteMultipartRollbackOnDeleteFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	objects, err := storage.NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}
	part1, err := objects.PutMultipartPart(ctx, "upload-1", 1, bytes.NewBufferString("hello "))
	if err != nil {
		t.Fatalf("PutMultipartPart() error = %v", err)
	}
	part2, err := objects.PutMultipartPart(ctx, "upload-1", 2, bytes.NewBufferString("world"))
	if err != nil {
		t.Fatalf("PutMultipartPart() error = %v", err)
	}
	meta := &multipartMetadataStore{
		bucket:               "demo",
		uploadID:             "upload-1",
		parts:                []core.MultipartPart{part1, part2},
		deleteMultipartErr:   errors.New("delete failed"),
		allowMetadataDeletes: true,
	}
	deps := common.Dependencies{Metadata: meta, Objects: objects, Policy: iam.Evaluator{}}

	payload := struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}{}
	payload.Parts = append(payload.Parts, struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}{PartNumber: 1, ETag: `"` + part1.ETag + `"`})
	payload.Parts = append(payload.Parts, struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}{PartNumber: 2, ETag: `"` + part2.ETag + `"`})
	raw, _ := xml.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/demo/object.txt?uploadId=upload-1", bytes.NewReader(raw))
	req = req.WithContext(httpx.ContextWithSubject(req.Context(), allowSubject("s3:PutObject", "arn:mockbucket:s3:::demo/object.txt")))
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "object.txt")
	rec := httptest.NewRecorder()

	handleCompleteMultipartUpload(rec, req, deps, "demo", "object.txt")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if _, _, err := objects.OpenObject(ctx, "demo", "object.txt"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expected object rollback, got %v", err)
	}
}

func allowSubject(action, resource string) core.Subject {
	return core.Subject{
		PrincipalName: "admin",
		Policies: []core.PolicyDocument{{
			Statements: []core.PolicyStatement{{
				Effect:    core.EffectAllow,
				Actions:   []string{action},
				Resources: []string{resource},
			}},
		}},
	}
}

type failingMetadataStore struct {
	bucket    string
	putErr    error
	deleteErr error
}

func (m *failingMetadataStore) Ping(context.Context) error                 { return nil }
func (m *failingMetadataStore) EnsureBucket(context.Context, string) error { return nil }
func (m *failingMetadataStore) CreateBucket(context.Context, string) error { return nil }
func (m *failingMetadataStore) GetBucket(context.Context, string) (core.Bucket, error) {
	return core.Bucket{Name: m.bucket}, nil
}
func (m *failingMetadataStore) ListBuckets(context.Context) ([]core.Bucket, error) { return nil, nil }
func (m *failingMetadataStore) PutObject(context.Context, core.ObjectMetadata) error {
	return m.putErr
}
func (m *failingMetadataStore) GetObject(context.Context, string, string) (core.ObjectMetadata, error) {
	return core.ObjectMetadata{}, core.ErrNotFound
}
func (m *failingMetadataStore) DeleteObject(context.Context, string, string) error {
	return m.deleteErr
}
func (m *failingMetadataStore) ListObjects(context.Context, string, string, int, string) ([]core.ObjectMetadata, error) {
	return nil, nil
}
func (m *failingMetadataStore) UpsertPrincipal(context.Context, core.Principal) error { return nil }
func (m *failingMetadataStore) UpsertRole(context.Context, core.Role) error           { return nil }
func (m *failingMetadataStore) FindAccessKey(context.Context, string) (core.AccessKey, []core.PolicyDocument, error) {
	return core.AccessKey{}, nil, core.ErrNotFound
}
func (m *failingMetadataStore) GetRole(context.Context, string) (core.Role, error) {
	return core.Role{}, core.ErrNotFound
}
func (m *failingMetadataStore) CreateSession(context.Context, core.Session) error { return nil }
func (m *failingMetadataStore) GetSession(context.Context, string) (core.Session, []core.PolicyDocument, error) {
	return core.Session{}, nil, core.ErrNotFound
}
func (m *failingMetadataStore) DeleteExpiredSessions(context.Context, time.Time) error { return nil }
func (m *failingMetadataStore) CreateMultipartUpload(context.Context, core.MultipartUpload) error {
	return nil
}
func (m *failingMetadataStore) GetMultipartUpload(context.Context, string) (core.MultipartUpload, error) {
	return core.MultipartUpload{}, core.ErrNotFound
}
func (m *failingMetadataStore) PutMultipartPart(context.Context, core.MultipartPart) error {
	return nil
}
func (m *failingMetadataStore) ListMultipartParts(context.Context, string) ([]core.MultipartPart, error) {
	return nil, nil
}
func (m *failingMetadataStore) DeleteMultipartUpload(context.Context, string) error { return nil }
func (m *failingMetadataStore) UpsertServiceAccount(context.Context, core.ServiceAccount) error {
	return nil
}
func (m *failingMetadataStore) FindServiceAccountByToken(context.Context, string) (core.ServiceAccount, []core.PolicyDocument, error) {
	return core.ServiceAccount{}, nil, core.ErrNotFound
}
func (m *failingMetadataStore) FindServiceAccountByEmail(context.Context, string) (core.ServiceAccount, error) {
	return core.ServiceAccount{}, core.ErrNotFound
}
func (m *failingMetadataStore) ListServiceAccounts(context.Context) ([]core.ServiceAccount, error) {
	return nil, nil
}
func (m *failingMetadataStore) DeleteServiceAccounts(context.Context) error { return nil }
func (m *failingMetadataStore) Close() error                                { return nil }

type multipartMetadataStore struct {
	bucket               string
	uploadID             string
	parts                []core.MultipartPart
	deleteMultipartErr   error
	allowMetadataDeletes bool
}

func (m *multipartMetadataStore) Ping(context.Context) error                 { return nil }
func (m *multipartMetadataStore) EnsureBucket(context.Context, string) error { return nil }
func (m *multipartMetadataStore) CreateBucket(context.Context, string) error { return nil }
func (m *multipartMetadataStore) GetBucket(context.Context, string) (core.Bucket, error) {
	return core.Bucket{Name: m.bucket}, nil
}
func (m *multipartMetadataStore) ListBuckets(context.Context) ([]core.Bucket, error)   { return nil, nil }
func (m *multipartMetadataStore) PutObject(context.Context, core.ObjectMetadata) error { return nil }
func (m *multipartMetadataStore) GetObject(context.Context, string, string) (core.ObjectMetadata, error) {
	return core.ObjectMetadata{}, core.ErrNotFound
}
func (m *multipartMetadataStore) DeleteObject(context.Context, string, string) error {
	if m.allowMetadataDeletes {
		return nil
	}
	return errors.New("delete object failed")
}
func (m *multipartMetadataStore) ListObjects(context.Context, string, string, int, string) ([]core.ObjectMetadata, error) {
	return nil, nil
}
func (m *multipartMetadataStore) UpsertPrincipal(context.Context, core.Principal) error { return nil }
func (m *multipartMetadataStore) UpsertRole(context.Context, core.Role) error           { return nil }
func (m *multipartMetadataStore) FindAccessKey(context.Context, string) (core.AccessKey, []core.PolicyDocument, error) {
	return core.AccessKey{}, nil, core.ErrNotFound
}
func (m *multipartMetadataStore) GetRole(context.Context, string) (core.Role, error) {
	return core.Role{}, core.ErrNotFound
}
func (m *multipartMetadataStore) CreateSession(context.Context, core.Session) error { return nil }
func (m *multipartMetadataStore) GetSession(context.Context, string) (core.Session, []core.PolicyDocument, error) {
	return core.Session{}, nil, core.ErrNotFound
}
func (m *multipartMetadataStore) DeleteExpiredSessions(context.Context, time.Time) error { return nil }
func (m *multipartMetadataStore) CreateMultipartUpload(context.Context, core.MultipartUpload) error {
	return nil
}
func (m *multipartMetadataStore) GetMultipartUpload(context.Context, string) (core.MultipartUpload, error) {
	return core.MultipartUpload{UploadID: m.uploadID, Bucket: m.bucket, Key: "object.txt"}, nil
}
func (m *multipartMetadataStore) PutMultipartPart(context.Context, core.MultipartPart) error {
	return nil
}
func (m *multipartMetadataStore) ListMultipartParts(context.Context, string) ([]core.MultipartPart, error) {
	return m.parts, nil
}
func (m *multipartMetadataStore) DeleteMultipartUpload(context.Context, string) error {
	return m.deleteMultipartErr
}
func (m *multipartMetadataStore) UpsertServiceAccount(context.Context, core.ServiceAccount) error {
	return nil
}
func (m *multipartMetadataStore) FindServiceAccountByToken(context.Context, string) (core.ServiceAccount, []core.PolicyDocument, error) {
	return core.ServiceAccount{}, nil, core.ErrNotFound
}
func (m *multipartMetadataStore) FindServiceAccountByEmail(context.Context, string) (core.ServiceAccount, error) {
	return core.ServiceAccount{}, core.ErrNotFound
}
func (m *multipartMetadataStore) ListServiceAccounts(context.Context) ([]core.ServiceAccount, error) {
	return nil, nil
}
func (m *multipartMetadataStore) DeleteServiceAccounts(context.Context) error { return nil }
func (m *multipartMetadataStore) Close() error                                { return nil }
