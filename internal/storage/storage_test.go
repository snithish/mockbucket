package storage

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/core"
)

func TestFilesystemAndSQLiteObjectLifecycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	objects, err := NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}
	metadata, err := OpenSQLite(filepath.Join(dir, "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = metadata.Close() }()
	if err := metadata.EnsureBucket(ctx, "demo"); err != nil {
		t.Fatalf("EnsureBucket() error = %v", err)
	}
	meta, err := objects.PutObject(ctx, "demo", "prefix/object.txt", bytes.NewBufferString("hello world"))
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	if err := metadata.PutObject(ctx, meta); err != nil {
		t.Fatalf("PutObject(metadata) error = %v", err)
	}
	stored, err := metadata.GetObject(ctx, "demo", "prefix/object.txt")
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	if stored.Size != int64(len("hello world")) {
		t.Fatalf("stored size = %d, want %d", stored.Size, len("hello world"))
	}
	listed, err := metadata.ListObjects(ctx, "demo", "prefix/", 10, "")
	if err != nil {
		t.Fatalf("ListObjects() error = %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("len(ListObjects()) = %d, want 1", len(listed))
	}
	reader, _, err := objects.OpenObject(ctx, "demo", "prefix/object.txt")
	if err != nil {
		t.Fatalf("OpenObject() error = %v", err)
	}
	defer func() { _ = reader.Close() }()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(reader); err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}
	if got := buf.String(); got != "hello world" {
		t.Fatalf("object content = %q, want hello world", got)
	}
}

func TestSessionRoundTrip(t *testing.T) {
	ctx := context.Background()
	metadata, err := OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = metadata.Close() }()
	if err := metadata.UpsertRole(ctx, core.Role{Name: "reader"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	session := core.Session{Token: "token", AccessKeyID: "ak", SecretKey: "sk", PrincipalName: "admin", RoleName: "reader", SessionName: "cli", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour)}
	if err := metadata.CreateSession(ctx, session); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	stored, err := metadata.GetSession(ctx, "token")
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if stored.RoleName != "reader" {
		t.Fatalf("unexpected session round trip: %+v", stored)
	}
}

func TestAccessKeyAllowedRoles(t *testing.T) {
	ctx := context.Background()
	metadata, err := OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = metadata.Close() }()

	state := SeedState{
		Roles:      []core.Role{{Name: "reader"}, {Name: "writer"}},
		AccessKeys: []SeedAccessKey{{ID: "k1", Secret: "s1", AllowedRoles: []string{"reader"}}},
	}
	if err := metadata.ApplySeedState(ctx, state, &noopObjectStore{}); err != nil {
		t.Fatalf("ApplySeedState() error = %v", err)
	}

	key, err := metadata.FindAccessKey(ctx, "k1")
	if err != nil {
		t.Fatalf("FindAccessKey() error = %v", err)
	}
	if len(key.AllowedRoles) != 1 || key.AllowedRoles[0] != "reader" {
		t.Fatalf("allowed_roles = %v, want [reader]", key.AllowedRoles)
	}
}

func TestMultipartUploadLifecycle(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	objects, err := NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}
	metadata, err := OpenSQLite(filepath.Join(dir, "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = metadata.Close() }()
	if err := metadata.EnsureBucket(ctx, "demo"); err != nil {
		t.Fatalf("EnsureBucket() error = %v", err)
	}
	upload := core.MultipartUpload{
		UploadID:  "upload-1",
		Bucket:    "demo",
		Key:       "multipart.txt",
		CreatedAt: time.Now().UTC(),
	}
	if err := metadata.CreateMultipartUpload(ctx, upload); err != nil {
		t.Fatalf("CreateMultipartUpload() error = %v", err)
	}
	part1, err := objects.PutMultipartPart(ctx, upload.UploadID, 1, bytes.NewBufferString("hello "))
	if err != nil {
		t.Fatalf("PutMultipartPart(1) error = %v", err)
	}
	part2, err := objects.PutMultipartPart(ctx, upload.UploadID, 2, bytes.NewBufferString("world"))
	if err != nil {
		t.Fatalf("PutMultipartPart(2) error = %v", err)
	}
	if err := metadata.PutMultipartPart(ctx, part1); err != nil {
		t.Fatalf("PutMultipartPart(metadata-1) error = %v", err)
	}
	if err := metadata.PutMultipartPart(ctx, part2); err != nil {
		t.Fatalf("PutMultipartPart(metadata-2) error = %v", err)
	}
	parts, err := metadata.ListMultipartParts(ctx, upload.UploadID)
	if err != nil {
		t.Fatalf("ListMultipartParts() error = %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("ListMultipartParts() count = %d, want 2", len(parts))
	}
	meta, err := objects.CompleteMultipartUpload(ctx, "demo", "multipart.txt", []core.MultipartPart{part1, part2})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload() error = %v", err)
	}
	if err := metadata.PutObject(ctx, meta); err != nil {
		t.Fatalf("PutObject(metadata) error = %v", err)
	}
	reader, _, err := objects.OpenObject(ctx, "demo", "multipart.txt")
	if err != nil {
		t.Fatalf("OpenObject() error = %v", err)
	}
	defer func() { _ = reader.Close() }()
	buf := new(bytes.Buffer)
	if _, err := buf.ReadFrom(reader); err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}
	if got := buf.String(); got != "hello world" {
		t.Fatalf("multipart content = %q, want hello world", got)
	}
	if err := metadata.DeleteMultipartUpload(ctx, upload.UploadID); err != nil {
		t.Fatalf("DeleteMultipartUpload() error = %v", err)
	}
	_ = objects.AbortMultipartUpload(ctx, upload.UploadID)
}

func TestTrailingSlashObjectDoesNotBlockNestedObjects(t *testing.T) {
	ctx := context.Background()
	objects, err := NewFilesystemObjectStore(filepath.Join(t.TempDir(), "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	markerMeta, err := objects.PutObject(ctx, "demo", "compat/pyspark/regular/_temporary/0/", bytes.NewBufferString(""))
	if err != nil {
		t.Fatalf("PutObject(marker) error = %v", err)
	}
	if got := markerMeta.ETag; got != "d41d8cd98f00b204e9800998ecf8427e" {
		t.Fatalf("marker ETag = %q, want empty md5", got)
	}

	nestedMeta, err := objects.PutObject(ctx, "demo", "compat/pyspark/regular/_temporary/0/_temporary/file.parquet", bytes.NewBufferString("data"))
	if err != nil {
		t.Fatalf("PutObject(nested) error = %v", err)
	}
	if nestedMeta.Size != 4 {
		t.Fatalf("nested size = %d, want 4", nestedMeta.Size)
	}

	reader, _, err := objects.OpenObject(ctx, "demo", "compat/pyspark/regular/_temporary/0/")
	if err != nil {
		t.Fatalf("OpenObject(marker) error = %v", err)
	}
	defer func() { _ = reader.Close() }()
	body := new(bytes.Buffer)
	if _, err := body.ReadFrom(reader); err != nil {
		t.Fatalf("ReadFrom(marker) error = %v", err)
	}
	if got := body.String(); got != "" {
		t.Fatalf("marker body = %q, want empty", got)
	}
}

func TestListObjectsUsesLiteralPrefix(t *testing.T) {
	ctx := context.Background()
	metadata, err := OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = metadata.Close() }()
	if err := metadata.EnsureBucket(ctx, "demo"); err != nil {
		t.Fatalf("EnsureBucket() error = %v", err)
	}
	for _, key := range []string{"logs/%done.txt", "logs/_done.txt", "logs/real.txt"} {
		meta := core.ObjectMetadata{
			Bucket:     "demo",
			Key:        key,
			Path:       "/tmp/" + key,
			Size:       1,
			ETag:       "etag",
			CreatedAt:  time.Now().UTC(),
			ModifiedAt: time.Now().UTC(),
		}
		if err := metadata.PutObject(ctx, meta); err != nil {
			t.Fatalf("PutObject() error = %v", err)
		}
	}
	items, err := metadata.ListObjects(ctx, "demo", "logs/%", 100, "")
	if err != nil {
		t.Fatalf("ListObjects() error = %v", err)
	}
	if len(items) != 1 || items[0].Key != "logs/%done.txt" {
		t.Fatalf("expected literal prefix match, got %+v", items)
	}
}

func TestUpsertServiceAccountUsesClientEmailUniqueness(t *testing.T) {
	ctx := context.Background()
	metadata, err := OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = metadata.Close() }()

	first := core.ServiceAccount{
		Token:       "first-token",
		ClientEmail: "sa@mock.iam.gserviceaccount.com",
		Principal:   "first-principal",
	}
	second := core.ServiceAccount{
		Token:       "second-token",
		ClientEmail: "sa@mock.iam.gserviceaccount.com",
		Principal:   "second-principal",
	}
	if err := metadata.UpsertServiceAccount(ctx, first); err != nil {
		t.Fatalf("UpsertServiceAccount(first) error = %v", err)
	}
	if err := metadata.UpsertServiceAccount(ctx, second); err != nil {
		t.Fatalf("UpsertServiceAccount(second) error = %v", err)
	}

	sa, err := metadata.FindServiceAccountByEmail(ctx, first.ClientEmail)
	if err != nil {
		t.Fatalf("FindServiceAccountByEmail() error = %v", err)
	}
	if got, want := sa.Token, second.Token; got != want {
		t.Fatalf("token = %q, want %q", got, want)
	}
	if got, want := sa.Principal, second.Principal; got != want {
		t.Fatalf("principal = %q, want %q", got, want)
	}
}

func TestDeleteBucketRemovesEmptyBucket(t *testing.T) {
	ctx := context.Background()
	metadata, err := OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = metadata.Close() }()

	if err := metadata.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
	if err := metadata.DeleteBucket(ctx, "demo"); err != nil {
		t.Fatalf("DeleteBucket() error = %v", err)
	}
	if _, err := metadata.GetBucket(ctx, "demo"); err != core.ErrNotFound {
		t.Fatalf("GetBucket() error = %v, want %v", err, core.ErrNotFound)
	}
}

func TestDeleteBucketRejectsNonEmptyBucket(t *testing.T) {
	ctx := context.Background()
	metadata, err := OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = metadata.Close() }()

	if err := metadata.CreateBucket(ctx, "demo"); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
	if err := metadata.PutObject(ctx, core.ObjectMetadata{
		Bucket:     "demo",
		Key:        "file.txt",
		Path:       "/tmp/file.txt",
		Size:       1,
		ETag:       "etag",
		CreatedAt:  time.Now().UTC(),
		ModifiedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	if err := metadata.DeleteBucket(ctx, "demo"); err != core.ErrConflict {
		t.Fatalf("DeleteBucket() error = %v, want %v", err, core.ErrConflict)
	}
}

type noopObjectStore struct{}

func (noopObjectStore) PutObject(context.Context, string, string, ObjectSource) (core.ObjectMetadata, error) {
	return core.ObjectMetadata{}, nil
}
func (noopObjectStore) OpenObject(context.Context, string, string) (ObjectReader, core.ObjectMetadata, error) {
	return nil, core.ObjectMetadata{}, core.ErrNotFound
}
func (noopObjectStore) DeleteObject(context.Context, string, string) error { return nil }
func (noopObjectStore) PutMultipartPart(context.Context, string, int, ObjectSource) (core.MultipartPart, error) {
	return core.MultipartPart{}, nil
}
func (noopObjectStore) CompleteMultipartUpload(context.Context, string, string, []core.MultipartPart) (core.ObjectMetadata, error) {
	return core.ObjectMetadata{}, nil
}
func (noopObjectStore) AbortMultipartUpload(context.Context, string) error { return nil }
