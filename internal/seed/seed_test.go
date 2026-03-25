package seed

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/storage"
)

func TestValidateRejectsUnknownReferences(t *testing.T) {
	doc := Document{
		Buckets:    []string{"demo"},
		Principals: []core.Principal{{Name: "admin"}},
		Roles:      []core.Role{{Name: "reader"}},
		Objects:    []ObjectSeed{{Bucket: "missing", Key: "object.txt", Content: "x"}},
	}
	if err := doc.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
}

func TestApplyRollsBackOnFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metadata, err := storage.OpenSQLite(filepath.Join(dir, "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = metadata.Close() }()
	objects, err := storage.NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}
	failingObjects := &failingObjectStore{ObjectStore: objects, failAfter: 1}

	doc := Document{
		Buckets:    []string{"demo"},
		Principals: []core.Principal{{Name: "admin"}},
		S3:         S3SeedConfig{AccessKeys: []S3AccessKeySeed{{ID: "admin", Secret: "secret", Principal: "admin"}}},
		Roles:      []core.Role{{Name: "reader"}},
		Objects: []ObjectSeed{
			{Bucket: "demo", Key: "a.txt", Content: "a"},
			{Bucket: "demo", Key: "b.txt", Content: "b"},
		},
	}

	if err := Apply(ctx, doc, metadata, failingObjects); err == nil {
		t.Fatal("Apply() error = nil, want error")
	}

	buckets, err := metadata.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets() error = %v", err)
	}
	if len(buckets) != 0 {
		t.Fatalf("expected zero buckets after rollback, got %d", len(buckets))
	}
	principals, err := metadata.ListPrincipals(ctx)
	if err != nil {
		t.Fatalf("ListPrincipals() error = %v", err)
	}
	if len(principals) != 0 {
		t.Fatalf("expected zero principals after rollback, got %d", len(principals))
	}
	roles, err := metadata.ListRoles(ctx)
	if err != nil {
		t.Fatalf("ListRoles() error = %v", err)
	}
	if len(roles) != 0 {
		t.Fatalf("expected zero roles after rollback, got %d", len(roles))
	}
	keys, err := metadata.ListAccessKeys(ctx)
	if err != nil {
		t.Fatalf("ListAccessKeys() error = %v", err)
	}
	if len(keys) != 0 {
		t.Fatalf("expected zero access keys after rollback, got %d", len(keys))
	}
	if _, _, err := objects.OpenObject(ctx, "demo", "a.txt"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expected object rollback, got %v", err)
	}
}

func TestApplyReconcilesIdentityState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metadata, err := storage.OpenSQLite(filepath.Join(dir, "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = metadata.Close() }()
	objects, err := storage.NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}

	if err := metadata.UpsertPrincipal(ctx, core.Principal{Name: "old"}); err != nil {
		t.Fatalf("UpsertPrincipal() error = %v", err)
	}
	if err := metadata.UpsertRole(ctx, core.Role{Name: "old-role"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}

	doc := Document{
		Principals: []core.Principal{{Name: "admin"}},
		S3:         S3SeedConfig{AccessKeys: []S3AccessKeySeed{{ID: "admin", Secret: "admin-secret", Principal: "admin"}}},
		Roles:      []core.Role{{Name: "reader"}},
	}

	if err := Apply(ctx, doc, metadata, objects); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	principals, err := metadata.ListPrincipals(ctx)
	if err != nil {
		t.Fatalf("ListPrincipals() error = %v", err)
	}
	if len(principals) != 1 || principals[0] != "admin" {
		t.Fatalf("expected only admin principal, got %v", principals)
	}
	roles, err := metadata.ListRoles(ctx)
	if err != nil {
		t.Fatalf("ListRoles() error = %v", err)
	}
	if len(roles) != 1 || roles[0] != "reader" {
		t.Fatalf("expected only reader role, got %v", roles)
	}
	keys, err := metadata.ListAccessKeys(ctx)
	if err != nil {
		t.Fatalf("ListAccessKeys() error = %v", err)
	}
	if len(keys) != 1 || keys[0].ID != "admin" {
		t.Fatalf("expected only admin key, got %v", keys)
	}
}

type failingObjectStore struct {
	storage.ObjectStore
	failAfter int
	seen      int
}

func (f *failingObjectStore) PutObject(ctx context.Context, bucket, key string, src storage.ObjectSource) (core.ObjectMetadata, error) {
	f.seen++
	if f.failAfter > 0 && f.seen > f.failAfter {
		return core.ObjectMetadata{}, errors.New("injected failure")
	}
	return f.ObjectStore.PutObject(ctx, bucket, key, src)
}

func (f *failingObjectStore) PutMultipartPart(ctx context.Context, uploadID string, partNumber int, src storage.ObjectSource) (core.MultipartPart, error) {
	return f.ObjectStore.PutMultipartPart(ctx, uploadID, partNumber, src)
}

func (f *failingObjectStore) CompleteMultipartUpload(ctx context.Context, bucket, key string, parts []core.MultipartPart) (core.ObjectMetadata, error) {
	return f.ObjectStore.CompleteMultipartUpload(ctx, bucket, key, parts)
}

func (f *failingObjectStore) AbortMultipartUpload(ctx context.Context, uploadID string) error {
	return f.ObjectStore.AbortMultipartUpload(ctx, uploadID)
}

func (f *failingObjectStore) OpenObject(ctx context.Context, bucket, key string) (storage.ObjectReader, core.ObjectMetadata, error) {
	return f.ObjectStore.OpenObject(ctx, bucket, key)
}

func (f *failingObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	return f.ObjectStore.DeleteObject(ctx, bucket, key)
}
