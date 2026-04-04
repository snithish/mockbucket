package s3

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/storage"
)

type s3TestFixture struct {
	ctx      context.Context
	metadata *storage.SQLiteStore
	objects  *storage.FilesystemObjectStore
}

func newS3TestFixture(t *testing.T) s3TestFixture {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	objects, err := storage.NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}
	metadata, err := storage.OpenSQLite(filepath.Join(dir, "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = metadata.Close() })
	if err := metadata.EnsureBucket(ctx, "demo"); err != nil {
		t.Fatalf("EnsureBucket() error = %v", err)
	}
	return s3TestFixture{ctx: ctx, metadata: metadata, objects: objects}
}

func (f s3TestFixture) deps() common.Dependencies {
	return common.Dependencies{Metadata: f.metadata, Objects: f.objects}
}
