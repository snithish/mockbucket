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
	role := core.Role{
		Name: "reader",
		Policies: []core.PolicyDocument{{
			Statements: []core.PolicyStatement{{
				Effect:    core.EffectAllow,
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"*"},
			}},
		}},
	}
	if err := metadata.UpsertRole(ctx, role); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	session := core.Session{Token: "token", AccessKeyID: "ak", SecretKey: "sk", PrincipalName: "admin", RoleName: "reader", SessionName: "cli", CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour)}
	if err := metadata.CreateSession(ctx, session); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	stored, policies, err := metadata.GetSession(ctx, "token")
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if stored.RoleName != "reader" || len(policies) != 1 {
		t.Fatalf("unexpected session round trip: %+v policies=%d", stored, len(policies))
	}
}
