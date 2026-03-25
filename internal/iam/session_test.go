package iam

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/storage"
)

func TestSessionManagerAssumeRole(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.UpsertRole(ctx, core.Role{Name: "reader"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	manager := SessionManager{Store: store, DefaultDuration: time.Hour}
	session, err := manager.AssumeRole(ctx, "reader", "cli", "")
	if err != nil {
		t.Fatalf("AssumeRole() error = %v", err)
	}
	subject, err := manager.ResolveSession(ctx, session.Token)
	if err != nil {
		t.Fatalf("ResolveSession() error = %v", err)
	}
	if subject.RoleName != "reader" {
		t.Fatalf("role name = %q, want reader", subject.RoleName)
	}
}

func TestSessionManagerAssumeRoleRejectsUnknownRole(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = store.Close() }()
	manager := SessionManager{Store: store, DefaultDuration: time.Hour}
	if _, err := manager.AssumeRole(ctx, "nonexistent", "cli", ""); err == nil {
		t.Fatal("AssumeRole() error = nil, want error for unknown role")
	}
}

func TestSessionManagerAssumeRoleHonorsAllowedRoles(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.UpsertRole(ctx, core.Role{Name: "admin"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	if err := store.UpsertRole(ctx, core.Role{Name: "reader"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	if _, err := store.FindAccessKey(ctx, "restricted"); err == nil {
		t.Fatal("expected access key to not exist yet")
	}

	// Seed an access key with allowed_roles = ["reader"]
	state := storage.SeedState{
		Roles: []core.Role{{Name: "admin"}, {Name: "reader"}},
		AccessKeys: []storage.SeedAccessKey{
			{ID: "restricted", Secret: "s", AllowedRoles: []string{"reader"}},
		},
	}
	if err := store.ApplySeedState(ctx, state, &noopObjectStore{}); err != nil {
		t.Fatalf("ApplySeedState() error = %v", err)
	}

	manager := SessionManager{Store: store, DefaultDuration: time.Hour}

	// Should succeed: reader is in allowed_roles
	if _, err := manager.AssumeRole(ctx, "reader", "cli", "restricted"); err != nil {
		t.Fatalf("AssumeRole(reader) error = %v, want nil", err)
	}

	// Should fail: admin is not in allowed_roles
	if _, err := manager.AssumeRole(ctx, "admin", "cli", "restricted"); err == nil {
		t.Fatal("AssumeRole(admin) error = nil, want access denied")
	}
}

func TestSessionManagerAssumeRoleOpenWhenNoAllowedRoles(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.UpsertRole(ctx, core.Role{Name: "any-role"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}

	// Seed an access key with empty allowed_roles (open)
	state := storage.SeedState{
		Roles:      []core.Role{{Name: "any-role"}},
		AccessKeys: []storage.SeedAccessKey{{ID: "open", Secret: "s", AllowedRoles: nil}},
	}
	if err := store.ApplySeedState(ctx, state, &noopObjectStore{}); err != nil {
		t.Fatalf("ApplySeedState() error = %v", err)
	}

	manager := SessionManager{Store: store, DefaultDuration: time.Hour}
	if _, err := manager.AssumeRole(ctx, "any-role", "cli", "open"); err != nil {
		t.Fatalf("AssumeRole() error = %v, want nil (empty allowed_roles = open)", err)
	}
}

type noopObjectStore struct{}

func (noopObjectStore) PutObject(context.Context, string, string, storage.ObjectSource) (core.ObjectMetadata, error) {
	return core.ObjectMetadata{}, nil
}
func (noopObjectStore) OpenObject(context.Context, string, string) (storage.ObjectReader, core.ObjectMetadata, error) {
	return nil, core.ObjectMetadata{}, core.ErrNotFound
}
func (noopObjectStore) DeleteObject(context.Context, string, string) error { return nil }
func (noopObjectStore) PutMultipartPart(context.Context, string, int, storage.ObjectSource) (core.MultipartPart, error) {
	return core.MultipartPart{}, nil
}
func (noopObjectStore) CompleteMultipartUpload(context.Context, string, string, []core.MultipartPart) (core.ObjectMetadata, error) {
	return core.ObjectMetadata{}, nil
}
func (noopObjectStore) AbortMultipartUpload(context.Context, string) error { return nil }
