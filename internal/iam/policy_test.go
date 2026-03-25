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
	session, err := manager.AssumeRole(ctx, "reader", "cli")
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
	if _, err := manager.AssumeRole(ctx, "nonexistent", "cli"); err == nil {
		t.Fatal("AssumeRole() error = nil, want error for unknown role")
	}
}
