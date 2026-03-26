package sts

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/iam"
	"github.com/snithish/mockbucket/internal/storage"
)

func TestHandleAssumeRoleUnsupportedActionReturnsNotFound(t *testing.T) {
	deps, _, cleanup := newSTSTestDeps(t)
	defer cleanup()

	req := httptest.NewRequest(http.MethodGet, "/?Action=GetCallerIdentity", nil)
	rec := httptest.NewRecorder()
	handleAssumeRole(rec, req, deps)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleAssumeRoleMissingRoleReturnsNotFound(t *testing.T) {
	deps, _, cleanup := newSTSTestDeps(t)
	defer cleanup()

	req := httptest.NewRequest(
		http.MethodGet,
		"/?Action=AssumeRole&RoleArn=arn:mockbucket:iam:::role/missing&RoleSessionName=cli",
		nil,
	)
	rec := httptest.NewRecorder()
	handleAssumeRole(rec, req, deps)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleAssumeRoleAccessDeniedWhenRoleNotAllowed(t *testing.T) {
	deps, metadata, cleanup := newSTSTestDeps(t)
	defer cleanup()
	state := storage.SeedState{
		Roles: []core.Role{{Name: "reader"}},
		AccessKeys: []storage.SeedAccessKey{
			{
				ID:           "restricted",
				Secret:       "restricted-secret",
				AllowedRoles: []string{"writer"},
			},
		},
	}
	if err := metadata.ApplySeedState(context.Background(), state, nil); err != nil {
		t.Fatalf("ApplySeedState() error = %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		"/?Action=AssumeRole&RoleArn=arn:mockbucket:iam:::role/reader&RoleSessionName=cli",
		nil,
	)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=restricted/20260326/us-east-1/sts/aws4_request,SignedHeaders=host;x-amz-date,Signature=deadbeef")
	rec := httptest.NewRecorder()
	handleAssumeRole(rec, req, deps)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestHandleAssumeRoleInvalidArgumentWhenSessionNameMissing(t *testing.T) {
	deps, metadata, cleanup := newSTSTestDeps(t)
	defer cleanup()
	if err := metadata.UpsertRole(context.Background(), core.Role{Name: "reader"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		"/?Action=AssumeRole&RoleArn=arn:mockbucket:iam:::role/reader",
		nil,
	)
	rec := httptest.NewRecorder()
	handleAssumeRole(rec, req, deps)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func newSTSTestDeps(t *testing.T) (common.Dependencies, *storage.SQLiteStore, func()) {
	t.Helper()
	metadata, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	sessionManager := iam.SessionManager{
		Store:           metadata,
		DefaultDuration: time.Hour,
	}
	deps := common.Dependencies{
		SessionManager: sessionManager,
	}
	return deps, metadata, func() { _ = metadata.Close() }
}
