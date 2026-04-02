package sts

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/iam"
	"github.com/snithish/mockbucket/internal/storage"
)

func TestHandleAssumeRoleUnsupportedActionReturnsNotFound(t *testing.T) {
	fixture := newSTSTestFixture(t)
	defer fixture.cleanup()

	req := httptest.NewRequest(http.MethodGet, "/?Action=UnknownAction", nil)
	rec := httptest.NewRecorder()
	handleAction(rec, req, fixture.deps)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleAssumeRoleMissingRoleReturnsNotFound(t *testing.T) {
	fixture := newSTSTestFixture(t)
	defer fixture.cleanup()

	req := httptest.NewRequest(
		http.MethodGet,
		"/?Action=AssumeRole&RoleArn=arn:mockbucket:iam:::role/missing&RoleSessionName=cli",
		nil,
	)
	rec := httptest.NewRecorder()
	handleAction(rec, req, fixture.deps)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleAssumeRoleAccessDeniedWhenRoleNotAllowed(t *testing.T) {
	fixture := newSTSTestFixture(t)
	defer fixture.cleanup()
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
	if err := fixture.metadata.ApplySeedState(context.Background(), state, nil); err != nil {
		t.Fatalf("ApplySeedState() error = %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		"/?Action=AssumeRole&RoleArn=arn:mockbucket:iam:::role/reader&RoleSessionName=cli",
		nil,
	)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=restricted/20260326/us-east-1/sts/aws4_request,SignedHeaders=host;x-amz-date,Signature=deadbeef")
	rec := httptest.NewRecorder()
	handleAction(rec, req, fixture.deps)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestHandleAssumeRoleInvalidArgumentWhenSessionNameMissing(t *testing.T) {
	fixture := newSTSTestFixture(t)
	defer fixture.cleanup()
	if err := fixture.metadata.UpsertRole(context.Background(), core.Role{Name: "reader"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}

	req := httptest.NewRequest(
		http.MethodGet,
		"/?Action=AssumeRole&RoleArn=arn:mockbucket:iam:::role/reader",
		nil,
	)
	rec := httptest.NewRecorder()
	handleAction(rec, req, fixture.deps)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestHandleGetCallerIdentityForAccessKey(t *testing.T) {
	fixture := newSTSTestFixture(t)
	defer fixture.cleanup()
	state := storage.SeedState{
		AccessKeys: []storage.SeedAccessKey{{ID: "admin", Secret: "admin-secret"}},
	}
	if err := fixture.metadata.ApplySeedState(context.Background(), state, nil); err != nil {
		t.Fatalf("ApplySeedState() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/?Action=GetCallerIdentity", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=admin/20260402/us-east-1/sts/aws4_request,SignedHeaders=host;x-amz-date,Signature=deadbeef")
	rec := httptest.NewRecorder()
	handleAction(rec, req, fixture.deps)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%q", rec.Code, http.StatusOK, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, "<Arn>arn:mockbucket:iam:::user/admin</Arn>", "<UserId>admin</UserId>") {
		t.Fatalf("body = %q, want caller identity response", body)
	}
}

func TestHandleGetCallerIdentityForSession(t *testing.T) {
	fixture := newSTSTestFixture(t)
	defer fixture.cleanup()
	if err := fixture.metadata.UpsertRole(context.Background(), core.Role{Name: "reader"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	session, err := fixture.deps.SessionManager.AssumeRole(context.Background(), "reader", "cli", "")
	if err != nil {
		t.Fatalf("AssumeRole() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/?Action=GetCallerIdentity", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+session.AccessKeyID+"/20260402/us-east-1/sts/aws4_request,SignedHeaders=host;x-amz-date,Signature=deadbeef")
	req.Header.Set("X-Amz-Security-Token", session.Token)
	rec := httptest.NewRecorder()
	handleAction(rec, req, fixture.deps)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%q", rec.Code, http.StatusOK, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, "<Arn>arn:mockbucket:sts:::assumed-role/reader/cli</Arn>", "<UserId>"+session.AccessKeyID+":cli</UserId>") {
		t.Fatalf("body = %q, want assumed-role identity response", body)
	}
}

func TestHandleGetSessionTokenIssuesBoundedSession(t *testing.T) {
	fixture := newSTSTestFixture(t)
	defer fixture.cleanup()
	state := storage.SeedState{
		AccessKeys: []storage.SeedAccessKey{{ID: "admin", Secret: "admin-secret"}},
	}
	if err := fixture.metadata.ApplySeedState(context.Background(), state, nil); err != nil {
		t.Fatalf("ApplySeedState() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/?Action=GetSessionToken&DurationSeconds=999999", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=admin/20260402/us-east-1/sts/aws4_request,SignedHeaders=host;x-amz-date,Signature=deadbeef")
	rec := httptest.NewRecorder()
	handleAction(rec, req, fixture.deps)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%q", rec.Code, http.StatusOK, rec.Body.String())
	}
	if body := rec.Body.String(); !containsAll(body, "<GetSessionTokenResponse", "<AccessKeyId>", "<SessionToken>") {
		t.Fatalf("body = %q, want session token response", body)
	}
}

func containsAll(body string, values ...string) bool {
	for _, value := range values {
		if !strings.Contains(body, value) {
			return false
		}
	}
	return true
}

type stsTestFixture struct {
	deps     common.Dependencies
	metadata *storage.SQLiteStore
}

func newSTSTestFixture(t *testing.T) stsTestFixture {
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
	return stsTestFixture{
		deps:     deps,
		metadata: metadata,
	}
}

func (fixture stsTestFixture) cleanup() {
	_ = fixture.metadata.Close()
}
