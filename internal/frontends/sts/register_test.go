package sts

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/storage"
)

func TestHandleAssumeRoleUnsupportedActionReturnsNotFound(t *testing.T) {
	// This checks that unknown STS actions fall through to the not-found response.
	fixture := newSTSTestFixture(t)

	req := httptest.NewRequest(http.MethodGet, "/?Action=UnknownAction", nil)
	rec := httptest.NewRecorder()
	handleAction(rec, req, fixture.deps)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestHandleAssumeRoleMissingRoleReturnsNotFound(t *testing.T) {
	// This checks that AssumeRole returns not found when the requested role does not exist.
	fixture := newSTSTestFixture(t)

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
	// This checks that AssumeRole is forbidden when the caller's access key is not allowed to assume the role.
	fixture := newSTSTestFixture(t)
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
	// This checks that AssumeRole validates the required RoleSessionName parameter.
	fixture := newSTSTestFixture(t)
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
	// This checks that GetCallerIdentity reports the long-lived access key principal.
	fixture := newSTSTestFixture(t)
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
	// This checks that GetCallerIdentity reports the assumed-role session principal and user ID.
	fixture := newSTSTestFixture(t)
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
	// This checks that GetSessionToken returns a usable temporary session even when the requested duration is oversized.
	fixture := newSTSTestFixture(t)
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
