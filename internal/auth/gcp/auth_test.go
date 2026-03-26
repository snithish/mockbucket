package gcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/snithish/mockbucket/internal/core"
)

// mockAuth implements Authenticator for testing.
type mockAuth struct {
	tokens    map[string]core.Subject
	accessKey map[string]core.Subject
}

func (m mockAuth) ResolveBearerToken(_ context.Context, token string) (core.Subject, error) {
	s, ok := m.tokens[token]
	if !ok {
		return core.Subject{}, core.ErrNotFound
	}
	return s, nil
}

func (m mockAuth) ResolveAccessKey(_ context.Context, id string) (core.Subject, error) {
	s, ok := m.accessKey[id]
	if !ok {
		return core.Subject{}, core.ErrNotFound
	}
	return s, nil
}

func newMockAuth() mockAuth {
	admin := core.Subject{PrincipalName: "admin"}
	return mockAuth{
		tokens:    map[string]core.Subject{"test-token": admin},
		accessKey: map[string]core.Subject{"admin": admin},
	}
}

func okHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func TestAuthenticate_BearerHeader(t *testing.T) {
	h := Authenticate(newMockAuth(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestAuthenticate_AccessTokenQueryParam(t *testing.T) {
	h := Authenticate(newMockAuth(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet, "/?access_token=test-token", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestAuthenticate_AccessKeyHeader(t *testing.T) {
	h := Authenticate(newMockAuth(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Mockbucket-Access-Key", "admin")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestAuthenticate_NoCredentials(t *testing.T) {
	h := Authenticate(newMockAuth(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthenticate_InvalidToken(t *testing.T) {
	h := Authenticate(newMockAuth(), http.HandlerFunc(okHandler))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer bad-token")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestExtractEmailFromAssertion(t *testing.T) {
	claims := map[string]string{"iss": "sa@mock.iam.gserviceaccount.com"}
	claimsJSON, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)
	assertion := "eyJhbGciOiJSUzI1NiJ9." + payload + ".fakesig"

	got := extractEmailFromAssertion(assertion)
	if got != "sa@mock.iam.gserviceaccount.com" {
		t.Fatalf("extractEmailFromAssertion() = %q, want %q", got, "sa@mock.iam.gserviceaccount.com")
	}
}

func TestExtractEmailFromAssertion_SubClaim(t *testing.T) {
	claims := map[string]string{"sub": "sa@mock.iam.gserviceaccount.com"}
	claimsJSON, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(claimsJSON)
	assertion := "eyJhbGciOiJSUzI1NiJ9." + payload + ".fakesig"

	got := extractEmailFromAssertion(assertion)
	if got != "sa@mock.iam.gserviceaccount.com" {
		t.Fatalf("extractEmailFromAssertion() = %q, want %q", got, "sa@mock.iam.gserviceaccount.com")
	}
}

func TestExtractEmailFromAssertion_Invalid(t *testing.T) {
	tests := []string{
		"",
		"not-a-jwt",
		"aaa.bbb",
		"aaa." + base64.RawURLEncoding.EncodeToString([]byte("{}")) + ".ccc",
	}
	for _, assertion := range tests {
		got := extractEmailFromAssertion(assertion)
		if got != "" {
			t.Fatalf("extractEmailFromAssertion(%q) = %q, want empty", assertion, got)
		}
	}
}

func TestParseFormFields_URLEncoded(t *testing.T) {
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", "sa@test.com")
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	fields := parseFormFields(req)
	if got := fields["grant_type"]; got != "client_credentials" {
		t.Fatalf("grant_type = %q, want client_credentials", got)
	}
	if got := fields["client_id"]; got != "sa@test.com" {
		t.Fatalf("client_id = %q, want sa@test.com", got)
	}
}

func TestParseFormFields_JSON(t *testing.T) {
	body := `{"grant_type":"client_credentials","client_id":"sa@test.com"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	fields := parseFormFields(req)
	if got := fields["grant_type"]; got != "client_credentials" {
		t.Fatalf("grant_type = %q, want client_credentials", got)
	}
	if got := fields["client_id"]; got != "sa@test.com" {
		t.Fatalf("client_id = %q, want sa@test.com", got)
	}
}
