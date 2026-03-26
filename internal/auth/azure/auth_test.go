package azure

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseSharedKeyHeader(t *testing.T) {
	account, signature, err := parseSharedKeyHeader("SharedKey testaccount:signature")
	if err != nil {
		t.Fatalf("parseSharedKeyHeader() error = %v", err)
	}
	if account != "testaccount" {
		t.Fatalf("account = %q, want testaccount", account)
	}
	if signature != "signature" {
		t.Fatalf("signature = %q, want signature", signature)
	}
}

func TestParseSharedKeyHeaderRejectsInvalidValue(t *testing.T) {
	_, _, err := parseSharedKeyHeader("Bearer token")
	if err != ErrInvalidAuthorization {
		t.Fatalf("error = %v, want %v", err, ErrInvalidAuthorization)
	}
}

func TestAuthenticateAnonymousOrSharedKeyRejectsInvalidScheme(t *testing.T) {
	resolver := NewAuthResolver([]AccountConfig{{Name: "testaccount", Key: []byte("key")}})
	handler := AuthenticateAnonymousOrSharedKey(resolver)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodHead, "/container", nil)
	req.Header.Set("Authorization", "Bearer token")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthenticateAnonymousOrSharedKeyRejectsUnknownAccount(t *testing.T) {
	resolver := NewAuthResolver([]AccountConfig{{Name: "known", Key: []byte("key")}})
	handler := AuthenticateAnonymousOrSharedKey(resolver)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/container", nil)
	req.Header.Set("Authorization", "SharedKey missing:signature")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthenticateAnonymousOrSharedKeyAnnotatesContextAccount(t *testing.T) {
	resolver := NewAuthResolver([]AccountConfig{{Name: "testaccount", Key: []byte("key")}})
	handler := AuthenticateAnonymousOrSharedKey(resolver)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		account := GetAccountFromContext(r.Context())
		if account != "testaccount" {
			t.Fatalf("account = %q, want testaccount", account)
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/container", nil)
	req.Header.Set("Authorization", "SharedKey testaccount:signature")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}
