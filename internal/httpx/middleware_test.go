package httpx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/iam"
)

type fakeAuth struct {
	subject core.Subject
	err     error
}

func (f fakeAuth) ResolveAccessKey(context.Context, string) (core.Subject, error) {
	return f.subject, f.err
}
func (f fakeAuth) ResolveBearerToken(context.Context, string) (core.Subject, error) {
	return f.subject, f.err
}

func TestRequestIDSetsHeader(t *testing.T) {
	handler := RequestID(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if RequestIDFromContext(r.Context()) == "" {
			t.Fatal("request id missing from context")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if got := res.Header().Get("X-Request-Id"); got == "" {
		t.Fatal("X-Request-Id header missing")
	}
}

func TestAuthenticateAndAuthorize(t *testing.T) {
	evaluator := iam.Evaluator{}
	subject := core.Subject{
		PrincipalName: "admin",
		Policies: []core.PolicyDocument{{
			Statements: []core.PolicyStatement{{
				Effect:    core.EffectAllow,
				Actions:   []string{"s3:GetObject"},
				Resources: []string{"arn:mockbucket:s3:::demo/*"},
			}},
		}},
	}
	handler := Authenticate(fakeAuth{subject: subject}, Authorize(evaluator, "s3:GetObject", "arn:mockbucket:s3:::{bucket}/{key}", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))
	req := httptest.NewRequest(http.MethodGet, "/b/demo/o/file.txt", nil)
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "file.txt")
	req.Header.Set("X-Mockbucket-Access-Key", "admin")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if got, want := res.Code, http.StatusNoContent; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
}

func TestAuthenticateRejectsMalformedAuthorization(t *testing.T) {
	handler := Authenticate(fakeAuth{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Basic nope")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if got, want := res.Code, http.StatusBadRequest; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
}
