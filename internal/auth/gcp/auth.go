package gcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/httpx"
	"github.com/snithish/mockbucket/internal/iam"
)

// Authenticator resolves GCP-style credentials into a core.Subject.
type Authenticator interface {
	ResolveBearerToken(ctx context.Context, token string) (core.Subject, error)
}

// Authenticate wraps next with GCS-style request authentication.
// Accepted credential sources (checked in order):
//  1. Authorization: Bearer <token>  — resolved as a session token
//  2. access_token query parameter   — resolved as a session token
func Authenticate(resolver Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := strings.TrimSpace(r.Header.Get("Authorization"))
		accessToken := strings.TrimSpace(r.URL.Query().Get("access_token"))

		var (
			subject core.Subject
			err     error
		)
		switch {
		case strings.HasPrefix(strings.ToLower(header), "bearer "):
			token := strings.TrimSpace(header[7:])
			subject, err = resolver.ResolveBearerToken(r.Context(), token)
		case accessToken != "":
			subject, err = resolver.ResolveBearerToken(r.Context(), accessToken)
		case header != "":
			err = core.ErrInvalidArgument
		default:
			err = core.ErrUnauthenticated
		}
		if err == core.ErrNotFound {
			err = core.ErrUnauthenticated
		}
		if err != nil {
			http.Error(w, err.Error(), httpx.StatusCode(err))
			return
		}
		next.ServeHTTP(w, r.WithContext(httpx.ContextWithSubject(r.Context(), subject)))
	})
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

// TokenEndpoint returns an http.HandlerFunc that mimics Google's OAuth2 token
// endpoint for compatibility testing. It accepts two grant types and returns a
// bearer token derived from a seeded service account principal.
//
// POST /oauth2/v4/token
//
// JWT bearer assertion flow (used by service account JSON keys):
//
//	grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer
//	assertion=<jwt>   # "iss" or "sub" claim = client_email
//
// Client credentials flow (simpler testing):
//
//	grant_type=client_credentials
//	client_id=<email>
func TokenEndpoint(resolver iam.Resolver, sessionManager iam.SessionManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		fields := parseFormFields(r)
		grantType := fields["grant_type"]
		var clientEmail string

		switch grantType {
		case "urn:ietf:params:oauth:grant-type:jwt-bearer":
			clientEmail = extractEmailFromAssertion(fields["assertion"])
			if clientEmail == "" {
				http.Error(w, "invalid assertion: missing iss/sub claim", http.StatusBadRequest)
				return
			}
		case "client_credentials":
			clientEmail = strings.TrimSpace(fields["client_id"])
			if clientEmail == "" {
				http.Error(w, "client_id is required", http.StatusBadRequest)
				return
			}
		default:
			http.Error(w, "unsupported grant_type", http.StatusBadRequest)
			return
		}

		// Resolve the client email to verify it maps to a known service account.
		sa, err := resolver.Store.FindServiceAccountByEmail(r.Context(), clientEmail)
		if err != nil {
			serviceAccounts, listErr := resolver.Store.ListServiceAccounts(r.Context())
			if listErr != nil || len(serviceAccounts) != 1 {
				http.Error(w, "invalid_client", http.StatusUnauthorized)
				return
			}
			sa = serviceAccounts[0]
		}

		accessToken := sa.Token
		if accessToken == "" {
			// Fall back to issuing a dynamic session only if the seeded service
			// account does not already expose a bearer token.
			session, err := sessionManager.IssueTokenForPrincipal(r.Context(), sa.Principal)
			if err != nil {
				http.Error(w, "invalid_client: "+err.Error(), http.StatusUnauthorized)
				return
			}
			accessToken = session.Token
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(tokenResponse{
			AccessToken: accessToken,
			TokenType:   "Bearer",
			ExpiresIn:   3600,
		})
	}
}

// parseFormFields reads all fields from either JSON body or form-encoded POST data.
func parseFormFields(r *http.Request) map[string]string {
	contentType := r.Header.Get("Content-Type")
	if strings.Contains(contentType, "application/json") {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			return nil
		}
		fields := make(map[string]string, len(body))
		for k, v := range body {
			if s, ok := v.(string); ok {
				fields[k] = s
			}
		}
		return fields
	}
	_ = r.ParseForm()
	fields := make(map[string]string, len(r.Form))
	for k := range r.Form {
		fields[k] = r.FormValue(k)
	}
	return fields
}

// extractEmailFromAssertion parses a JWT's payload (without signature
// verification) and returns the "iss" or "sub" claim, which in Google's
// service account flow is the client_email.
func extractEmailFromAssertion(assertion string) string {
	parts := strings.SplitN(assertion, ".", 3)
	if len(parts) < 2 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	for _, key := range []string{"iss", "sub"} {
		if v, ok := claims[key].(string); ok && v != "" {
			return v
		}
	}
	return ""
}
