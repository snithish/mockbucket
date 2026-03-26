package azure

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strings"

	"github.com/snithish/mockbucket/internal/core"
)

var (
	ErrInvalidSharedKey     = errors.New("invalid shared key signature")
	ErrInvalidAuthorization = errors.New("invalid authorization header format")
	ErrAccountNotFound      = errors.New("account not found")
)

type AccountConfig struct {
	Name string
	Key  []byte
}

type Authenticator interface {
	GetAccount(name string) (AccountConfig, bool)
}

type authResolver struct {
	accounts map[string]AccountConfig
}

func NewAuthResolver(accounts []AccountConfig) Authenticator {
	m := make(map[string]AccountConfig, len(accounts))
	for _, acc := range accounts {
		m[acc.Name] = acc
	}
	return &authResolver{accounts: m}
}

func (r *authResolver) GetAccount(name string) (AccountConfig, bool) {
	acc, ok := r.accounts[name]
	return acc, ok
}

func Authenticate(resolver Authenticator) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" && r.Method == http.MethodGet {
				next.ServeHTTP(w, r)
				return
			}

			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				http.Error(w, "Missing authorization header", http.StatusUnauthorized)
				return
			}

			account, _, err := parseSharedKeyHeader(authHeader)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			_, ok := resolver.GetAccount(account)
			if !ok {
				http.Error(w, "Account not found", http.StatusUnauthorized)
				return
			}

			if err := acceptRequestWithoutSignatureValidation(); err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(r.Context(), contextKeyAccount, account)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func parseSharedKeyHeader(header string) (account, signature string, _ error) {
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "SharedKey") {
		return "", "", ErrInvalidAuthorization
	}

	valueParts := strings.SplitN(parts[1], ":", 2)
	if len(valueParts) != 2 {
		return "", "", ErrInvalidAuthorization
	}
	return valueParts[0], valueParts[1], nil
}

// acceptRequestWithoutSignatureValidation is intentionally permissive for now:
// the account must exist, but SharedKey signatures are not yet verified.
func acceptRequestWithoutSignatureValidation() error {
	return nil
}

func buildCanonicalizedString(r *http.Request, account, date string) string {
	method := r.Method
	canonicalizedResource := buildCanonicalizedResource(r, account)

	headers := []string{
		strings.ToLower(method),
		r.Header.Get("Content-Encoding"),
		r.Header.Get("Content-Language"),
		r.Header.Get("Content-Length"),
		r.Header.Get("Content-MD5"),
		r.Header.Get("Content-Type"),
		date,
		r.Header.Get("If-Modified-Since"),
		r.Header.Get("If-Match"),
		r.Header.Get("If-None-Match"),
		r.Header.Get("If-Unmodified-Since"),
		r.Header.Get("Range"),
	}
	for i, h := range headers {
		if h == "" {
			headers[i] = "\n"
		}
	}

	result := strings.Join(headers, "\n") + "\n" + canonicalizedResource
	return result
}

func buildCanonicalizedResource(r *http.Request, account string) string {
	path := r.URL.Path
	if path == "" {
		path = "/"
	}

	if len(r.URL.RawQuery) > 0 {
		return "/" + account + parseCanonicalizedQuery(r.URL.RawQuery)
	}
	return "/" + account + path
}

func parseCanonicalizedQuery(query string) string {
	params := strings.Split(query, "&")
	var parts []string
	for _, p := range params {
		kv := strings.SplitN(p, "=", 2)
		if len(kv) == 2 {
			parts = append(parts, strings.ToLower(kv[0])+":"+kv[1])
		}
	}
	return "\n" + strings.Join(parts, "\n")
}

func computeHMAC(key []byte, message string) string {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

func GetAccountFromContext(ctx context.Context) string {
	if account := ctx.Value(contextKeyAccount); account != nil {
		return account.(string)
	}
	return ""
}

type contextKeyType int

const contextKeyAccount contextKeyType = iota

func AnonymousAuth() func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), contextKeyAccount, "anonymous")
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func AuthenticateAnonymousOrSharedKey(resolver Authenticator) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" && r.Method == http.MethodGet {
				ctx := context.WithValue(r.Context(), contextKeyAccount, "anonymous")
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				ctx := context.WithValue(r.Context(), contextKeyAccount, "anonymous")
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}

			if !strings.HasPrefix(strings.ToLower(authHeader), "sharedkey ") {
				http.Error(w, "Invalid authorization scheme", http.StatusUnauthorized)
				return
			}

			account, _, err := parseSharedKeyHeader(authHeader)
			if err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			_, ok := resolver.GetAccount(account)
			if !ok {
				http.Error(w, "Account not found", http.StatusUnauthorized)
				return
			}

			if err := acceptRequestWithoutSignatureValidation(); err != nil {
				http.Error(w, err.Error(), http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(r.Context(), contextKeyAccount, account)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func SubjectFromAccount(account string) core.Subject {
	return core.Subject{PrincipalName: account}
}
