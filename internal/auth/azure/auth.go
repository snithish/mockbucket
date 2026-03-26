package azure

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/snithish/mockbucket/internal/core"
)

var (
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

func GetAccountFromContext(ctx context.Context) string {
	if account := ctx.Value(contextKeyAccount); account != nil {
		return account.(string)
	}
	return ""
}

type contextKeyType int

const contextKeyAccount contextKeyType = iota

// AuthenticateAnonymousOrSharedKey is intentionally account-aware, not
// signature-validating: when a SharedKey header is present, only account name
// parsing and account existence checks are enforced.
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

			if _, ok := resolver.GetAccount(account); !ok {
				http.Error(w, "Account not found", http.StatusUnauthorized)
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
