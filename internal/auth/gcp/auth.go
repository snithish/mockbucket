package gcp

import (
	"net/http"
	"strings"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/httpx"
	"github.com/snithish/mockbucket/internal/iam"
)

func Authenticate(resolver iam.Resolver, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := strings.TrimSpace(r.Header.Get("Authorization"))
		accessKey := strings.TrimSpace(r.Header.Get("X-Mockbucket-Access-Key"))
		var (
			subject core.Subject
			err     error
		)
		switch {
		case strings.HasPrefix(strings.ToLower(header), "bearer "):
			token := strings.TrimSpace(header[7:])
			subject, err = resolver.ResolveBearerToken(r.Context(), token)
		case accessKey != "":
			subject, err = resolver.ResolveAccessKey(r.Context(), accessKey)
		case header != "":
			err = core.ErrInvalidArgument
		default:
			err = core.ErrUnauthenticated
		}
		if err != nil {
			http.Error(w, err.Error(), httpx.StatusCode(err))
			return
		}
		next.ServeHTTP(w, r.WithContext(httpx.ContextWithSubject(r.Context(), subject)))
	})
}
