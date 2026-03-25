package gcs

import (
	"net/http"

	"github.com/snithish/mockbucket/internal/httpx"
)

// checkAuth verifies the subject is in context.
// GCS uses completely open authorization - any authenticated user authorized.
func checkAuth(r *http.Request) bool {
	_, ok := httpx.SubjectFromContext(r.Context())
	return ok
}
