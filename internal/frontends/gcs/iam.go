package gcs

import (
	"net/http"

	"github.com/snithish/mockbucket/internal/httpx"
)

// hasAuthenticatedSubject verifies that request auth resolved a subject.
// GCS currently gates access on authenticated identity only.
func hasAuthenticatedSubject(r *http.Request) bool {
	_, ok := httpx.SubjectFromContext(r.Context())
	return ok
}
