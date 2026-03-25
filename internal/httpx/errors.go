package httpx

import (
	"errors"
	"net/http"

	"github.com/snithish/mockbucket/internal/core"
)

func StatusCode(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, core.ErrUnauthenticated):
		return http.StatusUnauthorized
	case errors.Is(err, core.ErrAccessDenied):
		return http.StatusForbidden
	case errors.Is(err, core.ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, core.ErrInvalidArgument):
		return http.StatusBadRequest
	case errors.Is(err, core.ErrExpiredToken):
		return http.StatusUnauthorized
	case errors.Is(err, core.ErrConflict):
		return http.StatusConflict
	case errors.Is(err, core.ErrUnsupported):
		return http.StatusNotImplemented
	default:
		return http.StatusInternalServerError
	}
}
