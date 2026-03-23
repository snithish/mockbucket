package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/iam"
)

type contextKey string

const (
	requestIDKey contextKey = "request_id"
	subjectKey   contextKey = "subject"
)

type Authenticator interface {
	ResolveAccessKey(ctx context.Context, accessKeyID string) (core.Subject, error)
	ResolveBearerToken(ctx context.Context, token string) (core.Subject, error)
}

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-Id")
		if requestID == "" {
			requestID = mustRandomID()
		}
		w.Header().Set("X-Request-Id", requestID)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, requestID)))
	})
}

func RequestLog(logger *slog.Logger, enabled bool, next http.Handler) http.Handler {
	if !enabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(recorder, r)
		logger.Info("http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("request_id", RequestIDFromContext(r.Context())),
			slog.Int("status", recorder.status),
			slog.Int64("bytes_written", recorder.bytes),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

func Authenticate(auth Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		header := strings.TrimSpace(r.Header.Get("Authorization"))
		accessKey := strings.TrimSpace(r.Header.Get("X-Mockbucket-Access-Key"))
		var (
			subject core.Subject
			err     error
		)
		switch {
		case strings.HasPrefix(strings.ToLower(header), "bearer "):
			subject, err = auth.ResolveBearerToken(r.Context(), strings.TrimSpace(header[7:]))
		case accessKey != "":
			subject, err = auth.ResolveAccessKey(r.Context(), accessKey)
		case header != "":
			err = core.ErrInvalidArgument
		default:
			err = core.ErrUnauthenticated
		}
		if err != nil {
			writeError(w, err)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), subjectKey, subject)))
	})
}

func Authorize(evaluator iam.Evaluator, action, resource string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		subject, ok := SubjectFromContext(r.Context())
		if !ok {
			writeError(w, core.ErrUnauthenticated)
			return
		}
		resolvedResource := strings.ReplaceAll(resource, "{bucket}", r.PathValue("bucket"))
		resolvedResource = strings.ReplaceAll(resolvedResource, "{key}", r.PathValue("key"))
		if !evaluator.Allowed(action, resolvedResource, subject.Policies) {
			writeError(w, core.ErrAccessDenied)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func RequestIDFromContext(ctx context.Context) string {
	requestID, _ := ctx.Value(requestIDKey).(string)
	return requestID
}

func SubjectFromContext(ctx context.Context) (core.Subject, bool) {
	subject, ok := ctx.Value(subjectKey).(core.Subject)
	return subject, ok
}

func ContextWithSubject(ctx context.Context, subject core.Subject) context.Context {
	return context.WithValue(ctx, subjectKey, subject)
}

func writeError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), StatusCode(err))
}

func mustRandomID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return "request-id-error"
	}
	return hex.EncodeToString(buf)
}

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.status = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}

func (r *responseRecorder) Write(p []byte) (int, error) {
	n, err := r.ResponseWriter.Write(p)
	r.bytes += int64(n)
	return n, err
}
