package httpx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/snithish/mockbucket/internal/core"
)

type contextKey string

const (
	requestIDKey contextKey = "request_id"
	subjectKey   contextKey = "subject"
)

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

func RequestCapture(logger *slog.Logger, enabled bool, dir string, next http.Handler) (http.Handler, error) {
	if !enabled {
		return next, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create request capture dir: %w", err)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := RequestIDFromContext(r.Context())
		path := filepath.Join(dir, captureFileName(time.Now().UTC(), requestID, r.Method))
		capture, err := newRequestCapture(logger, path, requestID, r)
		if err != nil {
			logger.Error("open request capture", slog.String("path", path), slog.String("request_id", requestID), slog.Any("error", err))
			next.ServeHTTP(w, r)
			return
		}
		defer capture.Close()

		r.Body = capture.body
		next.ServeHTTP(w, r)
		if _, err := io.Copy(io.Discard, capture.body); err != nil && err != io.EOF {
			logger.Error("drain captured request body", slog.String("path", path), slog.String("request_id", requestID), slog.Any("error", err))
		}
	}), nil
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

type requestCapture struct {
	file *os.File
	body *captureReadCloser
}

func newRequestCapture(logger *slog.Logger, path string, requestID string, r *http.Request) (*requestCapture, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	if err := writeCapturedRequest(file, r); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &requestCapture{
		file: file,
		body: &captureReadCloser{
			ReadCloser: r.Body,
			file:       file,
			logger:     logger,
			path:       path,
			requestID:  requestID,
		},
	}, nil
}

func (c *requestCapture) Close() error {
	var firstErr error
	if c.body != nil {
		if err := c.body.Close(); err != nil {
			firstErr = err
		}
	}
	if c.file != nil {
		if err := c.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.file = nil
	}
	return firstErr
}

type captureReadCloser struct {
	io.ReadCloser
	file      *os.File
	logger    *slog.Logger
	path      string
	requestID string
	closed    bool
}

func (c *captureReadCloser) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	if n > 0 && c.file != nil {
		if _, writeErr := c.file.Write(p[:n]); writeErr != nil {
			c.logger.Error("write request capture body", slog.String("path", c.path), slog.String("request_id", c.requestID), slog.Any("error", writeErr))
			_ = c.file.Close()
			c.file = nil
		}
	}
	return n, err
}

func (c *captureReadCloser) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.ReadCloser.Close()
}

func writeCapturedRequest(w io.Writer, r *http.Request) error {
	requestURI := r.RequestURI
	if requestURI == "" || strings.HasPrefix(requestURI, "http://") || strings.HasPrefix(requestURI, "https://") {
		requestURI = r.URL.RequestURI()
	}
	if _, err := fmt.Fprintf(w, "%s %s %s\r\n", r.Method, requestURI, r.Proto); err != nil {
		return err
	}
	if r.Host != "" {
		if _, err := fmt.Fprintf(w, "Host: %s\r\n", r.Host); err != nil {
			return err
		}
	}
	if r.ContentLength >= 0 && r.Header.Get("Content-Length") == "" {
		if _, err := fmt.Fprintf(w, "Content-Length: %d\r\n", r.ContentLength); err != nil {
			return err
		}
	}
	if len(r.TransferEncoding) > 0 && r.Header.Get("Transfer-Encoding") == "" {
		if _, err := fmt.Fprintf(w, "Transfer-Encoding: %s\r\n", strings.Join(r.TransferEncoding, ", ")); err != nil {
			return err
		}
	}

	keys := make([]string, 0, len(r.Header))
	for key := range r.Header {
		if strings.EqualFold(key, "Host") || strings.EqualFold(key, "Content-Length") || strings.EqualFold(key, "Transfer-Encoding") {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		for _, value := range r.Header.Values(key) {
			if _, err := fmt.Fprintf(w, "%s: %s\r\n", key, value); err != nil {
				return err
			}
		}
	}
	_, err := io.WriteString(w, "\r\n")
	return err
}

func captureFileName(ts time.Time, requestID string, method string) string {
	return fmt.Sprintf("%s-%s-%s.http",
		ts.Format("20060102T150405.000000000Z"),
		sanitizeFilePart(requestID),
		sanitizeFilePart(strings.ToLower(method)),
	)
}

func sanitizeFilePart(value string) string {
	if value == "" {
		return "unknown"
	}
	var builder strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '-', r == '_':
			builder.WriteRune(r)
		default:
			builder.WriteByte('_')
		}
	}
	if builder.Len() == 0 {
		return "unknown"
	}
	return builder.String()
}
