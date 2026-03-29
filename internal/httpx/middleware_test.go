package httpx

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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

func TestRequestCaptureWritesHTTPRequestFile(t *testing.T) {
	dir := t.TempDir()
	logger := slog.New(slog.NewTextHandler(ioDiscard{}, nil))
	handler, err := RequestCapture(logger, true, dir, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			t.Fatalf("ReadAll() error = %v", readErr)
		}
		if got, want := string(body), "hello world"; got != want {
			t.Fatalf("body = %q, want %q", got, want)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	if err != nil {
		t.Fatalf("RequestCapture() error = %v", err)
	}
	handler = RequestID(handler)

	req := httptest.NewRequest(http.MethodPost, "http://example.com/upload?part=1", strings.NewReader("hello world"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Test", "value")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)

	files, err := filepath.Glob(filepath.Join(dir, "*.http"))
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if got, want := len(files), 1; got != want {
		t.Fatalf("capture files = %d, want %d", got, want)
	}
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	text := string(raw)
	for _, needle := range []string{
		"POST /upload?part=1 HTTP/1.1\r\n",
		"Host: example.com\r\n",
		"Content-Length: 11\r\n",
		"Content-Type: text/plain\r\n",
		"X-Test: value\r\n",
		"\r\nhello world",
	} {
		if !strings.Contains(text, needle) {
			t.Fatalf("capture missing %q in %q", needle, text)
		}
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}
