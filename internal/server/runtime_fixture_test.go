package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	mbconfig "github.com/snithish/mockbucket/internal/config"
)

func newTestRuntime(t *testing.T, configure func(*mbconfig.Config)) *Runtime {
	t.Helper()
	cfg := baseConfig(t)
	if configure != nil {
		configure(&cfg)
	}
	runtime, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(testWriter{t}, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	return runtime
}

func newHTTPTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func mustHTTPDo(t *testing.T, req *http.Request) *http.Response {
	t.Helper()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do(%s %s) error = %v", req.Method, req.URL.String(), err)
	}
	return resp
}

func mustHTTPRequest(t *testing.T, ctx context.Context, method, rawURL string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		t.Fatalf("NewRequestWithContext(%s, %q) error = %v", method, rawURL, err)
	}
	return req
}

func mustReadAll(t *testing.T, reader io.Reader) string {
	t.Helper()
	raw, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	return string(raw)
}

func baseConfig(t *testing.T) mbconfig.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := mbconfig.Default()
	cfg.Frontends.Type = mbconfig.FrontendS3
	cfg.Storage.RootDir = filepath.Join(dir, "objects")
	cfg.Storage.SQLitePath = filepath.Join(dir, "mockbucket.db")
	cfg.Server.RequestLog = false
	cfg.Server.ShutdownTimeout = time.Second
	if err := yaml.Unmarshal([]byte(defaultTestSeedYAML), &cfg.Seed); err != nil {
		t.Fatalf("parse seed: %v", err)
	}
	return cfg
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}

const defaultTestSeedYAML = `buckets:
  - demo
s3:
  access_keys:
    - id: admin
      secret: admin-secret
`
