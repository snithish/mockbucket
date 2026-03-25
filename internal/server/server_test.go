package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	mbconfig "github.com/snithish/mockbucket/internal/config"
)

func TestRuntimeRegistersHealthRoutes(t *testing.T) {
	runtime := newTestRuntime(t, func(*mbconfig.Config) {})
	defer func() { _ = runtime.Close() }()
	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		res := httptest.NewRecorder()
		runtime.HTTPServer.Handler.ServeHTTP(res, req)
		if got, want := res.Code, http.StatusOK; got != want {
			t.Fatalf("%s status = %d, want %d", path, got, want)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/readyz/details", nil)
	res := httptest.NewRecorder()
	runtime.HTTPServer.Handler.ServeHTTP(res, req)
	if got, want := res.Code, http.StatusOK; got != want {
		t.Fatalf("/readyz/details status = %d, want %d", got, want)
	}
}

func TestRuntimeRejectsUnsupportedFrontends(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Frontends.Azure = true
	_, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(testWriter{t}, nil)))
	if err == nil {
		t.Fatal("New() error = nil, want unsupported frontend error")
	}
}

func newTestRuntime(t *testing.T, configure func(*mbconfig.Config)) *Runtime {
	t.Helper()
	cfg := baseConfig(t)
	configure(&cfg)
	runtime, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(testWriter{t}, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return runtime
}

func baseConfig(t *testing.T) mbconfig.Config {
	t.Helper()
	dir := t.TempDir()
	cfg := mbconfig.Default()
	cfg.Storage.RootDir = dir + "/objects"
	cfg.Storage.SQLitePath = dir + "/mockbucket.db"
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
principals:
  - name: admin
s3:
  access_keys:
    - id: admin
      secret: admin-secret
      principal: admin
`
