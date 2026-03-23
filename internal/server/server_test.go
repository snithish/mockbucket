package server

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	seedPath := filepath.Join(dir, "seed.yaml")
	if err := osWriteFile(seedPath, []byte(defaultTestSeedYAML)); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	cfg := mbconfig.Default()
	cfg.Storage.RootDir = filepath.Join(dir, "objects")
	cfg.Storage.SQLitePath = filepath.Join(dir, "mockbucket.db")
	cfg.Seed.Path = seedPath
	cfg.Server.RequestLog = false
	cfg.Server.ShutdownTimeout = time.Second
	return cfg
}

func osWriteFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}

const defaultTestSeedYAML = `buckets:
  - demo
principals:
  - name: admin
    policies:
      - statements:
          - effect: Allow
            actions: ["*"]
            resources: ["*"]
    access_keys:
      - id: admin
        secret: admin-secret
`

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
