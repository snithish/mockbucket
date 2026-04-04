package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	mbconfig "github.com/snithish/mockbucket/internal/config"
)

func TestRuntimeRegistersHealthRoutes(t *testing.T) {
	// This checks that the runtime always exposes the health and readiness endpoints.
	runtime := newTestRuntime(t, func(*mbconfig.Config) {})
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

func TestParseServerAddress(t *testing.T) {
	// This checks that server addresses resolve to a concrete host and numeric port.
	tests := []struct {
		addr     string
		wantHost string
		wantPort int
		wantErr  bool
	}{
		{addr: "127.0.0.1:9000", wantHost: "127.0.0.1", wantPort: 9000},
		{addr: ":9000", wantHost: "127.0.0.1", wantPort: 9000},
		{addr: "[::1]:9000", wantHost: "::1", wantPort: 9000},
		{addr: "127.0.0.1", wantErr: true},
	}
	for _, tt := range tests {
		host, port, err := parseServerAddress(tt.addr)
		if (err != nil) != tt.wantErr {
			t.Fatalf("parseServerAddress(%q) error = %v, wantErr = %v", tt.addr, err, tt.wantErr)
		}
		if tt.wantErr {
			continue
		}
		if host != tt.wantHost || port != tt.wantPort {
			t.Fatalf("parseServerAddress(%q) = (%q, %d), want (%q, %d)", tt.addr, host, port, tt.wantHost, tt.wantPort)
		}
	}
}

func TestRuntimeCapturesRequestsWhenEnabled(t *testing.T) {
	// This checks that request capture persists the raw HTTP payload when the feature is enabled.
	runtime := newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Server.RequestCapture.Enabled = true
		cfg.Server.RequestCapture.Path = filepath.Join(t.TempDir(), "requests")
	})

	req := httptest.NewRequest(http.MethodPost, "/?Action=GetCallerIdentity", strings.NewReader("Action=GetCallerIdentity"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res := httptest.NewRecorder()
	runtime.HTTPServer.Handler.ServeHTTP(res, req)

	files, err := filepath.Glob(filepath.Join(runtime.Config.Server.RequestCapture.Path, "*.http"))
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
	if got := string(raw); !strings.Contains(got, "\r\nAction=GetCallerIdentity") {
		t.Fatalf("capture body missing from %q", got)
	}
}
