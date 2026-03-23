package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadResolvesRelativePaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mockbucket.yaml")
	raw := []byte("server:\n  address: 127.0.0.1:9000\nstorage:\n  root_dir: ./objects\n  sqlite_path: ./mockbucket.db\nseed:\n  path: ./seed.yaml\nauth:\n  session_duration: 30m\n")
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := cfg.Storage.RootDir, filepath.Join(dir, "objects"); got != want {
		t.Fatalf("root dir = %q, want %q", got, want)
	}
	if got, want := cfg.Storage.SQLitePath, filepath.Join(dir, "mockbucket.db"); got != want {
		t.Fatalf("sqlite path = %q, want %q", got, want)
	}
	if got, want := cfg.Seed.Path, filepath.Join(dir, "seed.yaml"); got != want {
		t.Fatalf("seed path = %q, want %q", got, want)
	}
}

func TestValidateRejectsInvalidConfig(t *testing.T) {
	cfg := Default()
	cfg.Server.Address = ""
	cfg.Auth.SessionDuration = 0
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want validation error")
	}
}
