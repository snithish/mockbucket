package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/snithish/mockbucket/internal/seed"
)

func TestLoadResolvesRelativePaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mockbucket.yaml")
	raw := []byte("frontends:\n  type: s3\nserver:\n  address: 127.0.0.1:9000\nstorage:\n  root_dir: ./objects\n  sqlite_path: ./mockbucket.db\n")
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
}

func TestValidateRejectsInvalidConfig(t *testing.T) {
	cfg := Default()
	cfg.Server.Address = ""
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want validation error")
	}
}

func TestValidateFrontendType(t *testing.T) {
	cases := []struct {
		name    string
		typ     FrontendType
		wantErr bool
	}{
		{name: "s3", typ: FrontendS3},
		{name: "gcs", typ: FrontendGCS},
		{name: "empty", typ: "", wantErr: true},
		{name: "invalid", typ: "invalid", wantErr: true},
	}

	for _, tt := range cases {
		cfg := Default()
		cfg.Frontends.Type = tt.typ
		err := cfg.Validate()
		if (err != nil) != tt.wantErr {
			t.Fatalf("%s: Validate() error = %v, wantErr %v", tt.name, err, tt.wantErr)
		}
	}
}

func TestValidateRejectsInvalidSeed(t *testing.T) {
	cfg := Default()
	cfg.Frontends.Type = FrontendS3
	cfg.Seed = seed.Document{
		Objects: []seed.ObjectSeed{
			{Bucket: "missing", Key: "orphan.txt", Content: "x"},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want seed validation error")
	}
}
