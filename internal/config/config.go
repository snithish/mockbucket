package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/snithish/mockbucket/internal/seed"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Server    ServerConfig   `yaml:"server"`
	Storage   StorageConfig  `yaml:"storage"`
	Seed      seed.Document  `yaml:"seed"`
	Frontends FrontendConfig `yaml:"frontends"`
}

type ServerConfig struct {
	Address         string               `yaml:"address"`
	RequestLog      bool                 `yaml:"request_log"`
	RequestCapture  RequestCaptureConfig `yaml:"request_capture"`
	ShutdownTimeout time.Duration        `yaml:"shutdown_timeout"`
}

type RequestCaptureConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

type StorageConfig struct {
	RootDir    string `yaml:"root_dir"`
	SQLitePath string `yaml:"sqlite_path"`
}

type FrontendConfig struct {
	Type FrontendType `yaml:"type"`
}

type FrontendType string

const (
	FrontendS3  FrontendType = "s3"
	FrontendGCS FrontendType = "gcs"
)

func Default() Config {
	return Config{
		Server: ServerConfig{
			Address:         "127.0.0.1:9000",
			RequestLog:      true,
			RequestCapture:  RequestCaptureConfig{Path: "./var/requests"},
			ShutdownTimeout: 10 * time.Second,
		},
		Storage: StorageConfig{
			RootDir:    "./var/objects",
			SQLitePath: "./var/mockbucket.db",
		},
	}
}

func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	return Parse(filepath.Dir(path), raw)
}

func Parse(baseDir string, raw []byte) (Config, error) {
	cfg := Default()
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	cfg.resolveRelativePaths(baseDir)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c *Config) resolveRelativePaths(baseDir string) {
	if !filepath.IsAbs(c.Storage.RootDir) {
		c.Storage.RootDir = filepath.Clean(filepath.Join(baseDir, c.Storage.RootDir))
	}
	if !filepath.IsAbs(c.Storage.SQLitePath) {
		c.Storage.SQLitePath = filepath.Clean(filepath.Join(baseDir, c.Storage.SQLitePath))
	}
	if c.Server.RequestCapture.Path != "" && !filepath.IsAbs(c.Server.RequestCapture.Path) {
		c.Server.RequestCapture.Path = filepath.Clean(filepath.Join(baseDir, c.Server.RequestCapture.Path))
	}
}

func (c Config) Validate() error {
	var problems []string
	if strings.TrimSpace(c.Server.Address) == "" {
		problems = append(problems, "server.address is required")
	}
	if strings.TrimSpace(c.Storage.RootDir) == "" {
		problems = append(problems, "storage.root_dir is required")
	}
	if strings.TrimSpace(c.Storage.SQLitePath) == "" {
		problems = append(problems, "storage.sqlite_path is required")
	}
	if c.Server.ShutdownTimeout <= 0 {
		problems = append(problems, "server.shutdown_timeout must be positive")
	}
	if c.Server.RequestCapture.Enabled && strings.TrimSpace(c.Server.RequestCapture.Path) == "" {
		problems = append(problems, "server.request_capture.path is required when request capture is enabled")
	}
	if c.Frontends.Type == "" {
		problems = append(problems, "frontends.type is required (s3, gcs)")
	}
	switch c.Frontends.Type {
	case FrontendS3, FrontendGCS:
		// valid
	default:
		problems = append(problems, "frontends.type must be one of: s3, gcs")
	}
	if err := c.Seed.Validate(); err != nil {
		problems = append(problems, fmt.Sprintf("seed: %v", err))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}
