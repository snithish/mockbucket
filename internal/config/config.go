package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server    ServerConfig   `yaml:"server"`
	Storage   StorageConfig  `yaml:"storage"`
	Seed      SeedConfig     `yaml:"seed"`
	Frontends FrontendConfig `yaml:"frontends"`
	Auth      AuthConfig     `yaml:"auth"`
}

type ServerConfig struct {
	Address         string        `yaml:"address"`
	RequestLog      bool          `yaml:"request_log"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type StorageConfig struct {
	RootDir    string `yaml:"root_dir"`
	SQLitePath string `yaml:"sqlite_path"`
}

type SeedConfig struct {
	Path string `yaml:"path"`
}

type FrontendConfig struct {
	S3    bool `yaml:"s3"`
	STS   bool `yaml:"sts"`
	GCS   bool `yaml:"gcs"`
	Azure bool `yaml:"azure"`
}

type AuthConfig struct {
	SessionDuration time.Duration `yaml:"session_duration"`
}

func Default() Config {
	return Config{
		Server: ServerConfig{
			Address:         "127.0.0.1:9000",
			RequestLog:      true,
			ShutdownTimeout: 10 * time.Second,
		},
		Storage: StorageConfig{
			RootDir:    "./var/objects",
			SQLitePath: "./var/mockbucket.db",
		},
		Seed: SeedConfig{Path: "./seed.yaml"},
		Auth: AuthConfig{SessionDuration: time.Hour},
	}
}

func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	cfg := Default()
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	cfg.resolveRelativePaths(filepath.Dir(path))
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
	if c.Seed.Path != "" && !filepath.IsAbs(c.Seed.Path) {
		c.Seed.Path = filepath.Clean(filepath.Join(baseDir, c.Seed.Path))
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
	if c.Auth.SessionDuration <= 0 {
		problems = append(problems, "auth.session_duration must be positive")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}
