package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/snithish/mockbucket/internal/core"
)

type Config struct {
	Server    ServerConfig   `yaml:"server"`
	Storage   StorageConfig  `yaml:"storage"`
	Seed      SeedData       `yaml:"seed"`
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

type SeedData struct {
	Buckets    []string         `yaml:"buckets"`
	Principals []core.Principal `yaml:"principals"`
	Roles      []core.Role      `yaml:"roles"`
	Objects    []SeedObject     `yaml:"objects"`
	S3         SeedS3Config     `yaml:"s3"`
	GCS        SeedGCSConfig    `yaml:"gcs"`
}

type SeedObject struct {
	Bucket  string `yaml:"bucket"`
	Key     string `yaml:"key"`
	Content string `yaml:"content"`
}

type SeedS3Config struct {
	AccessKeys []SeedS3AccessKey `yaml:"access_keys"`
}

type SeedS3AccessKey struct {
	ID        string `yaml:"id"`
	Secret    string `yaml:"secret"`
	Principal string `yaml:"principal"`
}

type SeedGCSConfig struct {
	Accounts []core.ServiceAccount `yaml:"accounts"`
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
		Auth: AuthConfig{SessionDuration: time.Hour},
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
