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
	Seed      SeedData       `yaml:"seed"`
	Frontends FrontendConfig `yaml:"frontends"`
	Auth      AuthConfig     `yaml:"auth"`
	Azure     AzureConfig    `yaml:"azure"`
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
	Buckets []string      `yaml:"buckets"`
	Roles   []SeedRole    `yaml:"roles"`
	Objects []SeedObject  `yaml:"objects"`
	S3      SeedS3Config  `yaml:"s3"`
	GCS     SeedGCSConfig `yaml:"gcs"`
}

type SeedRole struct {
	Name string `yaml:"name"`
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
	ID           string   `yaml:"id"`
	Secret       string   `yaml:"secret"`
	AllowedRoles []string `yaml:"allowed_roles"`
}

type SeedGCSConfig struct {
	Tokens             []GCSToken                 `yaml:"tokens"`
	ServiceCredentials []SeedGCSServiceCredential `yaml:"service_credentials"`
}

type SeedGCSServiceCredential struct {
	ClientEmail string `yaml:"client_email"`
	Principal   string `yaml:"principal"`
}

type GCSToken struct {
	Token     string `yaml:"token"`
	Principal string `yaml:"principal"`
}

type FrontendConfig struct {
	Type FrontendType `yaml:"type"`
}

type FrontendType string

const (
	FrontendS3            FrontendType = "s3"
	FrontendGCS           FrontendType = "gcs"
	FrontendAzureBlob     FrontendType = "azure_blob"
	FrontendAzureDataLake FrontendType = "azure_datalake"
)

type AzureConfig struct {
	Account   string `yaml:"account"`
	Key       string `yaml:"key"`
	DNSSuffix string `yaml:"dns_suffix"`
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
	if c.Frontends.Type == "" {
		problems = append(problems, "frontend.type is required (s3, gcs, azure_blob, azure_datalake)")
	}
	switch c.Frontends.Type {
	case FrontendS3, FrontendGCS, FrontendAzureBlob, FrontendAzureDataLake:
		// valid
	default:
		problems = append(problems, "frontend.type must be one of: s3, gcs, azure_blob, azure_datalake")
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}
