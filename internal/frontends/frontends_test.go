package frontends

import (
	"testing"

	"github.com/snithish/mockbucket/internal/config"
)

func TestValidateFrontends(t *testing.T) {
	cases := []struct {
		name    string
		cfg     config.Config
		wantErr bool
	}{
		{name: "s3 only", cfg: config.Config{Frontends: config.FrontendConfig{Type: config.FrontendS3}}},
		{name: "gcs only", cfg: config.Config{Frontends: config.FrontendConfig{Type: config.FrontendGCS}}},
		{name: "azure_blob only", cfg: config.Config{Frontends: config.FrontendConfig{Type: config.FrontendAzureBlob}}},
		{name: "azure_datalake only", cfg: config.Config{Frontends: config.FrontendConfig{Type: config.FrontendAzureDataLake}}},
		{name: "invalid empty", cfg: config.Config{Frontends: config.FrontendConfig{}}, wantErr: true},
		{name: "invalid type", cfg: config.Config{Frontends: config.FrontendConfig{Type: "invalid"}}, wantErr: true},
	}
	for _, tt := range cases {
		if err := Validate(tt.cfg); (err != nil) != tt.wantErr {
			t.Fatalf("%s: Validate() err=%v wantErr=%v", tt.name, err, tt.wantErr)
		}
	}
}
