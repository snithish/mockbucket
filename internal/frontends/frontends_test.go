package frontends

import (
	"testing"

	"github.com/snithish/mockbucket/internal/config"
)

func TestValidateFrontends(t *testing.T) {
	cases := []struct {
		name    string
		config  config.Config
		wantErr bool
	}{
		{name: "s3 only", config: config.Config{Frontends: config.FrontendConfig{S3: true}}},
		{name: "gcs only", config: config.Config{Frontends: config.FrontendConfig{GCS: true}}},
		{name: "azure blocked", config: config.Config{Frontends: config.FrontendConfig{Azure: true}}, wantErr: true},
		{name: "s3+gcs invalid", config: config.Config{Frontends: config.FrontendConfig{S3: true, GCS: true}}, wantErr: true},
	}
	for _, tt := range cases {
		if err := Validate(tt.config); (err != nil) != tt.wantErr {
			t.Fatalf("%s: Validate() err=%v wantErr=%v", tt.name, err, tt.wantErr)
		}
	}
}
