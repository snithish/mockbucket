package frontends

import (
	"fmt"
	"net/http"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/frontends/azure_blob"
	"github.com/snithish/mockbucket/internal/frontends/azure_datalake"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/frontends/gcs"
	"github.com/snithish/mockbucket/internal/frontends/s3"
	"github.com/snithish/mockbucket/internal/seed"
)

func Validate(cfg config.Config) error {
	if cfg.Frontends.Type == "" {
		return fmt.Errorf("frontend.type is required")
	}
	switch cfg.Frontends.Type {
	case config.FrontendS3, config.FrontendGCS, config.FrontendAzureBlob, config.FrontendAzureDataLake:
		return nil
	default:
		return fmt.Errorf("frontend.type must be one of: s3, gcs, azure_blob, azure_datalake")
	}
}

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies, gcsServiceAccounts []seed.ServiceAccountJSON) {
	switch cfg.Frontends.Type {
	case config.FrontendS3:
		registerAWSRoot(mux, cfg, deps)
		s3.Register(mux, cfg, deps)
	case config.FrontendGCS:
		gcs.Register(mux, cfg, deps, gcsServiceAccounts)
	case config.FrontendAzureBlob:
		azure_blob.Register(mux, cfg, deps)
	case config.FrontendAzureDataLake:
		azure_datalake.Register(mux, cfg, deps)
	default:
		// Should not happen - validated in config.Validate()
	}
}
