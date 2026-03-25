package frontends

import (
	"fmt"
	"net/http"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/frontends/azure"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/frontends/gcs"
	"github.com/snithish/mockbucket/internal/frontends/s3"
	"github.com/snithish/mockbucket/internal/seed"
)

func Validate(cfg config.Config) error {
	if cfg.Frontends.Azure {
		return fmt.Errorf("frontends.azure is not implemented yet")
	}
	if cfg.Frontends.S3 && cfg.Frontends.GCS {
		return fmt.Errorf("frontends.s3 and frontends.gcs cannot both be enabled")
	}
	return nil
}

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies, gcsServiceAccounts []seed.ServiceAccountJSON) {
	registerAWSRoot(mux, cfg, deps)
	s3.Register(mux, cfg, deps)
	gcs.Register(mux, cfg, deps, gcsServiceAccounts)
	azure.Register(mux, cfg)
}
