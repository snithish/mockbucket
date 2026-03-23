package frontends

import (
	"fmt"
	"net/http"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/frontends/azure"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/frontends/gcs"
	"github.com/snithish/mockbucket/internal/frontends/s3"
	"github.com/snithish/mockbucket/internal/frontends/sts"
)

func Validate(cfg config.Config) error {
	if cfg.Frontends.GCS {
		return fmt.Errorf("frontends.gcs is not implemented yet")
	}
	if cfg.Frontends.Azure {
		return fmt.Errorf("frontends.azure is not implemented yet")
	}
	return nil
}

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies) {
	registerAWSRoot(mux, cfg, deps)
	s3.Register(mux, cfg, deps)
	sts.Register(mux, cfg, deps)
	gcs.Register(mux, cfg)
	azure.Register(mux, cfg)
}
