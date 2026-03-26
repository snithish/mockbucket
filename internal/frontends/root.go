package frontends

import (
	"net/http"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/frontends/s3"
	"github.com/snithish/mockbucket/internal/frontends/sts"
)

func registerAWSRoot(mux *http.ServeMux, cfg config.Config, deps common.Dependencies) {
	s3Root := s3.RootHandler(cfg, deps)
	stsRoot := sts.RootHandler(deps)
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" && r.Method == http.MethodPost {
			stsRoot.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/" && r.Method == http.MethodGet && r.URL.Query().Get("Action") != "" {
			stsRoot.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/" && r.Method == http.MethodGet {
			s3Root.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	}))
}
