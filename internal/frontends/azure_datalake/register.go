package azure_datalake

import (
	"net/http"

	azauth "github.com/snithish/mockbucket/internal/auth/azure"
	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/frontends/azure_shared"
	"github.com/snithish/mockbucket/internal/frontends/common"
)

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies) {
	resolver, accountNames := azure_shared.BuildAuthResolver(cfg)

	dfsMux := http.NewServeMux()
	registerDFSHandlers(dfsMux, deps, accountNames)
	dfsHandler := azauth.AuthenticateAnonymousOrSharedKey(resolver)(dfsMux)

	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// DataLake SDK doesn't set X-Ms-Blob-Type header, but Blob SDK does.
		// If Blob header is present, reject with appropriate error.
		if r.Header.Get("X-Ms-Blob-Type") != "" {
			writeDFSDatalakeError(w, http.StatusBadRequest, "InvalidHeader", "This endpoint does not support Blob storage operations. Use azure_blob frontend.")
			return
		}
		dfsHandler.ServeHTTP(w, r)
	}))
}

func handleRoot(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	azure_shared.SetVersionHeader(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"filesystems":[]}`))
}
