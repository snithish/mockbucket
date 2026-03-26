package azure_blob

import (
	"net/http"

	azauth "github.com/snithish/mockbucket/internal/auth/azure"
	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/frontends/azure_shared"
	"github.com/snithish/mockbucket/internal/frontends/common"
)

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies) {
	resolver, _ := azure_shared.BuildAuthResolver(cfg)

	blobMux := http.NewServeMux()
	registerBlobHandlers(blobMux, deps)
	blobHandler := azauth.AuthenticateAnonymousOrSharedKey(resolver)(blobMux)

	mux.Handle("/", blobHandler)
}

func handleRoot(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	azure_shared.SetVersionHeader(w)
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="` + scheme + `://` + r.Host + `"><Containers></Containers></EnumerationResults>`))
}
