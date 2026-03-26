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
