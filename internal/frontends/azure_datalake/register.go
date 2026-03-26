package azure_datalake

import (
	"encoding/base64"
	"net/http"

	azauth "github.com/snithish/mockbucket/internal/auth/azure"
	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/frontends/common"
)

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies) {
	var accounts []azauth.AccountConfig
	if cfg.Azure.Account != "" {
		keyBytes, err := base64.StdEncoding.DecodeString(cfg.Azure.Key)
		if err != nil {
			keyBytes = []byte(cfg.Azure.Key)
		}
		accounts = append(accounts, azauth.AccountConfig{
			Name: cfg.Azure.Account,
			Key:  keyBytes,
		})
	}
	for _, acc := range cfg.Seed.Azure.Accounts {
		keyBytes, err := base64.StdEncoding.DecodeString(acc.Key)
		if err != nil {
			keyBytes = []byte(acc.Key)
		}
		accounts = append(accounts, azauth.AccountConfig{
			Name: acc.Name,
			Key:  keyBytes,
		})
	}

	resolver := azauth.NewAuthResolver(accounts)

	// Collect account names for path stripping (Python SDK includes account in path).
	accountNames := make(map[string]struct{}, len(accounts))
	for _, acc := range accounts {
		accountNames[acc.Name] = struct{}{}
	}

	dfsMux := http.NewServeMux()
	registerDFSHandlers(dfsMux, deps, resolver, accountNames)
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
	w.Header().Set("x-ms-version", "2021-06-08")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"filesystems":[]}`))
}
