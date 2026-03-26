package azure_blob

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

	resolver := azauth.NewAuthResolver(accounts)

	blobMux := http.NewServeMux()
	registerBlobHandlers(blobMux, deps, resolver)
	blobHandler := azauth.AuthenticateAnonymousOrSharedKey(resolver)(blobMux)

	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if len(host) == 0 {
			handleRoot(w, r, deps)
			return
		}
		blobHandler.ServeHTTP(w, r)
	}))
}

func handleRoot(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	w.Header().Set("x-ms-version", "2021-06-08")
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="utf-8"?><EnumerationResults ServiceEndpoint="` + scheme + `://` + r.Host + `"><Containers></Containers></EnumerationResults>`))
}
