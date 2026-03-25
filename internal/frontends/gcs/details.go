package gcs

import (
	"encoding/json"
	"net/http"

	"github.com/snithish/mockbucket/internal/seed"
)

// RegisterServiceAccountEndpoint registers the /api/v1/gcs/service-account
// endpoint that returns auto-generated service account JSON for each principal.
// This JSON can be used directly with Python SDK's from_service_account_info().
func RegisterServiceAccountEndpoint(mux *http.ServeMux, accounts []seed.ServiceAccountJSON) {
	if len(accounts) == 0 {
		return
	}

	response := make([]map[string]any, 0, len(accounts))
	for _, sa := range accounts {
		response = append(response, map[string]any{
			"client_email": sa.ClientEmail,
			"principal":    extractPrincipal(sa.ClientEmail),
			"secret_json": map[string]any{
				"type":         sa.Type,
				"client_email": sa.ClientEmail,
				"private_key":  sa.PrivateKey,
				"client_id":    sa.ClientID,
				"token_uri":    sa.TokenURI,
				"project_id":   sa.ProjectID,
			},
		})
	}

	mux.HandleFunc("/api/v1/gcs/service-account", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"service_accounts": response,
		})
	})
}

func extractPrincipal(clientEmail string) string {
	// Extract principal name from client_email like "admin@mockbucket.iam.gserviceaccount.com"
	for i, c := range clientEmail {
		if c == '@' {
			return clientEmail[:i]
		}
	}
	return clientEmail
}
