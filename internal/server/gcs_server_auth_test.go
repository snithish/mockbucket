package server

import (
	"encoding/json"
	"net/http"
	"testing"

	mbconfig "github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/seed"
)

func TestGCSServiceAccountEndpointUsesSeededPrincipal(t *testing.T) {
	// This checks that generated service account JSON reflects the principal configured in the startup seed.
	fixture := newGCSServerFixture(t, func(cfg *mbconfig.Config) {
		cfg.Seed.GCS.ServiceCredentials = []seed.GCSServiceCredSeed{
			{
				ClientEmail: "svc-acct@mockbucket.iam.gserviceaccount.com",
				Principal:   "custom-principal",
			},
		}
	})

	resp, err := http.Get(fixture.server.URL + "/api/v1/gcs/service-account")
	if err != nil {
		t.Fatalf("GET /api/v1/gcs/service-account error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", resp.StatusCode, mustReadAll(t, resp.Body))
	}
	var payload struct {
		ServiceAccounts []struct {
			ClientEmail string `json:"client_email"`
			Principal   string `json:"principal"`
			SecretJSON  struct {
				ClientEmail  string `json:"client_email"`
				ClientID     string `json:"client_id"`
				PrivateKey   string `json:"private_key"`
				PrivateKeyID string `json:"private_key_id"`
				TokenURI     string `json:"token_uri"`
				Type         string `json:"type"`
			} `json:"secret_json"`
		} `json:"service_accounts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode response error = %v", err)
	}
	if len(payload.ServiceAccounts) != 1 {
		t.Fatalf("service accounts count = %d, want 1", len(payload.ServiceAccounts))
	}
	if got, want := payload.ServiceAccounts[0].Principal, "custom-principal"; got != want {
		t.Fatalf("principal = %q, want %q", got, want)
	}
	if got := payload.ServiceAccounts[0].SecretJSON.PrivateKeyID; got == "" {
		t.Fatal("private_key_id is empty")
	}
	if got, want := payload.ServiceAccounts[0].SecretJSON.Type, "service_account"; got != want {
		t.Fatalf("secret_json.type = %q, want %q", got, want)
	}
	if got, want := payload.ServiceAccounts[0].SecretJSON.ClientEmail, "svc-acct@mockbucket.iam.gserviceaccount.com"; got != want {
		t.Fatalf("secret_json.client_email = %q, want %q", got, want)
	}
}
