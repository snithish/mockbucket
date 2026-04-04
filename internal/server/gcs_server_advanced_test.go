package server

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/storage"

	mbconfig "github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/seed"
)

func TestGCSSignedURLsRoundTripObjectAccess(t *testing.T) {
	// This checks that generated V4 signed URLs can write and read objects through the GCS frontend.
	fixture := newGCSServerFixture(t, func(cfg *mbconfig.Config) {
		cfg.Seed.GCS.ServiceCredentials = []seed.GCSServiceCredSeed{
			{
				ClientEmail: "signed@mockbucket.iam.gserviceaccount.com",
				Principal:   "signed-user",
			},
		}
	})
	serviceAccount := fixture.mustFetchServiceAccountInfo(t)
	client := fixture.client(t, "jwt:"+serviceAccount.ClientEmail)
	ctx := context.Background()

	bucket := client.Bucket("signed-bucket")
	if err := bucket.Create(ctx, "mock-project", nil); err != nil {
		t.Fatalf("Create(bucket) error = %v", err)
	}

	putURL, err := storage.SignedURL("signed-bucket", "signed-put.txt", &storage.SignedURLOptions{
		GoogleAccessID: serviceAccount.ClientEmail,
		PrivateKey:     []byte(serviceAccount.PrivateKey),
		Method:         http.MethodPut,
		ContentType:    "text/plain",
		Expires:        time.Now().Add(5 * time.Minute),
		Scheme:         storage.SigningSchemeV4,
		Insecure:       true,
		Hostname:       strings.TrimPrefix(fixture.server.URL, "http://"),
	})
	if err != nil {
		t.Fatalf("SignedURL(PUT) error = %v", err)
	}
	putReq := mustHTTPRequest(t, ctx, http.MethodPut, putURL, strings.NewReader("signed-body"))
	putReq.Header.Set("Content-Type", "text/plain")
	putResp := mustHTTPDo(t, putReq)
	_ = putResp.Body.Close()
	if got, want := putResp.StatusCode, http.StatusOK; got != want {
		t.Fatalf("signed PUT status = %d, want %d", got, want)
	}

	getURL, err := storage.SignedURL("signed-bucket", "signed-put.txt", &storage.SignedURLOptions{
		GoogleAccessID: serviceAccount.ClientEmail,
		PrivateKey:     []byte(serviceAccount.PrivateKey),
		Method:         http.MethodGet,
		Expires:        time.Now().Add(5 * time.Minute),
		Scheme:         storage.SigningSchemeV4,
		Insecure:       true,
		Hostname:       strings.TrimPrefix(fixture.server.URL, "http://"),
	})
	if err != nil {
		t.Fatalf("SignedURL(GET) error = %v", err)
	}
	getResp := mustHTTPDo(t, mustHTTPRequest(t, ctx, http.MethodGet, getURL, nil))
	defer getResp.Body.Close()
	if got, want := getResp.StatusCode, http.StatusOK; got != want {
		t.Fatalf("signed GET status = %d, want %d", got, want)
	}
	if got, want := mustReadAll(t, getResp.Body), "signed-body"; got != want {
		t.Fatalf("signed GET body = %q, want %q", got, want)
	}
}
