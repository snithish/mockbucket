package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/snithish/mockbucket/internal/config"
	mbconfig "github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/seed"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func TestGCSFrontendContract(t *testing.T) {
	runFrontendContractTests(t, newGCSContractClient)
}

func TestGCSListBucketsRequiresAuthentication(t *testing.T) {
	_, server := newGCSTestServer(t, nil, nil)

	resp, err := http.Get(server.URL + "/storage/v1/b")
	if err != nil {
		t.Fatalf("GET /storage/v1/b error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestGCSListBucketsRejectsInvalidBearerToken(t *testing.T) {
	_, server := newGCSTestServer(t, nil, nil)

	req, _ := http.NewRequest(http.MethodGet, server.URL+"/storage/v1/b", nil)
	req.Header.Set("Authorization", "Bearer invalid")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /storage/v1/b error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestGCSTokenEndpoint_ClientCredentials(t *testing.T) {
	_, server := newGCSTestServer(t, nil, func(runtime *Runtime) {
		seedGCSRoleAndAccount(t, runtime, "sa@mock.iam.gserviceaccount.com", "admin", "static-sa-token")
	})

	// Exchange client credentials for a bearer token and verify the same token reaches the bucket API.
	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", "sa@mock.iam.gserviceaccount.com")
	resp, err := http.Post(server.URL+"/oauth2/v4/token", "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("POST /oauth2/v4/token error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /oauth2/v4/token status = %d, body = %s", resp.StatusCode, body)
	}

	var tok struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		t.Fatalf("decode token: %v", err)
	}
	if tok.AccessToken == "" || tok.TokenType != "Bearer" {
		t.Fatalf("unexpected token response: %+v", tok)
	}

	// Use the token to list buckets.
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/storage/v1/b", nil)
	req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	listResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /storage/v1/b error = %v", err)
	}
	defer listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list buckets status = %d, want %d", listResp.StatusCode, http.StatusOK)
	}
}

func TestGCSTokenEndpoint_JWTBearer(t *testing.T) {
	_, server := newGCSTestServer(t, nil, func(runtime *Runtime) {
		seedGCSRoleAndAccount(t, runtime, "sa@mock.iam.gserviceaccount.com", "admin", "static-jwt-token")
	})

	// The JWT flow only inspects issuer/audience today, so this checks the emulator accepts SDK-style assertions.
	claims := `{"iss":"sa@mock.iam.gserviceaccount.com","aud":"` + server.URL + `/oauth2/v4/token"}`
	payload := base64.RawURLEncoding.EncodeToString([]byte(claims))
	assertion := "eyJhbGciOiJSUzI1NiJ9." + payload + ".fakesig"

	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("assertion", assertion)
	resp, err := http.Post(server.URL+"/oauth2/v4/token", "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("POST /oauth2/v4/token error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /oauth2/v4/token status = %d, body = %s", resp.StatusCode, body)
	}

	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		t.Fatalf("decode token: %v", err)
	}
	if tok.AccessToken == "" {
		t.Fatal("access_token is empty")
	}
}

func TestGCSTokenEndpointFailureModes(t *testing.T) {
	_, server := newGCSTestServer(t, nil, nil)

	claims := `{"aud":"` + server.URL + `/oauth2/v4/token"}`
	assertionNoIssuer := "eyJhbGciOiJSUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(claims)) + ".fakesig"

	tests := []struct {
		name        string
		method      string
		body        string
		contentType string
		wantStatus  int
		wantText    string
	}{
		{
			name:       "method not allowed",
			method:     http.MethodGet,
			wantStatus: http.StatusMethodNotAllowed,
			wantText:   "method not allowed",
		},
		{
			name:        "unsupported grant type",
			method:      http.MethodPost,
			body:        "grant_type=password",
			contentType: "application/x-www-form-urlencoded",
			wantStatus:  http.StatusBadRequest,
			wantText:    "unsupported grant_type",
		},
		{
			name:        "client credentials missing client id",
			method:      http.MethodPost,
			body:        "grant_type=client_credentials",
			contentType: "application/x-www-form-urlencoded",
			wantStatus:  http.StatusBadRequest,
			wantText:    "client_id is required",
		},
		{
			name:        "jwt bearer missing iss and sub",
			method:      http.MethodPost,
			body:        "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=" + url.QueryEscape(assertionNoIssuer),
			contentType: "application/x-www-form-urlencoded",
			wantStatus:  http.StatusBadRequest,
			wantText:    "invalid assertion",
		},
		{
			name:        "unknown client email",
			method:      http.MethodPost,
			body:        "grant_type=client_credentials&client_id=missing%40mock.iam.gserviceaccount.com",
			contentType: "application/x-www-form-urlencoded",
			wantStatus:  http.StatusUnauthorized,
			wantText:    "invalid_client",
		},
	}

	for _, tt := range tests {
		req, _ := http.NewRequest(tt.method, server.URL+"/oauth2/v4/token", strings.NewReader(tt.body))
		if tt.contentType != "" {
			req.Header.Set("Content-Type", tt.contentType)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s request error = %v", tt.name, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != tt.wantStatus {
			t.Fatalf("%s status = %d, want %d, body=%s", tt.name, resp.StatusCode, tt.wantStatus, string(body))
		}
		if !strings.Contains(string(body), tt.wantText) {
			t.Fatalf("%s body = %q, want to contain %q", tt.name, string(body), tt.wantText)
		}
	}
}

func TestGCSTokenEndpointFallsBackToSingleServiceAccount(t *testing.T) {
	_, server := newGCSTestServer(t, nil, func(runtime *Runtime) {
		seedGCSServiceAccount(t, runtime, "sa@mock.iam.gserviceaccount.com", "admin", "static-jwt-token")
	})

	claims := `{"iss":"unexpected@mock.iam.gserviceaccount.com","aud":"` + server.URL + `/oauth2/v4/token"}`
	payload := base64.RawURLEncoding.EncodeToString([]byte(claims))
	assertion := "eyJhbGciOiJSUzI1NiJ9." + payload + ".fakesig"

	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("assertion", assertion)
	resp, err := http.Post(server.URL+"/oauth2/v4/token", "application/x-www-form-urlencoded", strings.NewReader(form.Encode()))
	if err != nil {
		t.Fatalf("POST /oauth2/v4/token error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /oauth2/v4/token status = %d, body = %s", resp.StatusCode, body)
	}
}

func TestGCSAuthenticatedBucketAndObjectFlow(t *testing.T) {
	_, server := newGCSTestServer(t, nil, func(runtime *Runtime) {
		seedGCSServiceAccount(t, runtime, "flow@mock.iam.gserviceaccount.com", "flow-user", "gcs-flow-token")
	})

	// This keeps the test at the HTTP layer so bucket creation, upload, metadata lookup, and media download share one auth path.
	createBucketReq, _ := http.NewRequest(http.MethodPost, server.URL+"/storage/v1/b", strings.NewReader(`{"name":"flow-bucket"}`))
	createBucketReq.Header.Set("Authorization", "Bearer gcs-flow-token")
	createBucketReq.Header.Set("Content-Type", "application/json")
	createBucketResp, err := http.DefaultClient.Do(createBucketReq)
	if err != nil {
		t.Fatalf("create bucket request error = %v", err)
	}
	defer createBucketResp.Body.Close()
	if createBucketResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(createBucketResp.Body)
		t.Fatalf("create bucket status = %d, want 200, body=%s", createBucketResp.StatusCode, string(body))
	}

	uploadReq, _ := http.NewRequest(
		http.MethodPost,
		server.URL+"/upload/storage/v1/b/flow-bucket/o?uploadType=media&name=hello.txt",
		strings.NewReader("hello gcs"),
	)
	uploadReq.Header.Set("Authorization", "Bearer gcs-flow-token")
	uploadResp, err := http.DefaultClient.Do(uploadReq)
	if err != nil {
		t.Fatalf("upload object request error = %v", err)
	}
	defer uploadResp.Body.Close()
	if uploadResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(uploadResp.Body)
		t.Fatalf("upload object status = %d, want 200, body=%s", uploadResp.StatusCode, string(body))
	}

	getBucketReq, _ := http.NewRequest(http.MethodGet, server.URL+"/storage/v1/b/flow-bucket", nil)
	getBucketReq.Header.Set("Authorization", "Bearer gcs-flow-token")
	getBucketResp, err := http.DefaultClient.Do(getBucketReq)
	if err != nil {
		t.Fatalf("get bucket request error = %v", err)
	}
	defer getBucketResp.Body.Close()
	if getBucketResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getBucketResp.Body)
		t.Fatalf("get bucket status = %d, want 200, body=%s", getBucketResp.StatusCode, string(body))
	}

	getObjectReq, _ := http.NewRequest(http.MethodGet, server.URL+"/storage/v1/b/flow-bucket/o/hello.txt?alt=media", nil)
	getObjectReq.Header.Set("Authorization", "Bearer gcs-flow-token")
	getObjectResp, err := http.DefaultClient.Do(getObjectReq)
	if err != nil {
		t.Fatalf("get object request error = %v", err)
	}
	defer getObjectResp.Body.Close()
	if getObjectResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(getObjectResp.Body)
		t.Fatalf("get object status = %d, want 200, body=%s", getObjectResp.StatusCode, string(body))
	}
	body, _ := io.ReadAll(getObjectResp.Body)
	if got, want := string(body), "hello gcs"; got != want {
		t.Fatalf("object body = %q, want %q", got, want)
	}
}

func TestGCSPhaseScaffolding(t *testing.T) {
	t.Run("MetadataRoundTrip", func(t *testing.T) {
		t.Skip("Phase 3: persisted metadata coverage")
	})
	t.Run("GenerationPreconditions", func(t *testing.T) {
		t.Skip("Phase 4: generation and metageneration precondition coverage")
	})
	t.Run("Compose", func(t *testing.T) {
		t.Skip("Phase 5: compose coverage")
	})
	t.Run("SignedURLs", func(t *testing.T) {
		t.Skip("Phase 5: signed URL coverage")
	})
}

func TestGCSServiceAccountEndpointUsesSeededPrincipal(t *testing.T) {
	_, server := newGCSTestServer(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.Type = config.FrontendGCS
		cfg.Seed.GCS.ServiceCredentials = []seed.GCSServiceCredSeed{
			{
				ClientEmail: "svc-acct@mockbucket.iam.gserviceaccount.com",
				Principal:   "custom-principal",
			},
		}
	}, nil)

	resp, err := http.Get(server.URL + "/api/v1/gcs/service-account")
	if err != nil {
		t.Fatalf("GET /api/v1/gcs/service-account error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want 200, body=%s", resp.StatusCode, string(body))
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

func TestGCSListObjectsUsesStartOffsetAndOpaqueToken(t *testing.T) {
	client := newGCSContractClient(t).(*gcsContractClient)
	ctx := context.Background()
	for _, key := range []string{
		"logs/2024-01.txt",
		"logs/2024-02.txt",
		"logs/2024-03.txt",
		"logs/2024-04.txt",
	} {
		if _, err := client.PutObject(ctx, "demo", key, "x"); err != nil {
			t.Fatalf("PutObject(%s) error = %v", key, err)
		}
	}

	first, err := client.ListObjects(ctx, "demo", "logs/", 2, "", "")
	if err != nil {
		t.Fatalf("ListObjects(first) error = %v", err)
	}
	if len(first.Keys) != 2 {
		t.Fatalf("first page keys = %d, want 2", len(first.Keys))
	}
	if first.NextToken == "" {
		t.Fatal("first page missing next token")
	}
	if first.NextToken == first.Keys[len(first.Keys)-1] {
		t.Fatalf("expected opaque page token, got raw key token %q", first.NextToken)
	}

	second, err := client.ListObjects(ctx, "demo", "logs/", 2, first.NextToken, "")
	if err != nil {
		t.Fatalf("ListObjects(second) error = %v", err)
	}
	if len(second.Keys) != 2 {
		t.Fatalf("second page keys = %d, want 2", len(second.Keys))
	}
	if second.Keys[0] != "logs/2024-03.txt" {
		t.Fatalf("second page first key = %q, want logs/2024-03.txt", second.Keys[0])
	}

	offset, err := client.ListObjects(ctx, "demo", "logs/", 1000, "", "logs/2024-02.txt")
	if err != nil {
		t.Fatalf("ListObjects(startOffset) error = %v", err)
	}
	if len(offset.Keys) != 2 {
		t.Fatalf("startOffset keys = %d, want 2", len(offset.Keys))
	}
	if offset.Keys[0] != "logs/2024-03.txt" {
		t.Fatalf("startOffset first key = %q, want logs/2024-03.txt", offset.Keys[0])
	}
}

type gcsContractClient struct {
	client    *storage.Client
	endpoint  string
	projectID string
	token     string
}

func newGCSContractClient(t *testing.T) frontendContractClient {
	t.Helper()
	_, server := newGCSTestServer(t, nil, func(runtime *Runtime) {
		seedGCSServiceAccount(t, runtime, "contract@mock.iam.gserviceaccount.com", "admin", "gcs-contract-token")
	})

	client := newGCSClient(t, server.URL, "gcs-contract-token")
	t.Cleanup(func() { _ = client.Close() })

	return &gcsContractClient{client: client, endpoint: server.URL, projectID: "mock-project", token: "gcs-contract-token"}
}

func newGCSTestServer(t *testing.T, configure func(*mbconfig.Config), seed func(*Runtime)) (*Runtime, *httptest.Server) {
	t.Helper()
	runtime := newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.Type = config.FrontendGCS
		if configure != nil {
			configure(cfg)
		}
	})
	t.Cleanup(func() { _ = runtime.Close() })
	if seed != nil {
		seed(runtime)
	}
	server := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(server.Close)
	return runtime, server
}

func seedGCSRoleAndAccount(t *testing.T, runtime *Runtime, email, principal, token string) {
	t.Helper()
	if err := runtime.Metadata.UpsertRole(context.Background(), core.Role{Name: "gcs-service-account"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	seedGCSServiceAccount(t, runtime, email, principal, token)
}

func seedGCSServiceAccount(t *testing.T, runtime *Runtime, email, principal, token string) {
	t.Helper()
	if err := runtime.Metadata.UpsertServiceAccount(context.Background(), core.ServiceAccount{
		ClientEmail: email,
		Principal:   principal,
		Token:       token,
	}); err != nil {
		t.Fatalf("UpsertServiceAccount() error = %v", err)
	}
}

func (c *gcsContractClient) CreateBucket(ctx context.Context, bucket string) error {
	return c.client.Bucket(bucket).Create(ctx, c.projectID, nil)
}

func (c *gcsContractClient) HeadBucket(ctx context.Context, bucket string) error {
	_, err := c.client.Bucket(bucket).Attrs(ctx)
	return err
}

func (c *gcsContractClient) ListBuckets(ctx context.Context) ([]string, error) {
	it := c.client.Buckets(ctx, c.projectID)
	var names []string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			return names, nil
		}
		if err != nil {
			return nil, err
		}
		names = append(names, attrs.Name)
	}
}

func (c *gcsContractClient) PutObject(ctx context.Context, bucket, key, body string) (string, error) {
	obj := c.client.Bucket(bucket).Object(key)
	writer := obj.NewWriter(ctx)
	writer.ChunkSize = 0
	writer.SendCRC32C = false
	if _, err := writer.Write([]byte(body)); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return "", err
	}
	return attrs.Etag, nil
}

func (c *gcsContractClient) HeadObject(ctx context.Context, bucket, key string) (contractObjectAttrs, error) {
	attrs, err := c.client.Bucket(bucket).Object(key).Attrs(ctx)
	if err != nil {
		return contractObjectAttrs{}, err
	}
	return contractObjectAttrs{ContentLength: attrs.Size, ETag: attrs.Etag}, nil
}

func (c *gcsContractClient) GetObject(ctx context.Context, bucket, key string) (string, error) {
	reader, err := c.client.Bucket(bucket).Object(key).NewReader(ctx)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (c *gcsContractClient) DeleteObject(ctx context.Context, bucket, key string) error {
	return c.client.Bucket(bucket).Object(key).Delete(ctx)
}

func (c *gcsContractClient) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int32, continuationToken, startAfter string) (contractListResult, error) {
	pageSize := int(maxKeys)
	if pageSize <= 0 {
		pageSize = 1000
	}
	endpoint := strings.TrimRight(c.endpoint, "/")
	reqURL := fmt.Sprintf("%s/storage/v1/b/%s/o?prefix=%s&maxResults=%d", endpoint, url.PathEscape(bucket), url.QueryEscape(prefix), pageSize)
	if continuationToken != "" {
		reqURL += "&pageToken=" + url.QueryEscape(continuationToken)
	}
	if startAfter != "" {
		reqURL += "&startOffset=" + url.QueryEscape(startAfter)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return contractListResult{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return contractListResult{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		return contractListResult{}, fmt.Errorf("list objects failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	var payload struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return contractListResult{}, err
	}
	keys := make([]string, 0, len(payload.Items))
	for _, item := range payload.Items {
		keys = append(keys, item.Name)
	}
	return contractListResult{Keys: keys, NextToken: payload.NextPageToken, Truncated: payload.NextPageToken != ""}, nil
}

func (c *gcsContractClient) MultipartUpload(ctx context.Context, bucket, key string, parts []string) error {
	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)

	metaHeader := textproto.MIMEHeader{}
	metaHeader.Set("Content-Type", "application/json; charset=UTF-8")
	metaPart, err := writer.CreatePart(metaHeader)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(metaPart).Encode(map[string]string{"name": key}); err != nil {
		return err
	}

	bodyHeader := textproto.MIMEHeader{}
	bodyHeader.Set("Content-Type", "application/octet-stream")
	bodyPart, err := writer.CreatePart(bodyHeader)
	if err != nil {
		return err
	}
	if _, err := bodyPart.Write([]byte(strings.Join(parts, ""))); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}

	uploadURL := fmt.Sprintf("%s/upload/storage/v1/b/%s/o?uploadType=multipart", strings.TrimRight(c.endpoint, "/"), url.PathEscape(bucket))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, &payload)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "multipart/related; boundary="+writer.Boundary())

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusBadRequest {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("multipart upload failed: status=%d body=%s", resp.StatusCode, string(body))
	}
	return nil
}

func newGCSClient(t *testing.T, endpoint, token string) *storage.Client {
	t.Helper()
	tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})
	apiEndpoint := strings.TrimRight(endpoint, "/") + "/storage/v1/"
	client, err := storage.NewClient(context.Background(),
		option.WithEndpoint(apiEndpoint),
		option.WithTokenSource(tokenSource),
		option.WithScopes(storage.ScopeFullControl),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}

func newGCSAuthedRequest(t *testing.T, ctx context.Context, method, endpoint, path, token string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(endpoint, "/")+path, body)
	if err != nil {
		t.Fatalf("NewRequestWithContext() error = %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}
