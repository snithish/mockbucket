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
	mbconfig "github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func TestGCSFrontendContract(t *testing.T) {
	runFrontendContractTests(t, newGCSContractClient)
}

func TestGCSTokenEndpoint_ClientCredentials(t *testing.T) {
	runtime := newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.GCS = true
	})
	t.Cleanup(func() { _ = runtime.Close() })

	// Create a role for service accounts.
	if err := runtime.Metadata.UpsertRole(context.Background(), core.Role{
		Name: "gcs-service-account",
	}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}

	// Create a service account for the client.
	if err := runtime.Metadata.UpsertServiceAccount(context.Background(), core.ServiceAccount{
		ClientEmail: "sa@mock.iam.gserviceaccount.com",
		Principal:   "admin",
		Token:       "static-sa-token",
	}); err != nil {
		t.Fatalf("UpsertServiceAccount() error = %v", err)
	}

	server := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(server.Close)

	// Exchange client credentials for a token.
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
	runtime := newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.GCS = true
	})
	t.Cleanup(func() { _ = runtime.Close() })

	if err := runtime.Metadata.UpsertRole(context.Background(), core.Role{
		Name: "gcs-service-account",
	}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	// Create a service account for the client.
	if err := runtime.Metadata.UpsertServiceAccount(context.Background(), core.ServiceAccount{
		ClientEmail: "sa@mock.iam.gserviceaccount.com",
		Principal:   "admin",
		Token:       "static-jwt-token",
	}); err != nil {
		t.Fatalf("UpsertServiceAccount() error = %v", err)
	}

	server := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(server.Close)

	// Build a JWT assertion with iss claim.
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

type gcsContractClient struct {
	client    *storage.Client
	endpoint  string
	projectID string
	token     string
}

func newGCSContractClient(t *testing.T) frontendContractClient {
	t.Helper()
	runtime := newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.GCS = true
	})
	t.Cleanup(func() { _ = runtime.Close() })

	// Create a service account for static token auth.
	if err := runtime.Metadata.UpsertServiceAccount(context.Background(), core.ServiceAccount{
		ClientEmail: "contract@mock.iam.gserviceaccount.com",
		Principal:   "admin",
		Token:       "gcs-contract-token",
	}); err != nil {
		t.Fatalf("UpsertServiceAccount() error = %v", err)
	}

	server := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(server.Close)

	client := newGCSClient(t, server.URL, "gcs-contract-token")
	t.Cleanup(func() { _ = client.Close() })

	return &gcsContractClient{client: client, endpoint: server.URL, projectID: "mock-project", token: "gcs-contract-token"}
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
	defer func() { _ = reader.Close() }()
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
	defer func() { _ = resp.Body.Close() }()
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
		if startAfter != "" && item.Name <= startAfter {
			continue
		}
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
	defer func() { _ = resp.Body.Close() }()
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
