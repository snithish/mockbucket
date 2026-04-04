package server

import (
	"bytes"
	"context"
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
	"github.com/snithish/mockbucket/internal/core"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

	mbconfig "github.com/snithish/mockbucket/internal/config"
)

type gcsServerFixture struct {
	runtime *Runtime
	server  *httptest.Server
}

type gcsContractClient struct {
	client    *storage.Client
	endpoint  string
	projectID string
	token     string
}

type gcsServiceAccountSecret struct {
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
}

func newGCSServerFixture(t *testing.T, configure func(*mbconfig.Config)) *gcsServerFixture {
	t.Helper()
	runtime := newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.Type = mbconfig.FrontendGCS
		if configure != nil {
			configure(cfg)
		}
	})
	return &gcsServerFixture{
		runtime: runtime,
		server:  newHTTPTestServer(t, runtime.HTTPServer.Handler),
	}
}

func (f *gcsServerFixture) authedRequest(t *testing.T, ctx context.Context, method, path, token string, body io.Reader) *http.Request {
	t.Helper()
	req := mustHTTPRequest(t, ctx, method, strings.TrimRight(f.server.URL, "/")+path, body)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return req
}

func (f *gcsServerFixture) client(t *testing.T, token string) *storage.Client {
	t.Helper()
	client := newGCSClient(t, f.server.URL, token)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func (f *gcsServerFixture) seedRoleAndAccount(t *testing.T, email, principal, token string) {
	t.Helper()
	if err := f.runtime.Metadata.UpsertRole(context.Background(), core.Role{Name: "gcs-service-account"}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	f.seedServiceAccount(t, email, principal, token)
}

func (f *gcsServerFixture) seedServiceAccount(t *testing.T, email, principal, token string) {
	t.Helper()
	if err := f.runtime.Metadata.UpsertServiceAccount(context.Background(), core.ServiceAccount{
		ClientEmail: email,
		Principal:   principal,
		Token:       token,
	}); err != nil {
		t.Fatalf("UpsertServiceAccount() error = %v", err)
	}
}

func (f *gcsServerFixture) mustCreateBucket(t *testing.T, token, bucket string) {
	t.Helper()
	req := f.authedRequest(t, context.Background(), http.MethodPost, "/storage/v1/b", token, strings.NewReader(`{"name":"`+bucket+`"}`))
	req.Header.Set("Content-Type", "application/json")
	resp := mustHTTPDo(t, req)
	defer resp.Body.Close()
	body := mustReadAll(t, resp.Body)
	if got, want := resp.StatusCode, http.StatusOK; got != want {
		t.Fatalf("create bucket status = %d, want %d, body=%s", got, want, body)
	}
}

func (f *gcsServerFixture) mustFetchServiceAccountInfo(t *testing.T) gcsServiceAccountSecret {
	t.Helper()
	resp, err := http.Get(strings.TrimRight(f.server.URL, "/") + "/api/v1/gcs/service-account")
	if err != nil {
		t.Fatalf("GET /api/v1/gcs/service-account error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", resp.StatusCode, mustReadAll(t, resp.Body))
	}
	var payload struct {
		ServiceAccounts []struct {
			SecretJSON gcsServiceAccountSecret `json:"secret_json"`
		} `json:"service_accounts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(payload.ServiceAccounts) == 0 {
		t.Fatal("service_accounts is empty")
	}
	return payload.ServiceAccounts[0].SecretJSON
}

func newGCSContractClient(t *testing.T) frontendContractClient {
	t.Helper()
	fixture := newGCSServerFixture(t, nil)
	fixture.seedServiceAccount(t, "contract@mock.iam.gserviceaccount.com", "admin", "gcs-contract-token")

	return &gcsContractClient{
		client:    fixture.client(t, "gcs-contract-token"),
		endpoint:  fixture.server.URL,
		projectID: "mock-project",
		token:     "gcs-contract-token",
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
	return contractObjectAttrs{
		ContentLength: attrs.Size,
		ETag:          attrs.Etag,
	}, nil
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
	reqURL := fmt.Sprintf("%s/storage/v1/b/%s/o?prefix=%s&maxResults=%d", strings.TrimRight(c.endpoint, "/"), url.PathEscape(bucket), url.QueryEscape(prefix), pageSize)
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
	return contractListResult{
		Keys:      keys,
		NextToken: payload.NextPageToken,
		Truncated: payload.NextPageToken != "",
	}, nil
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
	client, err := storage.NewClient(
		context.Background(),
		option.WithEndpoint(apiEndpoint),
		option.WithTokenSource(tokenSource),
		option.WithScopes(storage.ScopeFullControl),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	return client
}
