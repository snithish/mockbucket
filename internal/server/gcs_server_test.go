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
	"time"

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

	if err := runtime.Metadata.UpsertRole(context.Background(), core.Role{
		Name: "gcs-admin",
		Policies: []core.PolicyDocument{{
			Statements: []core.PolicyStatement{{
				Effect:    core.EffectAllow,
				Actions:   []string{"*"},
				Resources: []string{"*"},
			}},
		}},
	}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	session := core.Session{
		Token:         "gcs-token",
		AccessKeyID:   "gcs-access",
		SecretKey:     "gcs-secret",
		PrincipalName: "admin",
		RoleName:      "gcs-admin",
		SessionName:   "gcs",
		CreatedAt:     time.Now().UTC(),
		ExpiresAt:     time.Now().UTC().Add(time.Hour),
	}
	if err := runtime.Metadata.CreateSession(context.Background(), session); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}

	server := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(server.Close)

	client := newGCSClient(t, server.URL, session.Token)
	t.Cleanup(func() { _ = client.Close() })

	return &gcsContractClient{client: client, endpoint: server.URL, projectID: "mock-project", token: session.Token}
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
