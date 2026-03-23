package server

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/config"
)

func TestRuntimeRegistersHealthRoutes(t *testing.T) {
	runtime := newTestRuntime(t, false, false)
	defer func() { _ = runtime.Close() }()
	for _, path := range []string{"/healthz", "/readyz"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		res := httptest.NewRecorder()
		runtime.HTTPServer.Handler.ServeHTTP(res, req)
		if got, want := res.Code, http.StatusOK; got != want {
			t.Fatalf("%s status = %d, want %d", path, got, want)
		}
	}
}

func TestRuntimeRejectsUnsupportedFrontends(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Frontends.GCS = true
	_, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(testWriter{t}, nil)))
	if err == nil {
		t.Fatal("New() error = nil, want unsupported frontend error")
	}
}

func TestS3BucketLevelAPI(t *testing.T) {
	runtime := newTestRuntime(t, true, false)
	defer func() { _ = runtime.Close() }()

	listReq := httptest.NewRequest(http.MethodGet, "http://mockbucket.local/", nil)
	signAWSRequest(t, listReq, "s3", "us-east-1", "admin", "admin-secret", "")
	listRes := httptest.NewRecorder()
	runtime.HTTPServer.Handler.ServeHTTP(listRes, listReq)
	if got, want := listRes.Code, http.StatusOK; got != want {
		t.Fatalf("list buckets status = %d, want %d, body=%s", got, want, listRes.Body.String())
	}
	if !strings.Contains(listRes.Body.String(), "<Name>demo</Name>") {
		t.Fatalf("list buckets body = %q, want demo bucket", listRes.Body.String())
	}

	createReq := httptest.NewRequest(http.MethodPut, "http://mockbucket.local/logs", nil)
	signAWSRequest(t, createReq, "s3", "us-east-1", "admin", "admin-secret", "")
	createRes := httptest.NewRecorder()
	runtime.HTTPServer.Handler.ServeHTTP(createRes, createReq)
	if got, want := createRes.Code, http.StatusOK; got != want {
		t.Fatalf("create bucket status = %d, want %d, body=%s", got, want, createRes.Body.String())
	}

	headReq := httptest.NewRequest(http.MethodHead, "http://mockbucket.local/logs", nil)
	signAWSRequest(t, headReq, "s3", "us-east-1", "admin", "admin-secret", "")
	headRes := httptest.NewRecorder()
	runtime.HTTPServer.Handler.ServeHTTP(headRes, headReq)
	if got, want := headRes.Code, http.StatusOK; got != want {
		t.Fatalf("head bucket status = %d, want %d", got, want)
	}

	locationReq := httptest.NewRequest(http.MethodGet, "http://mockbucket.local/logs?location=", nil)
	signAWSRequest(t, locationReq, "s3", "us-east-1", "admin", "admin-secret", "")
	locationRes := httptest.NewRecorder()
	runtime.HTTPServer.Handler.ServeHTTP(locationRes, locationReq)
	if got, want := locationRes.Code, http.StatusOK; got != want {
		t.Fatalf("get bucket location status = %d, want %d, body=%s", got, want, locationRes.Body.String())
	}
	if !strings.Contains(locationRes.Body.String(), "us-east-1") {
		t.Fatalf("bucket location body = %q, want us-east-1", locationRes.Body.String())
	}
}

func TestSTSAssumeRoleAndSessionCanHeadBucket(t *testing.T) {
	runtime := newTestRuntime(t, true, true)
	defer func() { _ = runtime.Close() }()

	body := "Action=AssumeRole&RoleArn=arn%3Amockbucket%3Aiam%3A%3A%3Arole%2Fdata-reader&RoleSessionName=cli&Version=2011-06-15"
	stsReq := httptest.NewRequest(http.MethodPost, "http://mockbucket.local/", strings.NewReader(body))
	stsReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	signAWSRequest(t, stsReq, "sts", "us-east-1", "admin", "admin-secret", "")
	stsRes := httptest.NewRecorder()
	runtime.HTTPServer.Handler.ServeHTTP(stsRes, stsReq)
	if got, want := stsRes.Code, http.StatusOK; got != want {
		t.Fatalf("assume role status = %d, want %d, body=%s", got, want, stsRes.Body.String())
	}
	var response assumeRoleEnvelope
	if err := xml.Unmarshal(stsRes.Body.Bytes(), &response); err != nil {
		t.Fatalf("xml.Unmarshal() error = %v", err)
	}
	if response.Result.Credentials.AccessKeyID == "" || response.Result.Credentials.SessionToken == "" {
		t.Fatalf("assume role response missing credentials: %+v", response)
	}

	headReq := httptest.NewRequest(http.MethodHead, "http://mockbucket.local/demo", nil)
	signAWSRequest(t, headReq, "s3", "us-east-1", response.Result.Credentials.AccessKeyID, response.Result.Credentials.SecretAccessKey, response.Result.Credentials.SessionToken)
	headRes := httptest.NewRecorder()
	runtime.HTTPServer.Handler.ServeHTTP(headRes, headReq)
	if got, want := headRes.Code, http.StatusOK; got != want {
		t.Fatalf("session head bucket status = %d, want %d, body=%s", got, want, headRes.Body.String())
	}
}

func TestS3RejectsBadSignature(t *testing.T) {
	runtime := newTestRuntime(t, true, false)
	defer func() { _ = runtime.Close() }()
	req := httptest.NewRequest(http.MethodGet, "http://mockbucket.local/", nil)
	signAWSRequest(t, req, "s3", "us-east-1", "admin", "wrong-secret", "")
	res := httptest.NewRecorder()
	runtime.HTTPServer.Handler.ServeHTTP(res, req)
	if got, want := res.Code, http.StatusBadRequest; got != want {
		t.Fatalf("bad signature status = %d, want %d, body=%s", got, want, res.Body.String())
	}
}

type assumeRoleEnvelope struct {
	Result struct {
		Credentials struct {
			AccessKeyID     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
			SessionToken    string `xml:"SessionToken"`
		} `xml:"Credentials"`
	} `xml:"AssumeRoleResult"`
}

func newTestRuntime(t *testing.T, enableS3, enableSTS bool) *Runtime {
	t.Helper()
	cfg := baseConfig(t)
	cfg.Frontends.S3 = enableS3
	cfg.Frontends.STS = enableSTS
	runtime, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(testWriter{t}, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return runtime
}

func baseConfig(t *testing.T) config.Config {
	t.Helper()
	dir := t.TempDir()
	seedPath := filepath.Join(dir, "seed.yaml")
	seedContent := []byte(`buckets:
  - demo
principals:
  - name: admin
    policies:
      - statements:
          - effect: Allow
            actions: ["*"]
            resources: ["*"]
    access_keys:
      - id: admin
        secret: admin-secret
roles:
  - name: data-reader
    trust:
      statements:
        - effect: Allow
          principals: ["admin"]
          actions: ["sts:AssumeRole"]
    policies:
      - statements:
          - effect: Allow
            actions: ["s3:ListBucket", "s3:GetObject"]
            resources: ["arn:mockbucket:s3:::demo", "arn:mockbucket:s3:::demo/*"]
`)
	if err := osWriteFile(seedPath, seedContent); err != nil {
		t.Fatalf("write seed: %v", err)
	}
	cfg := config.Default()
	cfg.Storage.RootDir = filepath.Join(dir, "objects")
	cfg.Storage.SQLitePath = filepath.Join(dir, "mockbucket.db")
	cfg.Seed.Path = seedPath
	cfg.Server.RequestLog = false
	cfg.Server.ShutdownTimeout = time.Second
	return cfg
}

func signAWSRequest(t *testing.T, req *http.Request, service, region, accessKeyID, secretKey, sessionToken string) {
	t.Helper()
	if req.Host == "" {
		req.Host = req.URL.Host
	}
	payload := hashRequestBody(t, req)
	amzDate := "20260323T120000Z"
	date := amzDate[:8]
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payload)
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	if sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", sessionToken)
		signedHeaders = append(signedHeaders, "x-amz-security-token")
	}
	sort.Strings(signedHeaders)
	canonicalHeaders := canonicalHeaders(req, signedHeaders)
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI(req.URL.Path),
		canonicalQueryString(req.URL.Query()),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		payload,
	}, "\n")
	canonicalHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		strings.Join([]string{date, region, service, "aws4_request"}, "/"),
		hex.EncodeToString(canonicalHash[:]),
	}, "\n")
	signature := hex.EncodeToString(signingKey(secretKey, date, region, service, stringToSign))
	req.Header.Set("Authorization", fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s/%s/%s/aws4_request, SignedHeaders=%s, Signature=%s", accessKeyID, date, region, service, strings.Join(signedHeaders, ";"), signature))
}

func hashRequestBody(t *testing.T, req *http.Request) string {
	t.Helper()
	var body []byte
	if req.Body != nil {
		data, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatalf("ReadAll(body) error = %v", err)
		}
		body = data
		req.Body = io.NopCloser(bytes.NewReader(data))
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func canonicalHeaders(req *http.Request, signedHeaders []string) string {
	var lines []string
	for _, name := range signedHeaders {
		value := req.Header.Get(name)
		if name == "host" {
			value = req.Host
		}
		lines = append(lines, strings.ToLower(name)+":"+strings.Join(strings.Fields(value), " "))
	}
	return strings.Join(lines, "\n") + "\n"
}

func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	parts := strings.Split(path, "/")
	for i, part := range parts {
		parts[i] = escapeRFC3986(part)
	}
	joined := strings.Join(parts, "/")
	if !strings.HasPrefix(joined, "/") {
		return "/" + joined
	}
	return joined
}

func canonicalQueryString(values url.Values) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		vals := append([]string(nil), values[key]...)
		sort.Strings(vals)
		if len(vals) == 0 {
			parts = append(parts, escapeRFC3986(key)+"=")
			continue
		}
		for _, value := range vals {
			parts = append(parts, escapeRFC3986(key)+"="+escapeRFC3986(value))
		}
	}
	return strings.Join(parts, "&")
}

func escapeRFC3986(value string) string {
	escaped := url.QueryEscape(value)
	escaped = strings.ReplaceAll(escaped, "+", "%20")
	escaped = strings.ReplaceAll(escaped, "*", "%2A")
	escaped = strings.ReplaceAll(escaped, "%7E", "~")
	return escaped
}

func signingKey(secret, date, region, service, stringToSign string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return hmacSHA256(kSigning, stringToSign)
}

func hmacSHA256(key []byte, message string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(message))
	return mac.Sum(nil)
}

func osWriteFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
