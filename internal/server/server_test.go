package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	mbconfig "github.com/snithish/mockbucket/internal/config"
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

	svc := newS3Client(t, runtime, "admin", "admin-secret", "")
	ctx := context.Background()

	listOut, err := svc.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		t.Fatalf("ListBuckets() error = %v", err)
	}
	found := false
	for _, b := range listOut.Buckets {
		if aws.ToString(b.Name) == "demo" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("ListBuckets() missing demo bucket")
	}

	if _, err := svc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("logs")}); err != nil {
		t.Fatalf("CreateBucket() error = %v", err)
	}
	if _, err := svc.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("logs")}); err != nil {
		t.Fatalf("HeadBucket() error = %v", err)
	}
	locOut, err := svc.GetBucketLocation(ctx, &s3.GetBucketLocationInput{Bucket: aws.String("logs")})
	if err != nil {
		t.Fatalf("GetBucketLocation() error = %v", err)
	}
	if locOut.LocationConstraint != "us-east-1" {
		t.Fatalf("location = %s, want us-east-1", locOut.LocationConstraint)
	}
}

func TestS3ObjectCRUD(t *testing.T) {
	runtime := newTestRuntime(t, true, false)
	defer func() { _ = runtime.Close() }()
	svc := newS3Client(t, runtime, "admin", "admin-secret", "")
	ctx := context.Background()

	putOut, err := svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("logs/app.log"),
		Body:   strings.NewReader("hello"),
	})
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	if aws.ToString(putOut.ETag) == "" {
		t.Fatal("PutObject() missing ETag")
	}

	headOut, err := svc.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("logs/app.log"),
	})
	if err != nil {
		t.Fatalf("HeadObject() error = %v", err)
	}
	if aws.ToInt64(headOut.ContentLength) != 5 {
		t.Fatalf("head content length = %d, want 5", aws.ToInt64(headOut.ContentLength))
	}
	if aws.ToString(headOut.ETag) == "" {
		t.Fatal("head ETag missing")
	}

	getOut, err := svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("logs/app.log"),
	})
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	body, err := io.ReadAll(getOut.Body)
	_ = getOut.Body.Close()
	if err != nil {
		t.Fatalf("GetObject() read error = %v", err)
	}
	if string(body) != "hello" {
		t.Fatalf("get object body = %q, want hello", string(body))
	}

	putOut, err = svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("logs/app.log"),
		Body:   strings.NewReader("goodbye"),
	})
	if err != nil {
		t.Fatalf("PutObject(update) error = %v", err)
	}
	if aws.ToString(putOut.ETag) == "" {
		t.Fatal("update ETag missing")
	}

	headOut, err = svc.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("logs/app.log"),
	})
	if err != nil {
		t.Fatalf("HeadObject(update) error = %v", err)
	}
	if aws.ToInt64(headOut.ContentLength) != 7 {
		t.Fatalf("head content length after update = %d, want 7", aws.ToInt64(headOut.ContentLength))
	}

	if _, err := svc.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("logs/app.log"),
	}); err != nil {
		t.Fatalf("DeleteObject() error = %v", err)
	}

	_, err = svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("logs/app.log"),
	})
	if err == nil {
		t.Fatal("GetObject() after delete error = nil, want error")
	}
	var respErr *smithyhttp.ResponseError
	if !errors.As(err, &respErr) {
		t.Fatalf("GetObject() after delete error = %v, want response error", err)
	}
}

func TestSTSAssumeRoleAndSessionCanHeadBucket(t *testing.T) {
	runtime := newTestRuntime(t, true, true)
	defer func() { _ = runtime.Close() }()
	stsClient := newSTSClient(t, runtime, "admin", "admin-secret", "")
	ctx := context.Background()
	stsOut, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String("arn:mockbucket:iam:::role/data-reader"),
		RoleSessionName: aws.String("cli"),
	})
	if err != nil {
		t.Fatalf("AssumeRole() error = %v", err)
	}
	if stsOut.Credentials == nil || aws.ToString(stsOut.Credentials.AccessKeyId) == "" || aws.ToString(stsOut.Credentials.SessionToken) == "" {
		t.Fatalf("AssumeRole() missing credentials: %+v", stsOut.Credentials)
	}

	sessionClient := newS3Client(t, runtime, aws.ToString(stsOut.Credentials.AccessKeyId), aws.ToString(stsOut.Credentials.SecretAccessKey), aws.ToString(stsOut.Credentials.SessionToken))
	if _, err := sessionClient.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("demo")}); err != nil {
		t.Fatalf("HeadBucket() with session error = %v", err)
	}
}

func TestS3RejectsBadSignature(t *testing.T) {
	runtime := newTestRuntime(t, true, false)
	defer func() { _ = runtime.Close() }()
	svc := newS3Client(t, runtime, "admin", "wrong-secret", "")
	_, err := svc.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	if err == nil {
		t.Fatal("ListBuckets() error = nil, want signature error")
	}
	var respErr *smithyhttp.ResponseError
	if !errors.As(err, &respErr) || respErr.HTTPStatusCode() != http.StatusBadRequest {
		t.Fatalf("ListBuckets() error = %v, want 400 response error", err)
	}
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

func baseConfig(t *testing.T) mbconfig.Config {
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
	cfg := mbconfig.Default()
	cfg.Storage.RootDir = filepath.Join(dir, "objects")
	cfg.Storage.SQLitePath = filepath.Join(dir, "mockbucket.db")
	cfg.Seed.Path = seedPath
	cfg.Server.RequestLog = false
	cfg.Server.ShutdownTimeout = time.Second
	return cfg
}

func newS3Client(t *testing.T, runtime *Runtime, accessKeyID, secretKey, sessionToken string) *s3.Client {
	t.Helper()
	endpoint := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(endpoint.Close)
	cfg := newAWSConfig(t, endpoint.URL, accessKeyID, secretKey, sessionToken)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint.URL)
		o.UsePathStyle = true
	})
}

func newSTSClient(t *testing.T, runtime *Runtime, accessKeyID, secretKey, sessionToken string) *sts.Client {
	t.Helper()
	endpoint := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(endpoint.Close)
	cfg := newAWSConfig(t, endpoint.URL, accessKeyID, secretKey, sessionToken)
	return sts.NewFromConfig(cfg, func(o *sts.Options) {
		o.BaseEndpoint = aws.String(endpoint.URL)
	})
}

func newAWSConfig(t *testing.T, endpointURL, accessKeyID, secretKey, sessionToken string) aws.Config {
	t.Helper()
	creds := credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, sessionToken)
	cfg, err := awscfg.LoadDefaultConfig(context.Background(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(creds),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig() error = %v", err)
	}
	return cfg
}

func osWriteFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0o644)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(string(p))
	return len(p), nil
}
