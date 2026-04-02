package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"gopkg.in/yaml.v3"

	"github.com/snithish/mockbucket/internal/config"
	mbconfig "github.com/snithish/mockbucket/internal/config"
)

func TestS3FrontendContract(t *testing.T) {
	runFrontendContractTests(t, newS3ContractClient)
}

func TestSTSAssumeRoleAndSessionCanHeadBucket(t *testing.T) {
	runtime := newAWSTestRuntime(t)
	defer runtime.Close()

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

	sessionClient := newS3Client(
		t,
		runtime,
		aws.ToString(stsOut.Credentials.AccessKeyId),
		aws.ToString(stsOut.Credentials.SecretAccessKey),
		aws.ToString(stsOut.Credentials.SessionToken),
	)
	if _, err := sessionClient.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("demo")}); err != nil {
		t.Fatalf("HeadBucket() with session error = %v", err)
	}
}

func TestSTSGetCallerIdentityForLongLivedAndSessionCredentials(t *testing.T) {
	runtime := newAWSTestRuntime(t)
	defer runtime.Close()
	ctx := context.Background()

	stsClient := newSTSClient(t, runtime, "admin", "admin-secret", "")
	identityOut, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("GetCallerIdentity(long-lived) error = %v", err)
	}
	if got, want := aws.ToString(identityOut.Arn), "arn:mockbucket:iam:::user/admin"; got != want {
		t.Fatalf("GetCallerIdentity(long-lived) arn = %q, want %q", got, want)
	}
	if got, want := aws.ToString(identityOut.UserId), "admin"; got != want {
		t.Fatalf("GetCallerIdentity(long-lived) user id = %q, want %q", got, want)
	}

	assumeOut, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String("arn:mockbucket:iam:::role/data-reader"),
		RoleSessionName: aws.String("identity-check"),
	})
	if err != nil {
		t.Fatalf("AssumeRole() error = %v", err)
	}
	sessionSTS := newSTSClient(
		t,
		runtime,
		aws.ToString(assumeOut.Credentials.AccessKeyId),
		aws.ToString(assumeOut.Credentials.SecretAccessKey),
		aws.ToString(assumeOut.Credentials.SessionToken),
	)
	sessionIdentityOut, err := sessionSTS.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("GetCallerIdentity(session) error = %v", err)
	}
	if got, want := aws.ToString(sessionIdentityOut.Arn), "arn:mockbucket:sts:::assumed-role/data-reader/identity-check"; got != want {
		t.Fatalf("GetCallerIdentity(session) arn = %q, want %q", got, want)
	}
}

func TestSTSGetSessionTokenReturnsUsableTemporaryCredentials(t *testing.T) {
	runtime := newAWSTestRuntime(t)
	defer runtime.Close()
	ctx := context.Background()

	stsClient := newSTSClient(t, runtime, "admin", "admin-secret", "")
	sessionOut, err := stsClient.GetSessionToken(ctx, &sts.GetSessionTokenInput{
		DurationSeconds: aws.Int32(1800),
	})
	if err != nil {
		t.Fatalf("GetSessionToken() error = %v", err)
	}
	if sessionOut.Credentials == nil || aws.ToString(sessionOut.Credentials.AccessKeyId) == "" || aws.ToString(sessionOut.Credentials.SessionToken) == "" {
		t.Fatalf("GetSessionToken() missing credentials: %+v", sessionOut.Credentials)
	}
	if got := sessionOut.Credentials.Expiration.Sub(time.Now().UTC()); got < 25*time.Minute || got > 35*time.Minute {
		t.Fatalf("GetSessionToken() expiration delta = %v, want about 30m", got)
	}

	sessionSTS := newSTSClient(
		t,
		runtime,
		aws.ToString(sessionOut.Credentials.AccessKeyId),
		aws.ToString(sessionOut.Credentials.SecretAccessKey),
		aws.ToString(sessionOut.Credentials.SessionToken),
	)
	identityOut, err := sessionSTS.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		t.Fatalf("GetCallerIdentity(session token) error = %v", err)
	}
	if got, want := aws.ToString(identityOut.Arn), "arn:mockbucket:iam:::user/admin"; got != want {
		t.Fatalf("GetCallerIdentity(session token) arn = %q, want %q", got, want)
	}
}

func TestS3GetObjectNotFoundErrorsAreBucketAndKeySpecific(t *testing.T) {
	runtime := newAWSTestRuntime(t)
	defer runtime.Close()

	s3Client := newS3Client(t, runtime, "admin", "admin-secret", "")
	ctx := context.Background()

	_, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("missing-bucket"),
		Key:    aws.String("object.txt"),
	})
	assertAWSAPIErrorCode(t, err, "NoSuchBucket")

	_, err = s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("missing-object.txt"),
	})
	assertAWSAPIErrorCode(t, err, "NoSuchKey")
}

func TestS3VirtualHostedStyleAccess(t *testing.T) {
	runtime := newAWSTestRuntime(t)
	runtime.Config.Server.Address = "localhost:9000"
	virtualHostedClient := newLocalhostS3Client(t, runtime, "admin", "admin-secret", "", false)
	ctx := context.Background()

	if _, err := virtualHostedClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("host-style.txt"),
		Body:   bytes.NewReader([]byte("host-style")),
	}); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}

	headOut, err := virtualHostedClient.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("host-style.txt"),
	})
	if err != nil {
		t.Fatalf("HeadObject() error = %v", err)
	}
	if got, want := aws.ToInt64(headOut.ContentLength), int64(len("host-style")); got != want {
		t.Fatalf("HeadObject() content length = %d, want %d", got, want)
	}

	getOut, err := virtualHostedClient.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("host-style.txt"),
	})
	if err != nil {
		t.Fatalf("GetObject() error = %v", err)
	}
	defer getOut.Body.Close()
	body, err := io.ReadAll(getOut.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got, want := string(body), "host-style"; got != want {
		t.Fatalf("GetObject() body = %q, want %q", got, want)
	}
}

func TestS3PresignedGetPutAndHead(t *testing.T) {
	runtime := newAWSTestRuntime(t)
	presignClient := newS3PresignClient(t, runtime, "admin", "admin-secret", "", true)
	ctx := context.Background()

	putURL, err := presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("presigned.txt"),
	}, s3.WithPresignExpires(5*time.Minute))
	if err != nil {
		t.Fatalf("PresignPutObject() error = %v", err)
	}
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, putURL.URL, strings.NewReader("presigned-body"))
	if err != nil {
		t.Fatalf("NewRequestWithContext(PUT) error = %v", err)
	}
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT presigned request error = %v", err)
	}
	defer putResp.Body.Close()
	if got, want := putResp.StatusCode, http.StatusOK; got != want {
		body, _ := io.ReadAll(putResp.Body)
		t.Fatalf("PUT presigned status = %d, want %d, body = %q", got, want, string(body))
	}

	headURL, err := presignClient.PresignHeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("presigned.txt"),
	}, s3.WithPresignExpires(5*time.Minute))
	if err != nil {
		t.Fatalf("PresignHeadObject() error = %v", err)
	}
	headResp, err := http.DefaultClient.Do(mustHTTPRequest(t, ctx, http.MethodHead, headURL.URL, nil))
	if err != nil {
		t.Fatalf("HEAD presigned request error = %v", err)
	}
	defer headResp.Body.Close()
	if got, want := headResp.StatusCode, http.StatusOK; got != want {
		t.Fatalf("HEAD presigned status = %d, want %d", got, want)
	}
	if got, want := headResp.Header.Get("Content-Length"), strconv.Itoa(len("presigned-body")); got != want {
		t.Fatalf("HEAD presigned content length = %q, want %q", got, want)
	}

	getURL, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("presigned.txt"),
	}, s3.WithPresignExpires(5*time.Minute))
	if err != nil {
		t.Fatalf("PresignGetObject() error = %v", err)
	}
	getResp, err := http.DefaultClient.Do(mustHTTPRequest(t, ctx, http.MethodGet, getURL.URL, nil))
	if err != nil {
		t.Fatalf("GET presigned request error = %v", err)
	}
	defer getResp.Body.Close()
	if got, want := getResp.StatusCode, http.StatusOK; got != want {
		t.Fatalf("GET presigned status = %d, want %d", got, want)
	}
	body, err := io.ReadAll(getResp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got, want := string(body), "presigned-body"; got != want {
		t.Fatalf("GET presigned body = %q, want %q", got, want)
	}
}

func TestAWSPhaseScaffolding(t *testing.T) {
	t.Run("MetadataRoundTrip", func(t *testing.T) {
		t.Skip("Phase 3: persisted object metadata coverage")
	})
	t.Run("ConditionalHeaders", func(t *testing.T) {
		t.Skip("Phase 4: conditional request coverage")
	})
}

func TestS3AWSChunkedPutObject(t *testing.T) {
	runtime := newAWSTestRuntime(t)
	defer runtime.Close()

	tests := []struct {
		name     string
		key      string
		body     string
		wantBody string
		wantETag string
	}{
		{
			name:     "EmptyPayload",
			key:      "compat/pyspark/regular/_temporary/0/",
			body:     "0;chunk-signature=deadbeef\r\n\r\n",
			wantBody: "",
			wantETag: "\"d41d8cd98f00b204e9800998ecf8427e\"",
		},
		{
			name:     "NonEmptyPayload",
			key:      "compat/pyspark/regular/_temporary/part-0000",
			body:     "5;chunk-signature=deadbeef\r\nhello\r\n0;chunk-signature=feedface\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n",
			wantBody: "hello",
			wantETag: "\"5d41402abc4b2a76b9719d911017c592\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPut, "http://mockbucket.local/demo/"+tt.key, strings.NewReader(tt.body))
			req.Header.Set("Content-Encoding", "aws-chunked")
			req.Header.Set("X-Amz-Content-Sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
			req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(len(tt.wantBody)))
			res := httptest.NewRecorder()

			runtime.HTTPServer.Handler.ServeHTTP(res, req)

			if got, want := res.Code, http.StatusOK; got != want {
				body, _ := io.ReadAll(res.Body)
				t.Fatalf("status = %d, want %d, body = %q", got, want, string(body))
			}
			if got := res.Header().Get("ETag"); got != tt.wantETag {
				t.Fatalf("ETag = %q, want %q", got, tt.wantETag)
			}

			out, _, err := runtime.Objects.OpenObject(context.Background(), "demo", tt.key)
			if err != nil {
				t.Fatalf("OpenObject() error = %v", err)
			}
			defer out.Close()

			gotBody, err := io.ReadAll(out)
			if err != nil {
				t.Fatalf("ReadAll() error = %v", err)
			}
			if got, want := string(gotBody), tt.wantBody; got != want {
				t.Fatalf("body = %q, want %q", got, want)
			}

			meta, err := runtime.Metadata.GetObject(context.Background(), "demo", tt.key)
			if err != nil {
				t.Fatalf("GetObject(metadata) error = %v", err)
			}
			if got, want := `"`+meta.ETag+`"`, tt.wantETag; got != want {
				t.Fatalf("metadata ETag = %q, want %q", got, want)
			}
		})
	}
}

type s3ContractClient struct {
	svc *s3.Client
}

func newS3ContractClient(t *testing.T) frontendContractClient {
	t.Helper()
	runtime := newAWSTestRuntime(t)
	t.Cleanup(func() { _ = runtime.Close() })
	return &s3ContractClient{svc: newS3Client(t, runtime, "admin", "admin-secret", "")}
}

func (c *s3ContractClient) CreateBucket(ctx context.Context, bucket string) error {
	_, err := c.svc.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
	return err
}

func (c *s3ContractClient) HeadBucket(ctx context.Context, bucket string) error {
	_, err := c.svc.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	return err
}

func (c *s3ContractClient) ListBuckets(ctx context.Context) ([]string, error) {
	out, err := c.svc.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(out.Buckets))
	for _, b := range out.Buckets {
		names = append(names, aws.ToString(b.Name))
	}
	return names, nil
}

func (c *s3ContractClient) PutObject(ctx context.Context, bucket, key, body string) (string, error) {
	out, err := c.svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte(body)),
	})
	if err != nil {
		return "", err
	}
	return aws.ToString(out.ETag), nil
}

func (c *s3ContractClient) HeadObject(ctx context.Context, bucket, key string) (contractObjectAttrs, error) {
	out, err := c.svc.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return contractObjectAttrs{}, err
	}
	return contractObjectAttrs{ContentLength: aws.ToInt64(out.ContentLength), ETag: aws.ToString(out.ETag)}, nil
}

func (c *s3ContractClient) GetObject(ctx context.Context, bucket, key string) (string, error) {
	out, err := c.svc.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return "", err
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func (c *s3ContractClient) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.svc.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	return err
}

func (c *s3ContractClient) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int32, continuationToken, startAfter string) (contractListResult, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(maxKeys),
	}
	if continuationToken != "" {
		input.ContinuationToken = aws.String(continuationToken)
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}
	out, err := c.svc.ListObjectsV2(ctx, input)
	if err != nil {
		return contractListResult{}, err
	}
	keys := make([]string, 0, len(out.Contents))
	for _, obj := range out.Contents {
		keys = append(keys, aws.ToString(obj.Key))
	}
	return contractListResult{Keys: keys, NextToken: aws.ToString(out.NextContinuationToken), Truncated: aws.ToBool(out.IsTruncated)}, nil
}

func (c *s3ContractClient) MultipartUpload(ctx context.Context, bucket, key string, parts []string) error {
	createOut, err := c.svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return err
	}
	uploadID := aws.ToString(createOut.UploadId)
	completed := make([]s3types.CompletedPart, 0, len(parts))
	for i, part := range parts {
		out, err := c.svc.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(int32(i + 1)),
			Body:       bytes.NewReader([]byte(part)),
		})
		if err != nil {
			return err
		}
		completed = append(completed, s3types.CompletedPart{PartNumber: aws.Int32(int32(i + 1)), ETag: out.ETag})
	}
	_, err = c.svc.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: completed,
		},
	})
	return err
}

func newAWSTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	return newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.Type = config.FrontendS3
		if err := yaml.Unmarshal([]byte(awsSTSTestSeedYAML), &cfg.Seed); err != nil {
			t.Fatalf("parse sts seed: %v", err)
		}
	})
}

func newS3Client(t *testing.T, runtime *Runtime, accessKeyID, secretKey, sessionToken string) *s3.Client {
	t.Helper()
	return newS3ClientWithAddressing(t, runtime, accessKeyID, secretKey, sessionToken, true)
}

func newS3ClientWithAddressing(t *testing.T, runtime *Runtime, accessKeyID, secretKey, sessionToken string, usePathStyle bool) *s3.Client {
	t.Helper()
	endpoint := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(endpoint.Close)
	return newS3ClientWithBaseEndpoint(t, accessKeyID, secretKey, sessionToken, endpoint.URL, usePathStyle)
}

func newLocalhostS3Client(t *testing.T, runtime *Runtime, accessKeyID, secretKey, sessionToken string, usePathStyle bool) *s3.Client {
	t.Helper()
	endpoint := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(endpoint.Close)
	endpointURL := mustParseURL(t, endpoint.URL)
	endpointURL.Host = "localhost:" + endpointURL.Port()
	return newS3ClientWithBaseEndpoint(t, accessKeyID, secretKey, sessionToken, endpointURL.String(), usePathStyle)
}

func newS3ClientWithBaseEndpoint(t *testing.T, accessKeyID, secretKey, sessionToken, baseEndpoint string, usePathStyle bool) *s3.Client {
	t.Helper()
	cfg := newAWSConfig(t, accessKeyID, secretKey, sessionToken)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(baseEndpoint)
		o.UsePathStyle = usePathStyle
	})
}

func newS3PresignClient(t *testing.T, runtime *Runtime, accessKeyID, secretKey, sessionToken string, usePathStyle bool) *s3.PresignClient {
	t.Helper()
	return s3.NewPresignClient(newS3ClientWithAddressing(t, runtime, accessKeyID, secretKey, sessionToken, usePathStyle))
}

func newSTSClient(t *testing.T, runtime *Runtime, accessKeyID, secretKey, sessionToken string) *sts.Client {
	t.Helper()
	endpoint := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(endpoint.Close)
	cfg := newAWSConfig(t, accessKeyID, secretKey, sessionToken)
	return sts.NewFromConfig(cfg, func(o *sts.Options) {
		o.BaseEndpoint = aws.String(endpoint.URL)
	})
}

func newAWSConfig(t *testing.T, accessKeyID, secretKey, sessionToken string) aws.Config {
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

func assertAWSAPIErrorCode(t *testing.T, err error, wantCode string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s", wantCode)
	}
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error type = %T, want smithy.APIError", err)
	}
	if got := apiErr.ErrorCode(); got != wantCode {
		t.Fatalf("error code = %q, want %q", got, wantCode)
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("url.Parse(%q) error = %v", raw, err)
	}
	return parsed
}

func mustHTTPRequest(t *testing.T, ctx context.Context, method, rawURL string, body io.Reader) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, method, rawURL, body)
	if err != nil {
		t.Fatalf("NewRequestWithContext(%s, %q) error = %v", method, rawURL, err)
	}
	return req
}

const awsSTSTestSeedYAML = `buckets:
  - demo
roles:
  - name: data-reader
s3:
  access_keys:
    - id: admin
      secret: admin-secret
    - id: restricted
      secret: restricted-secret
      allowed_roles:
        - data-reader
`
