package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"gopkg.in/yaml.v3"

	mbconfig "github.com/snithish/mockbucket/internal/config"
)

type awsServerFixture struct {
	runtime *Runtime
}

type s3ContractClient struct {
	svc *s3.Client
}

func newAWSServerFixture(t *testing.T, configure func(*mbconfig.Config)) *awsServerFixture {
	t.Helper()
	runtime := newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.Type = mbconfig.FrontendS3
		if err := yaml.Unmarshal([]byte(awsSTSTestSeedYAML), &cfg.Seed); err != nil {
			t.Fatalf("parse sts seed: %v", err)
		}
		if configure != nil {
			configure(cfg)
		}
	})
	return &awsServerFixture{runtime: runtime}
}

func (f *awsServerFixture) adminS3Client(t *testing.T) *s3.Client {
	t.Helper()
	return f.s3Client(t, "admin", "admin-secret", "")
}

func (f *awsServerFixture) s3Client(t *testing.T, accessKeyID, secretKey, sessionToken string) *s3.Client {
	t.Helper()
	return f.s3ClientWithAddressing(t, accessKeyID, secretKey, sessionToken, true)
}

func (f *awsServerFixture) s3ClientWithAddressing(t *testing.T, accessKeyID, secretKey, sessionToken string, usePathStyle bool) *s3.Client {
	t.Helper()
	endpoint := newHTTPTestServer(t, f.runtime.HTTPServer.Handler)
	return newS3ClientWithBaseEndpoint(t, accessKeyID, secretKey, sessionToken, endpoint.URL, usePathStyle)
}

func (f *awsServerFixture) localhostS3Client(t *testing.T, accessKeyID, secretKey, sessionToken string, usePathStyle bool) *s3.Client {
	t.Helper()
	endpoint := newHTTPTestServer(t, f.runtime.HTTPServer.Handler)
	endpointURL := mustParseURL(t, endpoint.URL)
	endpointURL.Host = "localhost:" + endpointURL.Port()
	return newS3ClientWithBaseEndpoint(t, accessKeyID, secretKey, sessionToken, endpointURL.String(), usePathStyle)
}

func (f *awsServerFixture) adminPresignClient(t *testing.T, usePathStyle bool) *s3.PresignClient {
	t.Helper()
	return f.presignClient(t, "admin", "admin-secret", "", usePathStyle)
}

func (f *awsServerFixture) presignClient(t *testing.T, accessKeyID, secretKey, sessionToken string, usePathStyle bool) *s3.PresignClient {
	t.Helper()
	return s3.NewPresignClient(f.s3ClientWithAddressing(t, accessKeyID, secretKey, sessionToken, usePathStyle))
}

func (f *awsServerFixture) adminSTSClient(t *testing.T) *sts.Client {
	t.Helper()
	return f.stsClient(t, "admin", "admin-secret", "")
}

func (f *awsServerFixture) stsClient(t *testing.T, accessKeyID, secretKey, sessionToken string) *sts.Client {
	t.Helper()
	endpoint := newHTTPTestServer(t, f.runtime.HTTPServer.Handler)
	cfg := newAWSConfig(t, accessKeyID, secretKey, sessionToken)
	return sts.NewFromConfig(cfg, func(o *sts.Options) {
		o.BaseEndpoint = aws.String(endpoint.URL)
	})
}

func (f *awsServerFixture) serve(req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	f.runtime.HTTPServer.Handler.ServeHTTP(rec, req)
	return rec
}

func (f *awsServerFixture) mustObjectBody(t *testing.T, key string) string {
	t.Helper()
	out, _, err := f.runtime.Objects.OpenObject(context.Background(), "demo", key)
	if err != nil {
		t.Fatalf("OpenObject() error = %v", err)
	}
	defer out.Close()
	return mustReadAll(t, out)
}

func (f *awsServerFixture) mustObjectETag(t *testing.T, key string) string {
	t.Helper()
	meta, err := f.runtime.Metadata.GetObject(context.Background(), "demo", key)
	if err != nil {
		t.Fatalf("GetObject(metadata) error = %v", err)
	}
	return `"` + meta.ETag + `"`
}

func newS3ContractClient(t *testing.T) frontendContractClient {
	t.Helper()
	fixture := newAWSServerFixture(t, nil)
	return &s3ContractClient{svc: fixture.adminS3Client(t)}
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
	for _, bucket := range out.Buckets {
		names = append(names, aws.ToString(bucket.Name))
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
	return contractObjectAttrs{
		ContentLength: aws.ToInt64(out.ContentLength),
		ETag:          aws.ToString(out.ETag),
	}, nil
}

func (c *s3ContractClient) GetObject(ctx context.Context, bucket, key string) (string, error) {
	out, err := c.svc.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return "", err
	}
	return readS3ObjectBody(out)
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
	return contractListResult{
		Keys:      keys,
		NextToken: aws.ToString(out.NextContinuationToken),
		Truncated: aws.ToBool(out.IsTruncated),
	}, nil
}

func (c *s3ContractClient) MultipartUpload(ctx context.Context, bucket, key string, parts []string) error {
	createOut, err := c.svc.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	uploadID := aws.ToString(createOut.UploadId)
	completed := make([]s3types.CompletedPart, 0, len(parts))
	for index, part := range parts {
		out, err := c.svc.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(int32(index + 1)),
			Body:       bytes.NewReader([]byte(part)),
		})
		if err != nil {
			return err
		}
		completed = append(completed, s3types.CompletedPart{
			PartNumber: aws.Int32(int32(index + 1)),
			ETag:       out.ETag,
		})
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

func newS3ClientWithBaseEndpoint(t *testing.T, accessKeyID, secretKey, sessionToken, baseEndpoint string, usePathStyle bool) *s3.Client {
	t.Helper()
	cfg := newAWSConfig(t, accessKeyID, secretKey, sessionToken)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(baseEndpoint)
		o.UsePathStyle = usePathStyle
	})
}

func newAWSConfig(t *testing.T, accessKeyID, secretKey, sessionToken string) aws.Config {
	t.Helper()
	creds := credentials.NewStaticCredentialsProvider(accessKeyID, secretKey, sessionToken)
	cfg, err := awscfg.LoadDefaultConfig(
		context.Background(),
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

func readS3ObjectBody(out *s3.GetObjectOutput) (string, error) {
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return "", err
	}
	return string(body), nil
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
