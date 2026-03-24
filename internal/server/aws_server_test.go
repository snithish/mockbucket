package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	mbconfig "github.com/snithish/mockbucket/internal/config"
)

func TestS3FrontendContract(t *testing.T) {
	runFrontendContractTests(t, newS3ContractClient)
}

func TestSTSAssumeRoleAndSessionCanHeadBucket(t *testing.T) {
	runtime := newAWSTestRuntime(t, true)
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

func TestS3RejectsBadSignature(t *testing.T) {
	runtime := newAWSTestRuntime(t, false)
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

type s3ContractClient struct {
	svc *s3.Client
}

func newS3ContractClient(t *testing.T) frontendContractClient {
	t.Helper()
	runtime := newAWSTestRuntime(t, false)
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
	defer func() { _ = out.Body.Close() }()
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

func newAWSTestRuntime(t *testing.T, enableSTS bool) *Runtime {
	t.Helper()
	return newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.S3 = true
		cfg.Frontends.STS = enableSTS
		if enableSTS {
			if err := osWriteFile(cfg.Seed.Path, []byte(awsSTSTestSeedYAML)); err != nil {
				t.Fatalf("write sts seed: %v", err)
			}
		}
	})
}

func newS3Client(t *testing.T, runtime *Runtime, accessKeyID, secretKey, sessionToken string) *s3.Client {
	t.Helper()
	endpoint := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(endpoint.Close)
	cfg := newAWSConfig(t, accessKeyID, secretKey, sessionToken)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint.URL)
		o.UsePathStyle = true
	})
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

const awsSTSTestSeedYAML = `buckets:
  - demo
principals:
  - name: admin
    policies:
      - statements:
          - effect: Allow
            actions: ["*"]
            resources: ["*"]
s3:
  access_keys:
    - id: admin
      secret: admin-secret
      principal: admin
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
`
