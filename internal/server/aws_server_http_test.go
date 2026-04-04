package server

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	mbconfig "github.com/snithish/mockbucket/internal/config"
)

func TestS3VirtualHostedStyleAccess(t *testing.T) {
	// This checks that virtual-hosted-style requests resolve bucket names from the host header.
	fixture := newAWSServerFixture(t, func(cfg *mbconfig.Config) {
		cfg.Server.Address = "localhost:9000"
	})
	virtualHostedClient := fixture.localhostS3Client(t, "admin", "admin-secret", "", false)
	ctx := context.Background()

	if _, err := virtualHostedClient.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("host-style.txt"),
		Body:   strings.NewReader("host-style"),
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
	body, err := readS3ObjectBody(getOut)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got, want := body, "host-style"; got != want {
		t.Fatalf("GetObject() body = %q, want %q", got, want)
	}
}

func TestS3PresignedRequestsSupportPutHeadAndGet(t *testing.T) {
	// This checks that presigned S3 URLs can write, inspect, and read objects without SDK credentials on the request.
	fixture := newAWSServerFixture(t, nil)
	presignClient := fixture.adminPresignClient(t, true)
	ctx := context.Background()

	putURL, err := presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("presigned.txt"),
	}, s3.WithPresignExpires(5*time.Minute))
	if err != nil {
		t.Fatalf("PresignPutObject() error = %v", err)
	}
	putReq := mustHTTPRequest(t, ctx, http.MethodPut, putURL.URL, strings.NewReader("presigned-body"))
	putResp := mustHTTPDo(t, putReq)
	defer putResp.Body.Close()
	if got, want := putResp.StatusCode, http.StatusOK; got != want {
		t.Fatalf("PUT presigned status = %d, want %d, body = %q", got, want, mustReadAll(t, putResp.Body))
	}

	headURL, err := presignClient.PresignHeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String("demo"),
		Key:    aws.String("presigned.txt"),
	}, s3.WithPresignExpires(5*time.Minute))
	if err != nil {
		t.Fatalf("PresignHeadObject() error = %v", err)
	}
	headResp := mustHTTPDo(t, mustHTTPRequest(t, ctx, http.MethodHead, headURL.URL, nil))
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
	getResp := mustHTTPDo(t, mustHTTPRequest(t, ctx, http.MethodGet, getURL.URL, nil))
	defer getResp.Body.Close()
	if got, want := getResp.StatusCode, http.StatusOK; got != want {
		t.Fatalf("GET presigned status = %d, want %d", got, want)
	}
	if got, want := mustReadAll(t, getResp.Body), "presigned-body"; got != want {
		t.Fatalf("GET presigned body = %q, want %q", got, want)
	}
}
