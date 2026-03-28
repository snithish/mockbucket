package server

import (
	"context"
	"strings"
	"testing"
)

type contractObjectAttrs struct {
	ContentLength int64
	ETag          string
}

type contractListResult struct {
	Keys      []string
	NextToken string
	Truncated bool
}

type frontendContractClient interface {
	CreateBucket(ctx context.Context, bucket string) error
	HeadBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]string, error)
	PutObject(ctx context.Context, bucket, key, body string) (string, error)
	HeadObject(ctx context.Context, bucket, key string) (contractObjectAttrs, error)
	GetObject(ctx context.Context, bucket, key string) (string, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix string, maxKeys int32, continuationToken, startAfter string) (contractListResult, error)
	MultipartUpload(ctx context.Context, bucket, key string, parts []string) error
}

func runFrontendContractTests(t *testing.T, newClient func(t *testing.T) frontendContractClient) {
	t.Helper()

	t.Run("BucketLevelAPI", func(t *testing.T) {
		client := newClient(t)
		ctx := context.Background()

		// Seed visibility and bucket creation need to behave the same across S3 and GCS clients.
		listOut, err := client.ListBuckets(ctx)
		if err != nil {
			t.Fatalf("ListBuckets() error = %v", err)
		}
		if !containsString(listOut, "demo") {
			t.Fatalf("ListBuckets() missing demo bucket: %v", listOut)
		}

		if _, err := client.PutObject(ctx, "demo", "seed/probe.txt", "ok"); err != nil {
			t.Fatalf("PutObject(seed/probe.txt) error = %v", err)
		}

		if err := client.CreateBucket(ctx, "logs"); err != nil {
			t.Fatalf("CreateBucket() error = %v", err)
		}
		if err := client.HeadBucket(ctx, "logs"); err != nil {
			t.Fatalf("HeadBucket() error = %v", err)
		}

		listOut, err = client.ListBuckets(ctx)
		if err != nil {
			t.Fatalf("ListBuckets(after create) error = %v", err)
		}
		if !containsString(listOut, "logs") {
			t.Fatalf("ListBuckets() missing logs bucket: %v", listOut)
		}
	})

	t.Run("ObjectCRUD", func(t *testing.T) {
		client := newClient(t)
		ctx := context.Background()

		// Overwrite semantics are part of the emulator contract, not just basic upload/download plumbing.
		etag, err := client.PutObject(ctx, "demo", "logs/app.log", "hello")
		if err != nil {
			t.Fatalf("PutObject() error = %v", err)
		}
		if strings.TrimSpace(etag) == "" {
			t.Fatal("PutObject() missing ETag")
		}

		headOut, err := client.HeadObject(ctx, "demo", "logs/app.log")
		if err != nil {
			t.Fatalf("HeadObject() error = %v", err)
		}
		if got, want := headOut.ContentLength, int64(5); got != want {
			t.Fatalf("head content length = %d, want %d", got, want)
		}
		if strings.TrimSpace(headOut.ETag) == "" {
			t.Fatal("HeadObject() missing ETag")
		}

		body, err := client.GetObject(ctx, "demo", "logs/app.log")
		if err != nil {
			t.Fatalf("GetObject() error = %v", err)
		}
		if got, want := body, "hello"; got != want {
			t.Fatalf("GetObject() body = %q, want %q", got, want)
		}

		etag, err = client.PutObject(ctx, "demo", "logs/app.log", "goodbye")
		if err != nil {
			t.Fatalf("PutObject(update) error = %v", err)
		}
		if strings.TrimSpace(etag) == "" {
			t.Fatal("PutObject(update) missing ETag")
		}

		headOut, err = client.HeadObject(ctx, "demo", "logs/app.log")
		if err != nil {
			t.Fatalf("HeadObject(update) error = %v", err)
		}
		if got, want := headOut.ContentLength, int64(7); got != want {
			t.Fatalf("head content length after update = %d, want %d", got, want)
		}

		if err := client.DeleteObject(ctx, "demo", "logs/app.log"); err != nil {
			t.Fatalf("DeleteObject() error = %v", err)
		}
		if _, err := client.GetObject(ctx, "demo", "logs/app.log"); err == nil {
			t.Fatal("GetObject() after delete error = nil, want error")
		}
	})

	t.Run("ListObjects", func(t *testing.T) {
		client := newClient(t)
		ctx := context.Background()
		// Pagination and cursor semantics are shared behavior that both frontends should expose consistently.
		for _, key := range []string{
			"logs/2024-01.txt",
			"logs/2024-02.txt",
			"logs/2024-03.txt",
			"tmp/skip.txt",
		} {
			if _, err := client.PutObject(ctx, "demo", key, "data"); err != nil {
				t.Fatalf("PutObject(%s) error = %v", key, err)
			}
		}

		first, err := client.ListObjects(ctx, "demo", "logs/", 2, "", "")
		if err != nil {
			t.Fatalf("ListObjects(first page) error = %v", err)
		}
		if got, want := len(first.Keys), 2; got != want {
			t.Fatalf("first page keys = %d, want %d", got, want)
		}
		if first.NextToken == "" {
			t.Fatal("first page missing next token")
		}
		if !first.Truncated {
			t.Fatal("first page truncated = false, want true")
		}
		if got, want := first.Keys[0], "logs/2024-01.txt"; got != want {
			t.Fatalf("first key = %q, want %q", got, want)
		}
		if got, want := first.Keys[1], "logs/2024-02.txt"; got != want {
			t.Fatalf("second key = %q, want %q", got, want)
		}

		second, err := client.ListObjects(ctx, "demo", "logs/", 1000, first.NextToken, "")
		if err != nil {
			t.Fatalf("ListObjects(second page) error = %v", err)
		}
		if got, want := len(second.Keys), 1; got != want {
			t.Fatalf("second page keys = %d, want %d", got, want)
		}
		if second.Truncated {
			t.Fatal("second page truncated = true, want false")
		}
		if got, want := second.Keys[0], "logs/2024-03.txt"; got != want {
			t.Fatalf("second page key = %q, want %q", got, want)
		}

		startAfter, err := client.ListObjects(ctx, "demo", "logs/", 1000, "", "logs/2024-01.txt")
		if err != nil {
			t.Fatalf("ListObjects(startAfter) error = %v", err)
		}
		if got, want := len(startAfter.Keys), 2; got != want {
			t.Fatalf("startAfter keys = %d, want %d", got, want)
		}
		if got, want := startAfter.Keys[0], "logs/2024-02.txt"; got != want {
			t.Fatalf("startAfter first key = %q, want %q", got, want)
		}
		if got, want := startAfter.Keys[1], "logs/2024-03.txt"; got != want {
			t.Fatalf("startAfter second key = %q, want %q", got, want)
		}
	})

	t.Run("MultipartUpload", func(t *testing.T) {
		client := newClient(t)
		ctx := context.Background()
		parts := []string{"hello ", "world"}
		key := "multipart/data.txt"

		// Multipart completion must materialize a normal object that can be fetched through the same read path.
		if err := client.MultipartUpload(ctx, "demo", key, parts); err != nil {
			t.Fatalf("MultipartUpload() error = %v", err)
		}

		body, err := client.GetObject(ctx, "demo", key)
		if err != nil {
			t.Fatalf("GetObject() after multipart error = %v", err)
		}
		if got, want := body, strings.Join(parts, ""); got != want {
			t.Fatalf("multipart object body = %q, want %q", got, want)
		}
	})
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
