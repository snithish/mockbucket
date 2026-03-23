package server

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"cloud.google.com/go/storage"
	"golang.org/x/oauth2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func TestGCSBucketAndObjectFlow(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Frontends.GCS = true
	runtime, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(testWriter{t}, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = runtime.Close() }()

	endpoint := newTestHTTPServer(t, runtime)
	client := newGCSClient(t, endpoint)
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	projectID := "mock-project"

	bucketName := "logs"
	bucket := client.Bucket(bucketName)
	if err := bucket.Create(ctx, projectID, nil); err != nil {
		t.Fatalf("Create bucket error = %v", err)
	}

	bucketNames := listBucketNames(t, client, projectID)
	if !contains(bucketNames, "demo") || !contains(bucketNames, bucketName) {
		t.Fatalf("List buckets missing demo/logs: %v", bucketNames)
	}

	obj := bucket.Object("app/log.txt")
	writer := obj.NewWriter(ctx)
	writer.ChunkSize = 0
	writer.SendCRC32C = false
	if _, err := writer.Write([]byte("hello")); err != nil {
		t.Fatalf("write object: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close object writer: %v", err)
	}

	if _, err := runtime.Metadata.GetObject(ctx, bucketName, "app/log.txt"); err != nil {
		t.Fatalf("metadata missing after upload: %v", err)
	}

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		t.Fatalf("object attrs: %v", err)
	}
	if attrs.Name != "app/log.txt" || attrs.Size != 5 {
		t.Fatalf("attrs = %+v", attrs)
	}

	reader, err := obj.NewReader(ctx)
	if err != nil {
		t.Fatalf("new reader: %v", err)
	}
	defer func() { _ = reader.Close() }()
	bodyBytes, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	body := string(bodyBytes)
	if body != "hello" {
		t.Fatalf("object body = %q", body)
	}

	objects := listObjectNames(t, bucket, "app/")
	if len(objects) != 1 || objects[0] != "app/log.txt" {
		t.Fatalf("list objects = %v", objects)
	}
}

func newGCSClient(t *testing.T, endpoint string) *storage.Client {
	t.Helper()
	tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "admin"})
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

func newTestHTTPServer(t *testing.T, runtime *Runtime) string {
	t.Helper()
	server := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(server.Close)
	return server.URL
}

func listBucketNames(t *testing.T, client *storage.Client, projectID string) []string {
	t.Helper()
	var names []string
	it := client.Buckets(context.Background(), projectID)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("list buckets error = %v", err)
		}
		names = append(names, attrs.Name)
	}
	return names
}

func listObjectNames(t *testing.T, bucket *storage.BucketHandle, prefix string) []string {
	t.Helper()
	var names []string
	it := bucket.Objects(context.Background(), &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatalf("list objects error = %v", err)
		}
		names = append(names, attrs.Name)
	}
	return names
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
