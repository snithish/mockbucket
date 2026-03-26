package server

import (
	"context"
	"encoding/base64"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	azdataservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"

	"github.com/snithish/mockbucket/internal/config"
	mbconfig "github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/seed"
)

func TestAzureDataLakeFrontendContract(t *testing.T) {
	t.Run("FilesystemLevelAPI", func(t *testing.T) {
		runtime := newAzureDataLakeTestRuntime(t)
		t.Cleanup(func() { _ = runtime.Close() })

		endpoint := httptest.NewServer(runtime.HTTPServer.Handler)
		t.Cleanup(endpoint.Close)

		accountName := "mockstorage"
		accountKey := base64.StdEncoding.EncodeToString([]byte("mockstorage-key-32bytes!!"))

		cred, err := azdatalake.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			t.Fatalf("NewSharedKeyCredential() error = %v", err)
		}

		svc, err := azdataservice.NewClientWithSharedKeyCredential(
			endpoint.URL,
			cred,
			nil,
		)
		if err != nil {
			t.Fatalf("NewClientWithSharedKeyCredential() error = %v", err)
		}

		ctx := context.Background()

		// List filesystems
		pager := svc.NewListFileSystemsPager(nil)
		var names []string
		for pager.More() {
			resp, err := pager.NextPage(ctx)
			if err != nil {
				t.Fatalf("NextPage() error = %v", err)
			}
			for _, fs := range resp.ListFileSystemsSegmentResponse.FileSystemItems {
				if fs.Name != nil {
					names = append(names, *fs.Name)
				}
			}
		}
		if len(names) != 1 || names[0] != "demo" {
			t.Fatalf("filesystems = %v, want [demo]", names)
		}

		// Create filesystem
		_, err = svc.CreateFileSystem(ctx, "testfs", nil)
		if err != nil {
			t.Fatalf("CreateFileSystem() error = %v", err)
		}

		// List filesystems again - should contain demo and testfs
		pager = svc.NewListFileSystemsPager(nil)
		names = make([]string, 0)
		for pager.More() {
			resp, err := pager.NextPage(ctx)
			if err != nil {
				t.Fatalf("NextPage() error = %v", err)
			}
			for _, fs := range resp.ListFileSystemsSegmentResponse.FileSystemItems {
				if fs.Name != nil {
					names = append(names, *fs.Name)
				}
			}
		}
		if len(names) != 2 {
			t.Fatalf("filesystems = %v, want 2 filesystems", names)
		}

		// Get filesystem properties
		fsClient := svc.NewFileSystemClient("testfs")
		_, err = fsClient.GetProperties(ctx, nil)
		if err != nil {
			t.Fatalf("GetProperties() error = %v", err)
		}

		// Delete filesystem
		_, err = svc.DeleteFileSystem(ctx, "testfs", nil)
		if err != nil {
			t.Fatalf("DeleteFileSystem() error = %v", err)
		}
	})

	t.Run("FileOperations", func(t *testing.T) {
		runtime := newAzureDataLakeTestRuntime(t)
		t.Cleanup(func() { _ = runtime.Close() })

		endpoint := httptest.NewServer(runtime.HTTPServer.Handler)
		t.Cleanup(endpoint.Close)

		accountName := "mockstorage"
		accountKey := base64.StdEncoding.EncodeToString([]byte("mockstorage-key-32bytes!!"))

		cred, err := azdatalake.NewSharedKeyCredential(accountName, accountKey)
		if err != nil {
			t.Fatalf("NewSharedKeyCredential() error = %v", err)
		}

		svc, err := azdataservice.NewClientWithSharedKeyCredential(
			endpoint.URL,
			cred,
			nil,
		)
		if err != nil {
			t.Fatalf("NewClientWithSharedKeyCredential() error = %v", err)
		}

		ctx := context.Background()

		// Create filesystem
		_, err = svc.CreateFileSystem(ctx, "testfs", nil)
		if err != nil {
			t.Fatalf("CreateFileSystem() error = %v", err)
		}

		// Create file via filesystem client
		fsClient := svc.NewFileSystemClient("testfs")
		fileClient := fsClient.NewFileClient("test.txt")
		_, err = fileClient.Create(ctx, nil)
		if err != nil {
			t.Fatalf("CreateFile() error = %v", err)
		}

		// Append data
		data := []byte("hello world")
		dataReader := &noopReadSeekCloser{Reader: strings.NewReader(string(data))}
		_, err = fileClient.AppendData(ctx, 0, dataReader, nil)
		if err != nil {
			t.Fatalf("AppendData() error = %v", err)
		}

		// Flush data
		_, err = fileClient.FlushData(ctx, int64(len(data)), nil)
		if err != nil {
			t.Fatalf("FlushData() error = %v", err)
		}

		moreData := []byte("!!!")
		moreReader := &noopReadSeekCloser{Reader: strings.NewReader(string(moreData))}
		_, err = fileClient.AppendData(ctx, int64(len(data)), moreReader, nil)
		if err != nil {
			t.Fatalf("AppendData(second) error = %v", err)
		}
		_, err = fileClient.FlushData(ctx, int64(len(data)+len(moreData)), nil)
		if err != nil {
			t.Fatalf("FlushData(second) error = %v", err)
		}

		// Read file
		resp, err := fileClient.DownloadStream(ctx, nil)
		if err != nil {
			t.Fatalf("ReadFile() error = %v", err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		if string(body) != "hello world!!!" {
			t.Fatalf("body = %q, want %q", string(body), "hello world!!!")
		}

		// Delete file
		_, err = fileClient.Delete(ctx, nil)
		if err != nil {
			t.Fatalf("DeleteFile() error = %v", err)
		}
	})
}

func newAzureDataLakeTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	return newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.Type = config.FrontendAzureDataLake
		cfg.Seed.Azure.Accounts = []seed.AzureAccountSeed{
			{
				Name: "mockstorage",
				Key:  base64.StdEncoding.EncodeToString([]byte("mockstorage-key-32bytes!!")),
			},
		}
	})
}

// noopReadSeekCloser wraps a strings.Reader to implement io.ReadSeekCloser
type noopReadSeekCloser struct {
	*strings.Reader
}

func (r *noopReadSeekCloser) Close() error {
	return nil
}
