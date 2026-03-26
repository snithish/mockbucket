package server

import (
	"context"
	"encoding/base64"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/snithish/mockbucket/internal/config"
	mbconfig "github.com/snithish/mockbucket/internal/config"
)

func TestAzureFrontendContract(t *testing.T) {
	runFrontendContractTests(t, newAzureBlobContractClient)
}

type azureBlobContractClient struct {
	svc *azblob.Client
}

func newAzureBlobContractClient(t *testing.T) frontendContractClient {
	t.Helper()
	runtime := newAzureTestRuntime(t)
	t.Cleanup(func() { _ = runtime.Close() })

	endpoint := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(endpoint.Close)

	accountName := "mockstorage"
	accountKey := base64.StdEncoding.EncodeToString([]byte("mockstorage-key-32bytes!!"))

	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		t.Fatalf("NewSharedKeyCredential() error = %v", err)
	}

	svc, err := azblob.NewClientWithSharedKeyCredential(
		endpoint.URL,
		cred,
		nil,
	)
	if err != nil {
		t.Fatalf("NewClientWithSharedKeyCredential() error = %v", err)
	}

	return &azureBlobContractClient{svc: svc}
}

func (c *azureBlobContractClient) CreateBucket(ctx context.Context, bucket string) error {
	_, err := c.svc.CreateContainer(ctx, bucket, nil)
	return err
}

func (c *azureBlobContractClient) HeadBucket(ctx context.Context, bucket string) error {
	containerClient := c.svc.ServiceClient().NewContainerClient(bucket)
	_, err := containerClient.GetProperties(ctx, nil)
	return err
}

func (c *azureBlobContractClient) ListBuckets(ctx context.Context) ([]string, error) {
	pager := c.svc.NewListContainersPager(nil)
	names := make([]string, 0)
	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, c := range resp.ContainerItems {
			if c.Name != nil {
				names = append(names, *c.Name)
			}
		}
	}
	return names, nil
}

func (c *azureBlobContractClient) PutObject(ctx context.Context, bucket, key, body string) (string, error) {
	_, err := c.svc.UploadBuffer(ctx, bucket, key, []byte(body), nil)
	if err != nil {
		return "", err
	}
	return key, nil
}

func (c *azureBlobContractClient) HeadObject(ctx context.Context, bucket, key string) (contractObjectAttrs, error) {
	containerClient := c.svc.ServiceClient().NewContainerClient(bucket)
	blobClient := containerClient.NewBlockBlobClient(key)
	resp, err := blobClient.GetProperties(ctx, nil)
	if err != nil {
		return contractObjectAttrs{}, err
	}
	return contractObjectAttrs{
		ContentLength: *resp.ContentLength,
		ETag:          string(*resp.ETag),
	}, nil
}

func (c *azureBlobContractClient) GetObject(ctx context.Context, bucket, key string) (string, error) {
	resp, err := c.svc.DownloadStream(ctx, bucket, key, nil)
	if err != nil {
		return "", err
	}
	body, err := io.ReadAll(resp.Body)
	return string(body), err
}

func (c *azureBlobContractClient) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.svc.DeleteBlob(ctx, bucket, key, nil)
	return err
}

func (c *azureBlobContractClient) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int32, continuationToken, startAfter string) (contractListResult, error) {
	pagerOpts := &container.ListBlobsFlatOptions{
		Prefix:     to.Ptr(prefix),
		MaxResults: to.Ptr(maxKeys),
	}
	if startAfter != "" {
		pagerOpts.Marker = to.Ptr(startAfter)
	} else if continuationToken != "" {
		pagerOpts.Marker = to.Ptr(continuationToken)
	}

	pager := c.svc.NewListBlobsFlatPager(bucket, pagerOpts)

	keys := make([]string, 0)
	var nextToken string
	truncated := false

	if !pager.More() {
		return contractListResult{Keys: keys, NextToken: nextToken, Truncated: truncated}, nil
	}

	resp, err := pager.NextPage(ctx)
	if err != nil {
		return contractListResult{}, err
	}
	for _, item := range resp.Segment.BlobItems {
		if item.Name != nil {
			keys = append(keys, *item.Name)
		}
	}
	if resp.NextMarker != nil && *resp.NextMarker != "" && len(keys) >= int(maxKeys) {
		nextToken = *resp.NextMarker
		truncated = true
	}

	return contractListResult{
		Keys:      keys,
		NextToken: nextToken,
		Truncated: truncated,
	}, nil
}

func (c *azureBlobContractClient) MultipartUpload(ctx context.Context, bucket, key string, parts []string) error {
	body := strings.Join(parts, "")
	_, err := c.svc.UploadBuffer(ctx, bucket, key, []byte(body), nil)
	return err
}

func newAzureTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	return newTestRuntime(t, func(cfg *mbconfig.Config) {
		cfg.Frontends.Type = config.FrontendAzureBlob
		cfg.Azure.Account = "mockstorage"
		cfg.Azure.Key = base64.StdEncoding.EncodeToString([]byte("mockstorage-key-32bytes!!"))
	})
}
