package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

type gcsBucketList struct {
	Items []struct {
		Name string `json:"name"`
	} `json:"items"`
}

type gcsObject struct {
	Name string `json:"name"`
	Size string `json:"size"`
}

type gcsObjectList struct {
	Items []gcsObject `json:"items"`
}

func TestGCSBucketAndObjectFlow(t *testing.T) {
	cfg := baseConfig(t)
	cfg.Frontends.GCS = true
	runtime, err := New(context.Background(), cfg, slog.New(slog.NewTextHandler(testWriter{t}, nil)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	defer func() { _ = runtime.Close() }()

	endpoint := httptest.NewServer(runtime.HTTPServer.Handler)
	t.Cleanup(endpoint.Close)
	client := endpoint.Client()

	listReq, err := http.NewRequest(http.MethodGet, endpoint.URL+"/storage/v1/b", nil)
	if err != nil {
		t.Fatalf("list buckets request: %v", err)
	}
	listReq.Header.Set("Authorization", "Bearer admin")
	listRes, err := client.Do(listReq)
	if err != nil {
		t.Fatalf("list buckets error = %v", err)
	}
	defer listRes.Body.Close()
	if listRes.StatusCode != http.StatusOK {
		t.Fatalf("list buckets status = %d", listRes.StatusCode)
	}
	var listPayload gcsBucketList
	if err := json.NewDecoder(listRes.Body).Decode(&listPayload); err != nil {
		t.Fatalf("list buckets decode: %v", err)
	}
	found := false
	for _, item := range listPayload.Items {
		if item.Name == "demo" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("list buckets missing demo")
	}

	createBody := bytes.NewBufferString(`{"name":"logs"}`)
	createReq, err := http.NewRequest(http.MethodPost, endpoint.URL+"/storage/v1/b?project=test", createBody)
	if err != nil {
		t.Fatalf("create bucket request: %v", err)
	}
	createReq.Header.Set("Authorization", "Bearer admin")
	createReq.Header.Set("Content-Type", "application/json")
	createRes, err := client.Do(createReq)
	if err != nil {
		t.Fatalf("create bucket error = %v", err)
	}
	createRes.Body.Close()
	if createRes.StatusCode != http.StatusOK {
		t.Fatalf("create bucket status = %d", createRes.StatusCode)
	}

	uploadReq, err := http.NewRequest(http.MethodPost, endpoint.URL+"/upload/storage/v1/b/logs/o?uploadType=media&name=app/log.txt", bytes.NewBufferString("hello"))
	if err != nil {
		t.Fatalf("upload request: %v", err)
	}
	uploadReq.Header.Set("Authorization", "Bearer admin")
	uploadRes, err := client.Do(uploadReq)
	if err != nil {
		t.Fatalf("upload error = %v", err)
	}
	uploadRes.Body.Close()
	if uploadRes.StatusCode != http.StatusOK {
		t.Fatalf("upload status = %d", uploadRes.StatusCode)
	}

	metaReq, err := http.NewRequest(http.MethodGet, endpoint.URL+"/storage/v1/b/logs/o/app/log.txt", nil)
	if err != nil {
		t.Fatalf("metadata request: %v", err)
	}
	metaReq.Header.Set("Authorization", "Bearer admin")
	metaRes, err := client.Do(metaReq)
	if err != nil {
		t.Fatalf("metadata error = %v", err)
	}
	defer metaRes.Body.Close()
	if metaRes.StatusCode != http.StatusOK {
		t.Fatalf("metadata status = %d", metaRes.StatusCode)
	}
	var metaPayload gcsObject
	if err := json.NewDecoder(metaRes.Body).Decode(&metaPayload); err != nil {
		t.Fatalf("metadata decode: %v", err)
	}
	if metaPayload.Name != "app/log.txt" || metaPayload.Size != "5" {
		t.Fatalf("metadata = %+v", metaPayload)
	}

	getReq, err := http.NewRequest(http.MethodGet, endpoint.URL+"/storage/v1/b/logs/o/app/log.txt?alt=media", nil)
	if err != nil {
		t.Fatalf("get request: %v", err)
	}
	getReq.Header.Set("Authorization", "Bearer admin")
	getRes, err := client.Do(getReq)
	if err != nil {
		t.Fatalf("get error = %v", err)
	}
	defer getRes.Body.Close()
	body, err := io.ReadAll(getRes.Body)
	if err != nil {
		t.Fatalf("get read: %v", err)
	}
	if string(body) != "hello" {
		t.Fatalf("get body = %q", string(body))
	}

	listObjReq, err := http.NewRequest(http.MethodGet, endpoint.URL+"/storage/v1/b/logs/o?prefix=app/", nil)
	if err != nil {
		t.Fatalf("list objects request: %v", err)
	}
	listObjReq.Header.Set("Authorization", "Bearer admin")
	listObjRes, err := client.Do(listObjReq)
	if err != nil {
		t.Fatalf("list objects error = %v", err)
	}
	defer listObjRes.Body.Close()
	if listObjRes.StatusCode != http.StatusOK {
		t.Fatalf("list objects status = %d", listObjRes.StatusCode)
	}
	var listObjPayload gcsObjectList
	if err := json.NewDecoder(listObjRes.Body).Decode(&listObjPayload); err != nil {
		t.Fatalf("list objects decode: %v", err)
	}
	if len(listObjPayload.Items) != 1 || listObjPayload.Items[0].Name != "app/log.txt" {
		t.Fatalf("list objects payload = %+v", listObjPayload.Items)
	}
}
