package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/storage"
	"github.com/snithish/mockbucket/internal/storagetest"
)

func TestPutObjectRollsBackOnMetadataFailure(t *testing.T) {
	fixture := newS3StoreFixture(t)
	meta := &storagetest.FailingMetadataStore{Bucket: "demo", PutErr: errors.New("db down")}
	deps := common.Dependencies{Metadata: meta, Objects: fixture.objects}

	req := httptest.NewRequest(http.MethodPut, "/demo/file.txt", bytes.NewBufferString("payload"))
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "file.txt")
	rec := httptest.NewRecorder()

	handlePutObject(rec, req, deps, "demo", "file.txt")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	// The object bytes must be rolled back when metadata persistence fails.
	if _, _, err := fixture.objects.OpenObject(fixture.ctx, "demo", "file.txt"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expected object rollback, got %v", err)
	}
}

func TestPutObjectDecodesAWSChunkedEmptyPayload(t *testing.T) {
	fixture := newS3StoreFixture(t)
	deps := fixture.deps()

	req := httptest.NewRequest(http.MethodPut, "/demo/empty/", strings.NewReader("0;chunk-signature=deadbeef\r\n\r\n"))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "empty/")
	rec := httptest.NewRecorder()

	handlePutObject(rec, req, deps, "demo", "empty/")

	// Empty aws-chunked uploads must still produce a persisted zero-byte object with a stable MD5 ETag.
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("ETag"); got != "\"d41d8cd98f00b204e9800998ecf8427e\"" {
		t.Fatalf("ETag = %q, want %q", got, "\"d41d8cd98f00b204e9800998ecf8427e\"")
	}
	reader, meta, err := fixture.objects.OpenObject(fixture.ctx, "demo", "empty/")
	if err != nil {
		t.Fatalf("OpenObject() error = %v", err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got := string(body); got != "" {
		t.Fatalf("body = %q, want empty", got)
	}
	if meta.Size != 0 {
		t.Fatalf("size = %d, want 0", meta.Size)
	}
}

func TestPutObjectDecodesAWSChunkedPayload(t *testing.T) {
	fixture := newS3StoreFixture(t)
	deps := fixture.deps()

	req := httptest.NewRequest(http.MethodPut, "/demo/file.txt", strings.NewReader("5;chunk-signature=deadbeef\r\nhello\r\n0;chunk-signature=feedface\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n"))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "file.txt")
	rec := httptest.NewRecorder()

	handlePutObject(rec, req, deps, "demo", "file.txt")

	// This verifies the decoder strips the chunk framing and stores only the reconstructed payload bytes.
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("ETag"); got != "\"5d41402abc4b2a76b9719d911017c592\"" {
		t.Fatalf("ETag = %q, want %q", got, "\"5d41402abc4b2a76b9719d911017c592\"")
	}
	reader, _, err := fixture.objects.OpenObject(fixture.ctx, "demo", "file.txt")
	if err != nil {
		t.Fatalf("OpenObject() error = %v", err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if got := string(body); got != "hello" {
		t.Fatalf("body = %q, want %q", got, "hello")
	}
}

func TestDeleteObjectUsesMetadataTruth(t *testing.T) {
	fixture := newS3StoreFixture(t)
	if _, err := fixture.objects.PutObject(fixture.ctx, "demo", "file.txt", bytes.NewBufferString("payload")); err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	meta := &storagetest.FailingMetadataStore{Bucket: "demo", DeleteErr: core.ErrNotFound}
	deps := common.Dependencies{Metadata: meta, Objects: fixture.objects}

	req := httptest.NewRequest(http.MethodDelete, "/demo/file.txt", nil)
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "file.txt")
	rec := httptest.NewRecorder()

	handleDeleteObject(rec, req, deps, "demo", "file.txt")

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", rec.Code)
	}
	// S3 delete is idempotent, so a metadata miss should still delete any lingering bytes.
	if _, _, err := fixture.objects.OpenObject(fixture.ctx, "demo", "file.txt"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expected object to be deleted, got %v", err)
	}
}

func TestDeleteObjectsRemovesExistingAndMissingKeys(t *testing.T) {
	fixture := newS3StoreFixture(t)
	for _, key := range []string{"compat/pyspark/regular/part-0000", "compat/pyspark/regular/_temporary/0/"} {
		meta, err := fixture.objects.PutObject(fixture.ctx, "demo", key, strings.NewReader("payload"))
		if err != nil {
			t.Fatalf("PutObject(%q) error = %v", key, err)
		}
		if err := fixture.metadata.PutObject(fixture.ctx, meta); err != nil {
			t.Fatalf("PutObject(metadata, %q) error = %v", key, err)
		}
	}
	deps := fixture.deps()
	body := `<Delete>
  <Object><Key>compat/pyspark/regular/part-0000</Key></Object>
  <Object><Key>compat/pyspark/regular/_temporary/0/</Key></Object>
  <Object><Key>compat/pyspark/regular/missing</Key></Object>
</Delete>`

	req := httptest.NewRequest(http.MethodPost, "/demo?delete", strings.NewReader(body))
	req.SetPathValue("bucket", "demo")
	rec := httptest.NewRecorder()

	handleDeleteObjects(rec, req, deps, "demo")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<DeleteResult") {
		t.Fatalf("expected DeleteResult XML, got %q", rec.Body.String())
	}
	for _, key := range []string{
		"compat/pyspark/regular/part-0000",
		"compat/pyspark/regular/_temporary/0/",
		"compat/pyspark/regular/missing",
	} {
		if !strings.Contains(rec.Body.String(), "<Key>"+key+"</Key>") {
			t.Fatalf("response missing key %q: %q", key, rec.Body.String())
		}
	}
	for _, key := range []string{"compat/pyspark/regular/part-0000", "compat/pyspark/regular/_temporary/0/"} {
		if _, _, err := fixture.objects.OpenObject(fixture.ctx, "demo", key); !errors.Is(err, core.ErrNotFound) {
			t.Fatalf("OpenObject(%q) error = %v, want not found", key, err)
		}
		if _, err := fixture.metadata.GetObject(fixture.ctx, "demo", key); !errors.Is(err, core.ErrNotFound) {
			t.Fatalf("GetObject(%q) error = %v, want not found", key, err)
		}
	}
}

func TestListObjectsV2GroupsCommonPrefixesWithDelimiter(t *testing.T) {
	fixture := newS3StoreFixture(t)
	for _, item := range []struct {
		key  string
		body string
	}{
		{key: "compat/pyspark/regular/_temporary/0/", body: ""},
		{key: "compat/pyspark/regular/part-0000.parquet", body: "a"},
		{key: "compat/pyspark/regular/part-0001.parquet", body: "b"},
		{key: "compat/pyspark/partitioned/group=a/file.parquet", body: "c"},
	} {
		meta, err := fixture.objects.PutObject(fixture.ctx, "demo", item.key, strings.NewReader(item.body))
		if err != nil {
			t.Fatalf("PutObject(%q) error = %v", item.key, err)
		}
		if err := fixture.metadata.PutObject(fixture.ctx, meta); err != nil {
			t.Fatalf("PutObject(metadata, %q) error = %v", item.key, err)
		}
	}
	deps := fixture.deps()

	req := httptest.NewRequest(http.MethodGet, "/demo?list-type=2&prefix=compat/pyspark/&delimiter=/", nil)
	req.SetPathValue("bucket", "demo")
	rec := httptest.NewRecorder()

	handleListObjectsV2(rec, req, deps, "demo")

	// Delimiter listing should collapse deeper descendants into prefixes instead of returning every leaf object.
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<Delimiter>/</Delimiter>") {
		t.Fatalf("expected delimiter in response, got %q", body)
	}
	for _, prefix := range []string{
		"compat/pyspark/partitioned/",
		"compat/pyspark/regular/",
	} {
		if !strings.Contains(body, "<Prefix>"+prefix+"</Prefix>") {
			t.Fatalf("expected common prefix %q in response %q", prefix, body)
		}
	}
	if strings.Contains(body, "<Key>compat/pyspark/regular/part-0000.parquet</Key>") {
		t.Fatalf("expected regular objects to be grouped into CommonPrefixes, got %q", body)
	}
}

func TestCopyObjectReturnsCopyResultXML(t *testing.T) {
	fixture := newS3StoreFixture(t)
	srcMeta, err := fixture.objects.PutObject(fixture.ctx, "demo", "src.txt", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("PutObject(src) error = %v", err)
	}
	if err := fixture.metadata.PutObject(fixture.ctx, srcMeta); err != nil {
		t.Fatalf("PutObject(metadata, src) error = %v", err)
	}
	deps := fixture.deps()

	req := httptest.NewRequest(http.MethodPut, "/demo/dst.txt", nil)
	req.Header.Set("X-Amz-Copy-Source", "/demo/src.txt")
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "dst.txt")
	rec := httptest.NewRecorder()

	handlePutObject(rec, req, deps, "demo", "dst.txt")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<CopyObjectResult") {
		t.Fatalf("expected CopyObjectResult XML, got %q", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "321c3cf486ed509164edec1e1981fec8") {
		t.Fatalf("expected copied ETag in response, got %q", rec.Body.String())
	}
	reader, _, err := fixture.objects.OpenObject(fixture.ctx, "demo", "dst.txt")
	if err != nil {
		t.Fatalf("OpenObject(dst) error = %v", err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll(dst) error = %v", err)
	}
	if got := string(body); got != "payload" {
		t.Fatalf("dst body = %q, want %q", got, "payload")
	}
}

func TestGetObjectInvalidRangeReturns416(t *testing.T) {
	fixture := newS3StoreFixture(t)
	meta, err := fixture.objects.PutObject(fixture.ctx, "demo", "file.txt", bytes.NewBufferString("hello"))
	if err != nil {
		t.Fatalf("PutObject() error = %v", err)
	}
	if err := fixture.metadata.PutObject(fixture.ctx, meta); err != nil {
		t.Fatalf("PutObject(metadata) error = %v", err)
	}
	deps := fixture.deps()

	req := httptest.NewRequest(http.MethodGet, "/demo/file.txt", nil)
	req.Header.Set("Range", "bytes=100-200")
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "file.txt")
	rec := httptest.NewRecorder()

	handleGetObject(rec, req, deps, "demo", "file.txt")

	if rec.Code != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", rec.Code)
	}
	if got := rec.Header().Get("Content-Range"); got != "bytes */5" {
		t.Fatalf("Content-Range = %q, want %q", got, "bytes */5")
	}
	if !strings.Contains(rec.Body.String(), "<Code>InvalidRange</Code>") {
		t.Fatalf("expected InvalidRange XML error, got %q", rec.Body.String())
	}
}

func TestWriteErrorUsesS3XMLEnvelope(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, core.ErrInvalidArgument)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/xml" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/xml")
	}
	body, err := io.ReadAll(rec.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if !strings.Contains(string(body), "<Error>") || !strings.Contains(string(body), "<Code>InvalidArgument</Code>") {
		t.Fatalf("unexpected XML error body: %q", string(body))
	}
}

func TestGetObjectMissingBucketReturnsNoSuchBucket(t *testing.T) {
	deps := common.Dependencies{
		Metadata: &storagetest.FailingMetadataStore{BucketErr: core.ErrNotFound},
	}
	req := httptest.NewRequest(http.MethodGet, "/missing/file.txt", nil)
	req.SetPathValue("bucket", "missing")
	req.SetPathValue("key", "file.txt")
	rec := httptest.NewRecorder()

	handleGetObject(rec, req, deps, "missing", "file.txt")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>NoSuchBucket</Code>") {
		t.Fatalf("expected NoSuchBucket XML error, got %q", rec.Body.String())
	}
}

func TestGetObjectMissingKeyReturnsNoSuchKey(t *testing.T) {
	deps := common.Dependencies{
		Metadata: &storagetest.FailingMetadataStore{Bucket: "demo"},
	}
	req := httptest.NewRequest(http.MethodGet, "/demo/missing.txt", nil)
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "missing.txt")
	rec := httptest.NewRecorder()

	handleGetObject(rec, req, deps, "demo", "missing.txt")

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>NoSuchKey</Code>") {
		t.Fatalf("expected NoSuchKey XML error, got %q", rec.Body.String())
	}
}

func TestCompleteMultipartRejectsMalformedPayload(t *testing.T) {
	meta := &storagetest.MultipartMetadataStore{
		Bucket:   "demo",
		Key:      "object.txt",
		UploadID: "upload-1",
	}
	deps := common.Dependencies{Metadata: meta}

	req := httptest.NewRequest(
		http.MethodPost,
		"/demo/object.txt?uploadId=upload-1",
		strings.NewReader("<CompleteMultipartUpload"),
	)
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "object.txt")
	rec := httptest.NewRecorder()

	handleCompleteMultipartUpload(rec, req, deps, "demo", "object.txt")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>InvalidArgument</Code>") {
		t.Fatalf("expected InvalidArgument XML error, got %q", rec.Body.String())
	}
}

func TestCompleteMultipartRejectsOutOfOrderParts(t *testing.T) {
	meta := &storagetest.MultipartMetadataStore{
		Bucket:   "demo",
		Key:      "object.txt",
		UploadID: "upload-1",
		Parts: []core.MultipartPart{
			{UploadID: "upload-1", PartNumber: 1, ETag: "etag-1"},
			{UploadID: "upload-1", PartNumber: 2, ETag: "etag-2"},
		},
	}
	deps := common.Dependencies{Metadata: meta}
	body := `<CompleteMultipartUpload>
  <Part><PartNumber>2</PartNumber><ETag>"etag-2"</ETag></Part>
  <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
</CompleteMultipartUpload>`

	req := httptest.NewRequest(http.MethodPost, "/demo/object.txt?uploadId=upload-1", strings.NewReader(body))
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "object.txt")
	rec := httptest.NewRecorder()

	handleCompleteMultipartUpload(rec, req, deps, "demo", "object.txt")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>InvalidArgument</Code>") {
		t.Fatalf("expected InvalidArgument XML error, got %q", rec.Body.String())
	}
}

func TestCompleteMultipartRejectsDuplicatePartNumbers(t *testing.T) {
	meta := &storagetest.MultipartMetadataStore{
		Bucket:   "demo",
		Key:      "object.txt",
		UploadID: "upload-1",
		Parts: []core.MultipartPart{
			{UploadID: "upload-1", PartNumber: 1, ETag: "etag-1"},
		},
	}
	deps := common.Dependencies{Metadata: meta}
	body := `<CompleteMultipartUpload>
  <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
  <Part><PartNumber>1</PartNumber><ETag>"etag-1"</ETag></Part>
</CompleteMultipartUpload>`

	req := httptest.NewRequest(http.MethodPost, "/demo/object.txt?uploadId=upload-1", strings.NewReader(body))
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "object.txt")
	rec := httptest.NewRecorder()

	handleCompleteMultipartUpload(rec, req, deps, "demo", "object.txt")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<Code>InvalidArgument</Code>") {
		t.Fatalf("expected InvalidArgument XML error, got %q", rec.Body.String())
	}
}

func TestCompleteMultipartRollbackOnDeleteFailure(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	objects, err := storage.NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}
	part1, err := objects.PutMultipartPart(ctx, "upload-1", 1, bytes.NewBufferString("hello "))
	if err != nil {
		t.Fatalf("PutMultipartPart() error = %v", err)
	}
	part2, err := objects.PutMultipartPart(ctx, "upload-1", 2, bytes.NewBufferString("world"))
	if err != nil {
		t.Fatalf("PutMultipartPart() error = %v", err)
	}
	meta := &storagetest.MultipartMetadataStore{
		Bucket:               "demo",
		Key:                  "object.txt",
		UploadID:             "upload-1",
		Parts:                []core.MultipartPart{part1, part2},
		DeleteMultipartErr:   errors.New("delete failed"),
		AllowMetadataDeletes: true,
	}
	deps := common.Dependencies{Metadata: meta, Objects: objects}

	payload := struct {
		XMLName xml.Name `xml:"CompleteMultipartUpload"`
		Parts   []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}{}
	payload.Parts = append(payload.Parts, struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}{PartNumber: 1, ETag: `"` + part1.ETag + `"`})
	payload.Parts = append(payload.Parts, struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	}{PartNumber: 2, ETag: `"` + part2.ETag + `"`})
	raw, _ := xml.Marshal(payload)

	req := httptest.NewRequest(http.MethodPost, "/demo/object.txt?uploadId=upload-1", bytes.NewReader(raw))
	req.SetPathValue("bucket", "demo")
	req.SetPathValue("key", "object.txt")
	rec := httptest.NewRecorder()

	handleCompleteMultipartUpload(rec, req, deps, "demo", "object.txt")

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if _, _, err := objects.OpenObject(ctx, "demo", "object.txt"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expected object rollback, got %v", err)
	}
}

type s3StoreFixture struct {
	ctx      context.Context
	metadata *storage.SQLiteStore
	objects  *storage.FilesystemObjectStore
}

func newS3StoreFixture(t *testing.T) s3StoreFixture {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	objects, err := storage.NewFilesystemObjectStore(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatalf("NewFilesystemObjectStore() error = %v", err)
	}
	metadata, err := storage.OpenSQLite(filepath.Join(dir, "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = metadata.Close() })
	if err := metadata.EnsureBucket(ctx, "demo"); err != nil {
		t.Fatalf("EnsureBucket() error = %v", err)
	}
	return s3StoreFixture{ctx: ctx, metadata: metadata, objects: objects}
}

func (f s3StoreFixture) deps() common.Dependencies {
	return common.Dependencies{Metadata: f.metadata, Objects: f.objects}
}
