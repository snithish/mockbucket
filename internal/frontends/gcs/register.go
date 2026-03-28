package gcs

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	authgcp "github.com/snithish/mockbucket/internal/auth/gcp"
	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/httpx"
	"github.com/snithish/mockbucket/internal/seed"
)

const (
	bucketsBasePath   = "/storage/v1/b"
	uploadBasePath    = "/upload/storage/v1/b"
	downloadBasePath  = "/download/storage/v1/b"
	resumableBasePath = "/upload/resumable"
)

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies, gcsServiceAccounts []seed.ServiceAccountJSON) {
	RegisterServiceAccountEndpoint(mux, gcsServiceAccounts)
	var resumableUploads sync.Map

	bucketsHandler := authgcp.Authenticate(deps.AuthResolver, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleListBuckets(w, r, deps)
		case http.MethodPost:
			handleCreateBucket(w, r, deps)
		default:
			http.NotFound(w, r)
		}
	}))
	bucketHandler := authgcp.Authenticate(deps.AuthResolver, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleGetBucket(w, r, deps, r.PathValue("bucket"))
		default:
			http.NotFound(w, r)
		}
	}))
	objectsHandler := authgcp.Authenticate(deps.AuthResolver, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			handleListObjects(w, r, deps, r.PathValue("bucket"))
		default:
			http.NotFound(w, r)
		}
	}))
	objectHandler := authgcp.Authenticate(deps.AuthResolver, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := r.PathValue("bucket")
		key, err := pathObject(r)
		if err != nil {
			writeError(w, core.ErrInvalidArgument)
			return
		}
		switch r.Method {
		case http.MethodGet:
			handleGetObject(w, r, deps, bucket, key, false)
		case http.MethodDelete:
			handleDeleteObject(w, r, deps, bucket, key)
		case http.MethodPost:
			if srcKey, dstBucket, dstKey, ok := rewriteRequest(r, key); ok {
				handleRewriteObject(w, r, deps, bucket, srcKey, dstBucket, dstKey)
				return
			}
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	downloadHandler := authgcp.Authenticate(deps.AuthResolver, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		key, err := pathObject(r)
		if err != nil {
			writeError(w, core.ErrInvalidArgument)
			return
		}
		handleGetObject(w, r, deps, r.PathValue("bucket"), key, true)
	}))
	uploadHandler := authgcp.Authenticate(deps.AuthResolver, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		handleUploadObject(w, r, deps, &resumableUploads, r.PathValue("bucket"))
	}))
	resumableUploadHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.NotFound(w, r)
			return
		}
		handleResumableUpload(w, r, deps, &resumableUploads, r.PathValue("session"))
	})

	mux.Handle(bucketsBasePath, bucketsHandler)
	mux.Handle(bucketsBasePath+"/{bucket}", bucketHandler)
	mux.Handle(bucketsBasePath+"/{bucket}/o", objectsHandler)
	mux.Handle(bucketsBasePath+"/{bucket}/o/{object...}", objectHandler)
	mux.Handle(downloadBasePath+"/{bucket}/o/{object...}", downloadHandler)
	mux.Handle(uploadBasePath+"/{bucket}/o", uploadHandler)
	mux.Handle(resumableBasePath+"/{session}", resumableUploadHandler)
	mux.HandleFunc("/oauth2/v4/token", authgcp.TokenEndpoint(deps.AuthResolver, deps.SessionManager))
	mux.Handle("/{bucket}/{object...}", authgcp.Authenticate(deps.AuthResolver, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		key, err := pathObject(r)
		if err != nil {
			writeError(w, core.ErrInvalidArgument)
			return
		}
		handleGetObject(w, r, deps, r.PathValue("bucket"), key, true)
	})))
}

type bucketResponse struct {
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	Name        string `json:"name"`
	TimeCreated string `json:"timeCreated"`
}

type listBucketsResponse struct {
	Kind  string           `json:"kind"`
	Items []bucketResponse `json:"items,omitempty"`
}

type objectResponse struct {
	Kind           string `json:"kind"`
	ID             string `json:"id"`
	Bucket         string `json:"bucket"`
	Name           string `json:"name"`
	Size           string `json:"size"`
	CRC32C         string `json:"crc32c,omitempty"`
	ETag           string `json:"etag"`
	Generation     string `json:"generation"`
	Metageneration string `json:"metageneration"`
	TimeCreated    string `json:"timeCreated"`
	Updated        string `json:"updated"`
}

type listObjectsResponse struct {
	Kind          string           `json:"kind"`
	Items         []objectResponse `json:"items,omitempty"`
	Prefixes      []string         `json:"prefixes,omitempty"`
	NextPageToken string           `json:"nextPageToken,omitempty"`
}

func handleListBuckets(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	if !requireAuthenticatedSubject(r) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	buckets, err := deps.Metadata.ListBuckets(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	resp := listBucketsResponse{Kind: "storage#buckets"}
	for _, bucket := range buckets {
		resp.Items = append(resp.Items, bucketResponse{
			Kind:        "storage#bucket",
			ID:          bucket.Name,
			Name:        bucket.Name,
			TimeCreated: bucket.CreatedAt.UTC().Format(time.RFC3339),
		})
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleCreateBucket(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	var payload struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	bucket := strings.TrimSpace(payload.Name)
	if bucket == "" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if !requireAuthenticatedSubject(r) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if err := deps.Metadata.CreateBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	resp := bucketResponse{
		Kind:        "storage#bucket",
		ID:          bucket,
		Name:        bucket,
		TimeCreated: time.Now().UTC().Format(time.RFC3339),
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleGetBucket(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if !requireAuthenticatedSubject(r) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	meta, err := deps.Metadata.GetBucket(r.Context(), bucket)
	if err != nil {
		writeError(w, err)
		return
	}
	resp := bucketResponse{
		Kind:        "storage#bucket",
		ID:          meta.Name,
		Name:        meta.Name,
		TimeCreated: meta.CreatedAt.UTC().Format(time.RFC3339),
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleListObjects(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if !requireAuthenticatedSubject(r) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	pageToken := r.URL.Query().Get("pageToken")
	startOffset := r.URL.Query().Get("startOffset")
	maxResults := parseMaxResults(r.URL.Query().Get("maxResults"))
	after := startOffset
	if pageToken != "" {
		var err error
		after, err = decodePageToken(pageToken)
		if err != nil {
			writeError(w, core.ErrInvalidArgument)
			return
		}
	}
	fetchLimit := maxResults + 1
	objects, err := deps.Metadata.ListObjects(r.Context(), bucket, prefix, fetchLimit, after)
	if err != nil {
		writeError(w, err)
		return
	}
	if delimiter != "" {
		objects, prefixes := applyDelimiter(objects, prefix, delimiter)
		resp := listObjectsResponse{Kind: "storage#objects", Prefixes: prefixes}
		for _, obj := range objects {
			resp.Items = append(resp.Items, newObjectResponse(obj, ""))
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}
	isTruncated := false
	if len(objects) > maxResults {
		isTruncated = true
		objects = objects[:maxResults]
	}
	resp := listObjectsResponse{Kind: "storage#objects"}
	for _, obj := range objects {
		resp.Items = append(resp.Items, newObjectResponse(obj, ""))
	}
	if isTruncated && len(objects) > 0 {
		resp.NextPageToken = encodePageToken(objects[len(objects)-1].Key)
	}
	writeJSON(w, http.StatusOK, resp)
}

type resumableUpload struct {
	Bucket string
	Key    string
	Path   string

	mu       sync.Mutex
	received int64
}

func handleUploadObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, resumableUploads *sync.Map, bucket string) {
	uploadType := r.URL.Query().Get("uploadType")
	switch uploadType {
	case "media":
		handleMediaUpload(w, r, deps, bucket)
	case "multipart":
		handleMultipartUpload(w, r, deps, bucket)
	case "resumable":
		handleResumableUploadInit(w, r, deps, resumableUploads, bucket)
	default:
		writeError(w, core.ErrInvalidArgument)
	}
}

func handleResumableUploadInit(w http.ResponseWriter, r *http.Request, deps common.Dependencies, resumableUploads *sync.Map, bucket string) {
	key := strings.TrimSpace(r.URL.Query().Get("name"))
	if key == "" && r.Body != nil {
		var metadata struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(r.Body).Decode(&metadata); err == nil {
			key = strings.TrimSpace(metadata.Name)
		}
	}
	if key == "" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if !requireAuthenticatedSubject(r) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	sessionID, err := newResumableUploadID()
	if err != nil {
		writeError(w, err)
		return
	}
	tempFile, err := os.CreateTemp("", "mockbucket-gcs-resumable-*")
	if err != nil {
		writeError(w, err)
		return
	}
	tempPath := tempFile.Name()
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		writeError(w, err)
		return
	}
	resumableUploads.Store(sessionID, &resumableUpload{Bucket: bucket, Key: key, Path: tempPath})
	w.Header().Set("Location", resumableUploadLocation(r, sessionID))
	w.WriteHeader(http.StatusOK)
}

func handleResumableUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, resumableUploads *sync.Map, sessionID string) {
	value, ok := resumableUploads.Load(sessionID)
	if !ok {
		writeError(w, core.ErrNotFound)
		return
	}
	upload := value.(*resumableUpload)
	if isZeroByteResumableFinalize(r) {
		upload.mu.Lock()
		defer upload.mu.Unlock()
		if err := finalizeResumableUpload(r.Context(), deps, resumableUploads, sessionID, upload, w); err != nil {
			writeError(w, err)
		}
		return
	}
	if isResumableStatusQuery(r) {
		upload.mu.Lock()
		defer upload.mu.Unlock()
		writeResumableIncomplete(w, upload.received)
		return
	}
	upload.mu.Lock()
	defer upload.mu.Unlock()

	rangeSpec, ok, err := parseResumableContentRange(r.Header.Get("Content-Range"))
	if err != nil {
		writeError(w, err)
		return
	}
	if !ok {
		if _, err := appendResumableChunk(upload.Path, 0, r.Body); err != nil {
			writeError(w, err)
			return
		}
		if err := finalizeResumableUpload(r.Context(), deps, resumableUploads, sessionID, upload, w); err != nil {
			writeError(w, err)
		}
		return
	}
	if rangeSpec.Start != upload.received {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	written, err := appendResumableChunk(upload.Path, rangeSpec.Start, r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	if written != rangeSpec.End-rangeSpec.Start+1 {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	upload.received += written
	if rangeSpec.Total >= 0 && upload.received > rangeSpec.Total {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if rangeSpec.Total < 0 || upload.received < rangeSpec.Total {
		writeResumableIncomplete(w, upload.received)
		return
	}
	if err := finalizeResumableUpload(r.Context(), deps, resumableUploads, sessionID, upload, w); err != nil {
		writeError(w, err)
	}
}

func handleMediaUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	key := strings.TrimSpace(r.URL.Query().Get("name"))
	if key == "" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if !requireAuthenticatedSubject(r) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	key = normalizeDirectoryMarkerKey(key, r.ContentLength)
	meta, crc32c, err := putObjectWithCRC32C(r.Context(), deps, bucket, key, r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := deps.Metadata.PutObject(r.Context(), meta); err != nil {
		_ = deps.Objects.DeleteObject(r.Context(), bucket, key)
		writeError(w, err)
		return
	}
	resp := newObjectResponse(meta, crc32c)
	writeJSON(w, http.StatusOK, resp)
}

func handleMultipartUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	contentType := r.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil || !strings.HasPrefix(mediaType, "multipart/") {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	boundary := params["boundary"]
	if boundary == "" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	reader := multipart.NewReader(r.Body, boundary)
	var (
		metadata struct {
			Name string `json:"name"`
		}
		media io.Reader
	)
	for i := 0; i < 2; i++ {
		part, err := reader.NextPart()
		if err != nil {
			writeError(w, core.ErrInvalidArgument)
			return
		}
		defer func() { _ = part.Close() }()
		partType := part.Header.Get("Content-Type")
		if i == 0 {
			if err := json.NewDecoder(part).Decode(&metadata); err != nil {
				writeError(w, core.ErrInvalidArgument)
				return
			}
		} else {
			media = part
			if partType == "" {
				partType = "application/octet-stream"
			}
		}
	}
	key := strings.TrimSpace(r.URL.Query().Get("name"))
	if key == "" {
		key = strings.TrimSpace(metadata.Name)
	}
	if key == "" || media == nil {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if !requireAuthenticatedSubject(r) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	meta, crc32c, err := putObjectWithCRC32C(r.Context(), deps, bucket, key, media)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := deps.Metadata.PutObject(r.Context(), meta); err != nil {
		_ = deps.Objects.DeleteObject(r.Context(), bucket, key)
		writeError(w, err)
		return
	}
	resp := newObjectResponse(meta, crc32c)
	writeJSON(w, http.StatusOK, resp)
}

func handleGetObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string, forceMedia bool) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return
	}
	if !requireAuthenticatedSubject(r) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	meta, err := deps.Metadata.GetObject(r.Context(), bucket, key)
	if err != nil {
		writeError(w, err)
		return
	}
	if !forceMedia && r.URL.Query().Get("alt") != "media" {
		resp := newObjectResponse(meta, "")
		writeJSON(w, http.StatusOK, resp)
		return
	}
	reader, _, err := deps.Objects.OpenObject(r.Context(), bucket, key)
	if err != nil {
		writeError(w, err)
		return
	}
	defer func() { _ = reader.Close() }()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
}

func handleDeleteObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if !requireAuthenticatedSubject(r) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	if err := deleteObjectTree(r.Context(), deps, bucket, key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleRewriteObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, srcBucket, srcKey, dstBucket, dstKey string) {
	if !requireAuthenticatedSubject(r) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), srcBucket); err != nil {
		writeError(w, err)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), dstBucket); err != nil {
		writeError(w, err)
		return
	}
	reader, _, err := deps.Objects.OpenObject(r.Context(), srcBucket, srcKey)
	if err != nil {
		writeError(w, err)
		return
	}
	defer func() { _ = reader.Close() }()

	meta, crc32c, err := putObjectWithCRC32C(r.Context(), deps, dstBucket, dstKey, reader)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := deps.Metadata.PutObject(r.Context(), meta); err != nil {
		_ = deps.Objects.DeleteObject(r.Context(), dstBucket, dstKey)
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"kind":                "storage#rewriteResponse",
		"totalBytesRewritten": strconv.FormatInt(meta.Size, 10),
		"objectSize":          strconv.FormatInt(meta.Size, 10),
		"done":                true,
		"resource":            newObjectResponse(meta, crc32c),
	})
}

func parseMaxResults(raw string) int {
	if raw == "" {
		return 1000
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 1000
	}
	if value > 1000 {
		return 1000
	}
	return value
}

func encodePageToken(lastKey string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(lastKey))
}

func decodePageToken(token string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func requireAuthenticatedSubject(r *http.Request) bool {
	return hasAuthenticatedSubject(r)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, err error) {
	status := httpx.StatusCode(err)
	reason := "backendError"
	switch status {
	case http.StatusBadRequest:
		reason = "invalid"
	case http.StatusUnauthorized:
		reason = "required"
	case http.StatusForbidden:
		reason = "forbidden"
	case http.StatusNotFound:
		reason = "notFound"
	case http.StatusConflict:
		reason = "conflict"
	}
	writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code":    status,
			"message": err.Error(),
			"errors": []map[string]any{
				{
					"message": err.Error(),
					"domain":  "global",
					"reason":  reason,
				},
			},
		},
	})
}

func rewriteRequest(r *http.Request, rawObject string) (string, string, string, bool) {
	if r.Method != http.MethodPost {
		return "", "", "", false
	}
	const marker = "/rewriteTo/b/"
	srcKey, remainder, ok := strings.Cut(rawObject, marker)
	if !ok {
		return "", "", "", false
	}
	dstBucket, dstPath, ok := strings.Cut(remainder, "/o/")
	if !ok || strings.TrimSpace(srcKey) == "" || strings.TrimSpace(dstBucket) == "" || strings.TrimSpace(dstPath) == "" {
		return "", "", "", false
	}
	dstKey, err := url.PathUnescape(dstPath)
	if err != nil {
		return "", "", "", false
	}
	return srcKey, dstBucket, dstKey, true
}

func putObjectWithCRC32C(ctx context.Context, deps common.Dependencies, bucket, key string, src io.Reader) (core.ObjectMetadata, string, error) {
	hasher := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	tee := io.TeeReader(src, hasher)
	meta, err := deps.Objects.PutObject(ctx, bucket, key, tee)
	if err != nil {
		return core.ObjectMetadata{}, "", err
	}
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], hasher.Sum32())
	return meta, base64.StdEncoding.EncodeToString(buf[:]), nil
}

type resumableContentRange struct {
	Start int64
	End   int64
	Total int64
}

func parseResumableContentRange(raw string) (resumableContentRange, bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return resumableContentRange{}, false, nil
	}
	if strings.HasPrefix(raw, "bytes */") {
		return resumableContentRange{}, false, nil
	}
	if !strings.HasPrefix(raw, "bytes ") {
		return resumableContentRange{}, false, core.ErrInvalidArgument
	}
	rangePart, totalPart, ok := strings.Cut(strings.TrimPrefix(raw, "bytes "), "/")
	if !ok {
		return resumableContentRange{}, false, core.ErrInvalidArgument
	}
	startPart, endPart, ok := strings.Cut(rangePart, "-")
	if !ok {
		return resumableContentRange{}, false, core.ErrInvalidArgument
	}
	start, err := strconv.ParseInt(startPart, 10, 64)
	if err != nil || start < 0 {
		return resumableContentRange{}, false, core.ErrInvalidArgument
	}
	end, err := strconv.ParseInt(endPart, 10, 64)
	if err != nil || end < start {
		return resumableContentRange{}, false, core.ErrInvalidArgument
	}
	total := int64(-1)
	if totalPart != "*" {
		total, err = strconv.ParseInt(totalPart, 10, 64)
		if err != nil || total <= end {
			return resumableContentRange{}, false, core.ErrInvalidArgument
		}
	}
	return resumableContentRange{Start: start, End: end, Total: total}, true, nil
}

func appendResumableChunk(path string, offset int64, body io.Reader) (int64, error) {
	file, err := os.OpenFile(path, os.O_WRONLY, 0o600)
	if err != nil {
		return 0, err
	}
	defer func() { _ = file.Close() }()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	written, err := io.Copy(file, body)
	if err != nil {
		return written, err
	}
	return written, nil
}

func finalizeResumableUpload(
	ctx context.Context,
	deps common.Dependencies,
	resumableUploads *sync.Map,
	sessionID string,
	upload *resumableUpload,
	w http.ResponseWriter,
) error {
	file, err := os.Open(upload.Path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	key := normalizeDirectoryMarkerKey(upload.Key, upload.received)
	meta, crc32c, err := putObjectWithCRC32C(ctx, deps, upload.Bucket, key, file)
	if err != nil {
		return err
	}
	if err := deps.Metadata.PutObject(ctx, meta); err != nil {
		_ = deps.Objects.DeleteObject(ctx, upload.Bucket, key)
		return err
	}
	resumableUploads.Delete(sessionID)
	_ = os.Remove(upload.Path)
	resp := newObjectResponse(meta, crc32c)
	writeJSON(w, http.StatusOK, resp)
	return nil
}

func newObjectResponse(meta core.ObjectMetadata, crc32c string) objectResponse {
	generation := objectGeneration(meta)
	return objectResponse{
		Kind:           "storage#object",
		ID:             meta.Bucket + "/" + meta.Key + "/" + generation,
		Bucket:         meta.Bucket,
		Name:           meta.Key,
		Size:           strconv.FormatInt(meta.Size, 10),
		CRC32C:         crc32c,
		ETag:           strings.Trim(meta.ETag, "\""),
		Generation:     generation,
		Metageneration: "1",
		TimeCreated:    meta.CreatedAt.UTC().Format(time.RFC3339),
		Updated:        meta.ModifiedAt.UTC().Format(time.RFC3339),
	}
}

func objectGeneration(meta core.ObjectMetadata) string {
	generation := meta.ModifiedAt.UTC().UnixMicro()
	if generation <= 0 {
		generation = meta.CreatedAt.UTC().UnixMicro()
	}
	if generation <= 0 {
		generation = 1
	}
	return strconv.FormatInt(generation, 10)
}

func writeResumableIncomplete(w http.ResponseWriter, received int64) {
	if received > 0 {
		w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", received-1))
	}
	w.WriteHeader(http.StatusPermanentRedirect)
}

func isResumableStatusQuery(r *http.Request) bool {
	contentRange := strings.TrimSpace(r.Header.Get("Content-Range"))
	if contentRange == "bytes */0" {
		return false
	}
	if !strings.HasPrefix(contentRange, "bytes */") {
		return false
	}
	if r.ContentLength > 0 {
		return false
	}
	return true
}

func isZeroByteResumableFinalize(r *http.Request) bool {
	return strings.TrimSpace(r.Header.Get("Content-Range")) == "bytes */0" && r.ContentLength == 0
}

func deleteObjectTree(ctx context.Context, deps common.Dependencies, bucket, key string) error {
	if err := deps.Metadata.DeleteObject(ctx, bucket, key); err != nil && err != core.ErrNotFound {
		return err
	}
	if err := deps.Objects.DeleteObject(ctx, bucket, key); err != nil && err != core.ErrNotFound && !isDirectoryNotEmptyError(err) {
		return err
	}
	if !strings.HasSuffix(key, "/") {
		markerKey := key + "/"
		if err := deps.Metadata.DeleteObject(ctx, bucket, markerKey); err != nil && err != core.ErrNotFound {
			return err
		}
		if err := deps.Objects.DeleteObject(ctx, bucket, markerKey); err != nil && err != core.ErrNotFound {
			return err
		}
	}

	prefix := key
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	for {
		objects, err := deps.Metadata.ListObjects(ctx, bucket, prefix, 1000, "")
		if err != nil {
			return err
		}
		if len(objects) == 0 {
			return nil
		}
		for _, obj := range objects {
			if err := deps.Metadata.DeleteObject(ctx, bucket, obj.Key); err != nil && err != core.ErrNotFound {
				return err
			}
			if err := deps.Objects.DeleteObject(ctx, bucket, obj.Key); err != nil && err != core.ErrNotFound {
				return err
			}
		}
	}
}

func normalizeDirectoryMarkerKey(key string, contentLength int64) string {
	return key
}

func isDirectoryNotEmptyError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "directory not empty")
}

func applyDelimiter(objects []core.ObjectMetadata, prefix, delimiter string) ([]core.ObjectMetadata, []string) {
	if delimiter == "" {
		return objects, nil
	}
	items := make([]core.ObjectMetadata, 0, len(objects))
	prefixes := make([]string, 0)
	seenPrefixes := make(map[string]struct{})
	for _, obj := range objects {
		trimmed := strings.TrimPrefix(obj.Key, prefix)
		if trimmed == obj.Key {
			items = append(items, obj)
			continue
		}
		if idx := strings.Index(trimmed, delimiter); idx >= 0 {
			commonPrefix := prefix + trimmed[:idx+len(delimiter)]
			if _, ok := seenPrefixes[commonPrefix]; !ok {
				seenPrefixes[commonPrefix] = struct{}{}
				prefixes = append(prefixes, commonPrefix)
			}
			continue
		}
		items = append(items, obj)
	}
	return items, prefixes
}

func pathObject(r *http.Request) (string, error) {
	raw := r.PathValue("object")
	if raw == "" {
		return "", nil
	}
	return url.PathUnescape(raw)
}

func resumableUploadLocation(r *http.Request, sessionID string) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return scheme + "://" + r.Host + resumableBasePath + "/" + sessionID
}

func newResumableUploadID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
