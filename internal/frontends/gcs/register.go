package gcs

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	authgcp "github.com/snithish/mockbucket/internal/auth/gcp"
	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
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

func handleListBuckets(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	if !requireAuthenticatedRequest(w, r) {
		return
	}
	buckets, err := deps.Metadata.ListBuckets(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	resp := listBucketsResponse{Kind: "storage#buckets"}
	for _, bucket := range buckets {
		resp.Items = append(resp.Items, newBucketResponse(bucket.Name, bucket.CreatedAt))
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
	if !requireAuthenticatedRequest(w, r) {
		return
	}
	if err := deps.Metadata.CreateBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newBucketResponse(bucket, time.Now().UTC()))
}

func handleGetBucket(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if !requireAuthenticatedRequest(w, r) {
		return
	}
	meta, ok := loadBucket(w, r, deps, bucket)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, newBucketResponse(meta.Name, meta.CreatedAt))
}

func handleListObjects(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if !requireAuthenticatedRequest(w, r) {
		return
	}
	if _, ok := loadBucket(w, r, deps, bucket); !ok {
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
	Bucket             string
	Key                string
	Path               string
	ContentType        string
	CacheControl       string
	ContentDisposition string
	ContentEncoding    string
	ContentLanguage    string
	CustomMetadata     map[string]string
	Preconditions      objectPreconditions

	mu       sync.Mutex
	received int64
}

type objectMetadataPayload struct {
	Name               string            `json:"name"`
	ContentType        string            `json:"contentType"`
	CacheControl       string            `json:"cacheControl"`
	ContentDisposition string            `json:"contentDisposition"`
	ContentEncoding    string            `json:"contentEncoding"`
	ContentLanguage    string            `json:"contentLanguage"`
	Metadata           map[string]string `json:"metadata"`
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
	var payload objectMetadataPayload
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil && err != io.EOF {
			writeError(w, core.ErrInvalidArgument)
			return
		}
	}
	key := strings.TrimSpace(r.URL.Query().Get("name"))
	if key == "" {
		key = strings.TrimSpace(payload.Name)
	}
	if key == "" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if !requireAuthenticatedRequest(w, r) {
		return
	}
	if _, ok := loadBucket(w, r, deps, bucket); !ok {
		return
	}
	preconditions, err := parseObjectPreconditions(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if !validateWriteObjectPreconditions(w, r, deps, bucket, key, preconditions) {
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
	resumableUploads.Store(sessionID, &resumableUpload{
		Bucket:             bucket,
		Key:                key,
		Path:               tempPath,
		ContentType:        firstNonEmpty(strings.TrimSpace(payload.ContentType), strings.TrimSpace(r.Header.Get("X-Upload-Content-Type"))),
		CacheControl:       strings.TrimSpace(payload.CacheControl),
		ContentDisposition: strings.TrimSpace(payload.ContentDisposition),
		ContentEncoding:    strings.TrimSpace(payload.ContentEncoding),
		ContentLanguage:    strings.TrimSpace(payload.ContentLanguage),
		CustomMetadata:     cloneCustomMetadata(payload.Metadata),
		Preconditions:      preconditions,
	})
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
			if err == errConditionNotMet {
				writePreconditionFailed(w)
				return
			}
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
		if err := finalizeUnboundedResumableUpload(r.Context(), deps, resumableUploads, sessionID, upload, r.Body, w); err != nil {
			if err == errConditionNotMet {
				writePreconditionFailed(w)
				return
			}
			writeError(w, err)
		}
		return
	}
	complete, err := appendResumableRange(upload, rangeSpec, r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	if !complete {
		writeResumableIncomplete(w, upload.received)
		return
	}
	if err := finalizeResumableUpload(r.Context(), deps, resumableUploads, sessionID, upload, w); err != nil {
		if err == errConditionNotMet {
			writePreconditionFailed(w)
			return
		}
		writeError(w, err)
	}
}

func handleMediaUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	key := strings.TrimSpace(r.URL.Query().Get("name"))
	if key == "" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if !requireAuthenticatedRequest(w, r) {
		return
	}
	if _, ok := loadBucket(w, r, deps, bucket); !ok {
		return
	}
	preconditions, err := parseObjectPreconditions(r)
	if err != nil {
		writeError(w, err)
		return
	}
	key = normalizeDirectoryMarkerKey(key, r.ContentLength)
	if !validateWriteObjectPreconditions(w, r, deps, bucket, key, preconditions) {
		return
	}
	meta, crc32c, err := putObjectWithCRC32C(r.Context(), deps, bucket, key, r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	applyObjectMetadata(&meta, objectMetadataPayload{
		ContentType: strings.TrimSpace(r.Header.Get("Content-Type")),
	})
	if err := common.CommitObject(r.Context(), deps, meta); err != nil {
		writeError(w, err)
		return
	}
	meta, err = deps.Metadata.GetObject(r.Context(), bucket, key)
	if err != nil {
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
		metadata objectMetadataPayload
		media    io.Reader
	)
	for i := 0; i < 2; i++ {
		part, err := reader.NextPart()
		if err != nil {
			writeError(w, core.ErrInvalidArgument)
			return
		}
		defer part.Close()
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
	if !requireAuthenticatedRequest(w, r) {
		return
	}
	if _, ok := loadBucket(w, r, deps, bucket); !ok {
		return
	}
	preconditions, err := parseObjectPreconditions(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if !validateWriteObjectPreconditions(w, r, deps, bucket, key, preconditions) {
		return
	}
	meta, crc32c, err := putObjectWithCRC32C(r.Context(), deps, bucket, key, media)
	if err != nil {
		writeError(w, err)
		return
	}
	if metadata.ContentType == "" {
		metadata.ContentType = strings.TrimSpace(r.Header.Get("Content-Type"))
	}
	applyObjectMetadata(&meta, metadata)
	if err := common.CommitObject(r.Context(), deps, meta); err != nil {
		writeError(w, err)
		return
	}
	meta, err = deps.Metadata.GetObject(r.Context(), bucket, key)
	if err != nil {
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
	if !requireAuthenticatedRequest(w, r) {
		return
	}
	if _, ok := loadBucket(w, r, deps, bucket); !ok {
		return
	}
	meta, ok := loadObject(w, r, deps, bucket, key)
	if !ok {
		return
	}
	preconditions, err := parseObjectPreconditions(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := checkExistingObjectPreconditions(meta, preconditions); err != nil {
		writePreconditionFailed(w)
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
	defer reader.Close()
	applyObjectHeaders(w, meta, "application/octet-stream")
	w.Header().Set("ETag", meta.ETag)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, reader)
}

func handleDeleteObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if !requireAuthenticatedRequest(w, r) {
		return
	}
	if _, ok := loadBucket(w, r, deps, bucket); !ok {
		return
	}
	preconditions, err := parseObjectPreconditions(r)
	if err != nil {
		writeError(w, err)
		return
	}
	meta, err := deps.Metadata.GetObject(r.Context(), bucket, key)
	if err != nil {
		if err == core.ErrNotFound && (preconditions.GenerationMatch != nil || preconditions.MetagenerationMatch != nil) {
			writePreconditionFailed(w)
			return
		}
		if err != core.ErrNotFound {
			writeError(w, err)
			return
		}
	} else if err := checkExistingObjectPreconditions(meta, preconditions); err != nil {
		writePreconditionFailed(w)
		return
	}
	if err := deleteObjectTree(r.Context(), deps, bucket, key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleRewriteObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, srcBucket, srcKey, dstBucket, dstKey string) {
	if !requireAuthenticatedRequest(w, r) {
		return
	}
	if _, ok := loadBucket(w, r, deps, srcBucket); !ok {
		return
	}
	if _, ok := loadBucket(w, r, deps, dstBucket); !ok {
		return
	}
	preconditions, err := parseObjectPreconditions(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if !validateWriteObjectPreconditions(w, r, deps, dstBucket, dstKey, preconditions) {
		return
	}
	srcMeta, ok := loadObject(w, r, deps, srcBucket, srcKey)
	if !ok {
		return
	}
	reader, _, err := deps.Objects.OpenObject(r.Context(), srcBucket, srcKey)
	if err != nil {
		writeError(w, err)
		return
	}
	defer reader.Close()

	meta, crc32c, err := putObjectWithCRC32C(r.Context(), deps, dstBucket, dstKey, reader)
	if err != nil {
		writeError(w, err)
		return
	}
	applyObjectMetadata(&meta, objectMetadataPayload{
		ContentType:        srcMeta.ContentType,
		CacheControl:       srcMeta.CacheControl,
		ContentDisposition: srcMeta.ContentDisposition,
		ContentEncoding:    srcMeta.ContentEncoding,
		ContentLanguage:    srcMeta.ContentLanguage,
		Metadata:           cloneCustomMetadata(srcMeta.CustomMetadata),
	})
	if err := common.CommitObject(r.Context(), deps, meta); err != nil {
		writeError(w, err)
		return
	}
	meta, err = deps.Metadata.GetObject(r.Context(), dstBucket, dstKey)
	if err != nil {
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
	defer file.Close()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return 0, err
	}
	written, err := io.Copy(file, body)
	if err != nil {
		return written, err
	}
	return written, nil
}

func appendResumableRange(upload *resumableUpload, rangeSpec resumableContentRange, body io.Reader) (bool, error) {
	if rangeSpec.Start != upload.received {
		return false, core.ErrInvalidArgument
	}
	written, err := appendResumableChunk(upload.Path, rangeSpec.Start, body)
	if err != nil {
		return false, err
	}
	if written != rangeSpec.End-rangeSpec.Start+1 {
		return false, core.ErrInvalidArgument
	}
	upload.received += written
	if rangeSpec.Total >= 0 && upload.received > rangeSpec.Total {
		return false, core.ErrInvalidArgument
	}
	return rangeSpec.Total >= 0 && upload.received == rangeSpec.Total, nil
}

func finalizeUnboundedResumableUpload(
	ctx context.Context,
	deps common.Dependencies,
	resumableUploads *sync.Map,
	sessionID string,
	upload *resumableUpload,
	body io.Reader,
	w http.ResponseWriter,
) error {
	if _, err := appendResumableChunk(upload.Path, 0, body); err != nil {
		return err
	}
	return finalizeResumableUpload(ctx, deps, resumableUploads, sessionID, upload, w)
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
	defer file.Close()

	key := normalizeDirectoryMarkerKey(upload.Key, upload.received)
	if err := validateWriteObjectPreconditionsNoRequest(ctx, deps, upload.Bucket, key, upload.Preconditions); err != nil {
		if err == errConditionNotMet {
			return errConditionNotMet
		}
		return err
	}
	meta, crc32c, err := putObjectWithCRC32C(ctx, deps, upload.Bucket, key, file)
	if err != nil {
		return err
	}
	applyObjectMetadata(&meta, objectMetadataPayload{
		ContentType:        upload.ContentType,
		CacheControl:       upload.CacheControl,
		ContentDisposition: upload.ContentDisposition,
		ContentEncoding:    upload.ContentEncoding,
		ContentLanguage:    upload.ContentLanguage,
		Metadata:           cloneCustomMetadata(upload.CustomMetadata),
	})
	if err := common.CommitObject(ctx, deps, meta); err != nil {
		return err
	}
	meta, err = deps.Metadata.GetObject(ctx, upload.Bucket, key)
	if err != nil {
		return err
	}
	resumableUploads.Delete(sessionID)
	_ = os.Remove(upload.Path)
	resp := newObjectResponse(meta, crc32c)
	writeJSON(w, http.StatusOK, resp)
	return nil
}

func objectGeneration(meta core.ObjectMetadata) string {
	generation := meta.Generation
	if generation <= 0 {
		generation = 1
	}
	return strconv.FormatInt(generation, 10)
}

func deleteObjectTree(ctx context.Context, deps common.Dependencies, bucket, key string) error {
	if err := common.DeleteObjectIfExists(ctx, deps, bucket, key); err != nil && !isDirectoryNotEmptyError(err) {
		return err
	}
	if !strings.HasSuffix(key, "/") {
		markerKey := key + "/"
		if err := common.DeleteObjectIfExists(ctx, deps, bucket, markerKey); err != nil {
			return err
		}
	}

	prefix := key
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	after := ""
	for {
		objects, err := deps.Metadata.ListObjects(ctx, bucket, prefix, 1000, after)
		if err != nil {
			return err
		}
		if len(objects) == 0 {
			return nil
		}
		after = objects[len(objects)-1].Key
		for _, obj := range objects {
			if err := common.DeleteObjectIfExists(ctx, deps, bucket, obj.Key); err != nil {
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

func applyObjectMetadata(meta *core.ObjectMetadata, payload objectMetadataPayload) {
	meta.ContentType = strings.TrimSpace(payload.ContentType)
	meta.CacheControl = strings.TrimSpace(payload.CacheControl)
	meta.ContentDisposition = strings.TrimSpace(payload.ContentDisposition)
	meta.ContentEncoding = strings.TrimSpace(payload.ContentEncoding)
	meta.ContentLanguage = strings.TrimSpace(payload.ContentLanguage)
	meta.CustomMetadata = cloneCustomMetadata(payload.Metadata)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func validateWriteObjectPreconditions(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string, preconditions objectPreconditions) bool {
	if err := validateWriteObjectPreconditionsNoRequest(r.Context(), deps, bucket, key, preconditions); err != nil {
		if err == errConditionNotMet {
			writePreconditionFailed(w)
			return false
		}
		writeError(w, err)
		return false
	}
	return true
}

func validateWriteObjectPreconditionsNoRequest(ctx context.Context, deps common.Dependencies, bucket, key string, preconditions objectPreconditions) error {
	current, err := deps.Metadata.GetObject(ctx, bucket, key)
	if err != nil {
		if err == core.ErrNotFound {
			return checkWriteObjectPreconditions(nil, preconditions)
		}
		return err
	}
	return checkWriteObjectPreconditions(&current, preconditions)
}
