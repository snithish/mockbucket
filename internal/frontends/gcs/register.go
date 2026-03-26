package gcs

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	authgcp "github.com/snithish/mockbucket/internal/auth/gcp"
	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/httpx"
	"github.com/snithish/mockbucket/internal/seed"
)

const (
	bucketsBasePath  = "/storage/v1/b"
	uploadBasePath   = "/upload/storage/v1/b"
	downloadBasePath = "/download/storage/v1/b"
)

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies, gcsServiceAccounts []seed.ServiceAccountJSON) {
	RegisterServiceAccountEndpoint(mux, gcsServiceAccounts)

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
		handleUploadObject(w, r, deps, r.PathValue("bucket"))
	}))

	mux.Handle(bucketsBasePath, bucketsHandler)
	mux.Handle(bucketsBasePath+"/{bucket}", bucketHandler)
	mux.Handle(bucketsBasePath+"/{bucket}/o", objectsHandler)
	mux.Handle(bucketsBasePath+"/{bucket}/o/{object...}", objectHandler)
	mux.Handle(downloadBasePath+"/{bucket}/o/{object...}", downloadHandler)
	mux.Handle(uploadBasePath+"/{bucket}/o", uploadHandler)
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
	Kind        string `json:"kind"`
	Bucket      string `json:"bucket"`
	Name        string `json:"name"`
	Size        string `json:"size"`
	CRC32C      string `json:"crc32c,omitempty"`
	ETag        string `json:"etag"`
	TimeCreated string `json:"timeCreated"`
	Updated     string `json:"updated"`
}

type listObjectsResponse struct {
	Kind          string           `json:"kind"`
	Items         []objectResponse `json:"items,omitempty"`
	NextPageToken string           `json:"nextPageToken,omitempty"`
}

func handleListBuckets(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	if !allowAuthenticatedSubject(r, "storage.buckets.list", gcsProjectResource()) {
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
	resource := gcsProjectResource()
	if !allowAuthenticatedSubject(r, "storage.buckets.create", resource) {
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
	resource := gcsBucketResource(bucket)
	if !allowAuthenticatedSubject(r, "storage.buckets.get", resource) {
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
	resource := gcsBucketResource(bucket)
	if !allowAuthenticatedSubject(r, "storage.objects.list", resource) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	prefix := r.URL.Query().Get("prefix")
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
	isTruncated := false
	if len(objects) > maxResults {
		isTruncated = true
		objects = objects[:maxResults]
	}
	resp := listObjectsResponse{Kind: "storage#objects"}
	for _, obj := range objects {
		resp.Items = append(resp.Items, objectResponse{
			Kind:        "storage#object",
			Bucket:      obj.Bucket,
			Name:        obj.Key,
			Size:        strconv.FormatInt(obj.Size, 10),
			ETag:        strings.Trim(obj.ETag, "\""),
			TimeCreated: obj.CreatedAt.UTC().Format(time.RFC3339),
			Updated:     obj.ModifiedAt.UTC().Format(time.RFC3339),
		})
	}
	if isTruncated && len(objects) > 0 {
		resp.NextPageToken = encodePageToken(objects[len(objects)-1].Key)
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleUploadObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	uploadType := r.URL.Query().Get("uploadType")
	switch uploadType {
	case "media":
		handleMediaUpload(w, r, deps, bucket)
	case "multipart":
		handleMultipartUpload(w, r, deps, bucket)
	default:
		writeError(w, core.ErrInvalidArgument)
	}
}

func handleMediaUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	key := strings.TrimSpace(r.URL.Query().Get("name"))
	if key == "" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	resource := gcsObjectResource(bucket, key)
	if !allowAuthenticatedSubject(r, "storage.objects.create", resource) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
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
	resp := objectResponse{
		Kind:        "storage#object",
		Bucket:      meta.Bucket,
		Name:        meta.Key,
		Size:        strconv.FormatInt(meta.Size, 10),
		CRC32C:      crc32c,
		ETag:        strings.Trim(meta.ETag, "\""),
		TimeCreated: meta.CreatedAt.UTC().Format(time.RFC3339),
		Updated:     meta.ModifiedAt.UTC().Format(time.RFC3339),
	}
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
	resource := gcsObjectResource(bucket, key)
	if !allowAuthenticatedSubject(r, "storage.objects.create", resource) {
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
	resp := objectResponse{
		Kind:        "storage#object",
		Bucket:      meta.Bucket,
		Name:        meta.Key,
		Size:        strconv.FormatInt(meta.Size, 10),
		CRC32C:      crc32c,
		ETag:        strings.Trim(meta.ETag, "\""),
		TimeCreated: meta.CreatedAt.UTC().Format(time.RFC3339),
		Updated:     meta.ModifiedAt.UTC().Format(time.RFC3339),
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleGetObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string, forceMedia bool) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return
	}
	resource := gcsObjectResource(bucket, key)
	if !allowAuthenticatedSubject(r, "storage.objects.get", resource) {
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
		resp := objectResponse{
			Kind:        "storage#object",
			Bucket:      meta.Bucket,
			Name:        meta.Key,
			Size:        strconv.FormatInt(meta.Size, 10),
			ETag:        strings.Trim(meta.ETag, "\""),
			TimeCreated: meta.CreatedAt.UTC().Format(time.RFC3339),
			Updated:     meta.ModifiedAt.UTC().Format(time.RFC3339),
		}
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
	resource := gcsObjectResource(bucket, key)
	if !allowAuthenticatedSubject(r, "storage.objects.delete", resource) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	if err := deps.Metadata.DeleteObject(r.Context(), bucket, key); err != nil {
		writeError(w, err)
		return
	}
	if err := deps.Objects.DeleteObject(r.Context(), bucket, key); err != nil && err != core.ErrNotFound {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

// allowAuthenticatedSubject keeps action/resource labels for API compatibility,
// but only authenticated-subject presence is currently enforced.
func allowAuthenticatedSubject(r *http.Request, action, resource string) bool {
	_ = action
	_ = resource
	return hasAuthenticatedSubject(r)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), httpx.StatusCode(err))
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

func gcsProjectResource() string {
	return "projects/_/buckets"
}

func gcsBucketResource(bucket string) string {
	return "projects/_/buckets/" + bucket
}

func gcsObjectResource(bucket, key string) string {
	return "projects/_/buckets/" + bucket + "/objects/" + key
}

func pathObject(r *http.Request) (string, error) {
	raw := r.PathValue("object")
	if raw == "" {
		return "", nil
	}
	return url.PathUnescape(raw)
}
