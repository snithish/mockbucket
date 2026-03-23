package gcs

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	authgcp "github.com/snithish/mockbucket/internal/auth/gcp"
	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/httpx"
)

const (
	bucketsBasePath = "/storage/v1/b"
	uploadBasePath  = "/upload/storage/v1/b"
)

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies) {
	if !cfg.Frontends.GCS {
		return
	}
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
		key := r.PathValue("object")
		switch r.Method {
		case http.MethodGet:
			handleGetObject(w, r, deps, bucket, key)
		case http.MethodDelete:
			handleDeleteObject(w, r, deps, bucket, key)
		default:
			http.NotFound(w, r)
		}
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
	mux.Handle(uploadBasePath+"/{bucket}/o", uploadHandler)
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
	if !allow(r, deps, "s3:ListAllMyBuckets", "*") {
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
	resource := bucketResource(bucket)
	if !allow(r, deps, "s3:CreateBucket", resource) {
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
	resource := bucketResource(bucket)
	if !allow(r, deps, "s3:ListBucket", resource) {
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
	resource := bucketResource(bucket)
	if !allow(r, deps, "s3:ListBucket", resource) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	pageToken := r.URL.Query().Get("pageToken")
	maxResults := parseMaxResults(r.URL.Query().Get("maxResults"))
	objects, err := deps.Metadata.ListObjects(r.Context(), bucket, prefix, maxResults, pageToken)
	if err != nil {
		writeError(w, err)
		return
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
	if len(objects) == maxResults && len(objects) > 0 {
		resp.NextPageToken = objects[len(objects)-1].Key
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleUploadObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if r.URL.Query().Get("uploadType") != "media" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	key := strings.TrimSpace(r.URL.Query().Get("name"))
	if key == "" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	resource := objectResource(bucket, key)
	if !allow(r, deps, "s3:PutObject", resource) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	meta, err := deps.Objects.PutObject(r.Context(), bucket, key, r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := deps.Metadata.PutObject(r.Context(), meta); err != nil {
		writeError(w, err)
		return
	}
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
}

func handleGetObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return
	}
	resource := objectResource(bucket, key)
	if !allow(r, deps, "s3:GetObject", resource) {
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
	if r.URL.Query().Get("alt") != "media" {
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
	resource := objectResource(bucket, key)
	if !allow(r, deps, "s3:DeleteObject", resource) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	if err := deps.Objects.DeleteObject(r.Context(), bucket, key); err != nil {
		writeError(w, err)
		return
	}
	if err := deps.Metadata.DeleteObject(r.Context(), bucket, key); err != nil {
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

func allow(r *http.Request, deps common.Dependencies, action, resource string) bool {
	subject, ok := httpx.SubjectFromContext(r.Context())
	if !ok {
		return false
	}
	return deps.Policy.Allowed(action, resource, subject.Policies)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), httpx.StatusCode(err))
}

func bucketResource(bucket string) string {
	return "arn:mockbucket:s3:::" + bucket
}

func objectResource(bucket, key string) string {
	return "arn:mockbucket:s3:::" + bucket + "/" + key
}
