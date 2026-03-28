package gcs

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/httpx"
)

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

func decodeResumableUploadKey(w http.ResponseWriter, r *http.Request) (string, bool) {
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
		return "", false
	}
	return key, true
}

func requireAuthenticatedRequest(w http.ResponseWriter, r *http.Request) bool {
	if requireAuthenticatedSubject(r) {
		return true
	}
	writeError(w, core.ErrAccessDenied)
	return false
}

func loadBucket(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) (core.Bucket, bool) {
	meta, err := deps.Metadata.GetBucket(r.Context(), bucket)
	if err != nil {
		writeError(w, err)
		return core.Bucket{}, false
	}
	return meta, true
}

func loadObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) (core.ObjectMetadata, bool) {
	meta, err := deps.Metadata.GetObject(r.Context(), bucket, key)
	if err != nil {
		writeError(w, err)
		return core.ObjectMetadata{}, false
	}
	return meta, true
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

func newBucketResponse(name string, createdAt time.Time) bucketResponse {
	return bucketResponse{
		Kind:        "storage#bucket",
		ID:          name,
		Name:        name,
		TimeCreated: createdAt.UTC().Format(time.RFC3339),
	}
}

func writeResumableIncomplete(w http.ResponseWriter, received int64) {
	if received > 0 {
		w.Header().Set("Range", "bytes=0-"+strconv.FormatInt(received-1, 10))
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

func pathObject(r *http.Request) (string, error) {
	raw := r.PathValue("object")
	if raw == "" {
		return "", nil
	}
	return url.PathUnescape(raw)
}
