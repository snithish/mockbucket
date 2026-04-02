package gcs

import (
	"encoding/base64"
	"encoding/json"
	"errors"
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
	Kind               string            `json:"kind"`
	ID                 string            `json:"id"`
	Bucket             string            `json:"bucket"`
	Name               string            `json:"name"`
	Size               string            `json:"size"`
	CRC32C             string            `json:"crc32c,omitempty"`
	ETag               string            `json:"etag"`
	Generation         string            `json:"generation"`
	Metageneration     string            `json:"metageneration"`
	TimeCreated        string            `json:"timeCreated"`
	Updated            string            `json:"updated"`
	ContentType        string            `json:"contentType,omitempty"`
	CacheControl       string            `json:"cacheControl,omitempty"`
	ContentDisposition string            `json:"contentDisposition,omitempty"`
	ContentEncoding    string            `json:"contentEncoding,omitempty"`
	ContentLanguage    string            `json:"contentLanguage,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

type listObjectsResponse struct {
	Kind          string           `json:"kind"`
	Items         []objectResponse `json:"items,omitempty"`
	Prefixes      []string         `json:"prefixes,omitempty"`
	NextPageToken string           `json:"nextPageToken,omitempty"`
}

var errConditionNotMet = errors.New("condition not met")

type objectPreconditions struct {
	GenerationMatch     *int64
	MetagenerationMatch *int64
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

func writePreconditionFailed(w http.ResponseWriter) {
	writeJSON(w, http.StatusPreconditionFailed, map[string]any{
		"error": map[string]any{
			"code":    http.StatusPreconditionFailed,
			"message": "At least one of the pre-conditions you specified did not hold.",
			"errors": []map[string]any{
				{
					"message": "At least one of the pre-conditions you specified did not hold.",
					"domain":  "global",
					"reason":  "conditionNotMet",
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

func parseObjectPreconditions(r *http.Request) (objectPreconditions, error) {
	preconditions := objectPreconditions{}
	ifGenerationMatch, err := parseOptionalInt64(r.URL.Query().Get("ifGenerationMatch"))
	if err != nil {
		return objectPreconditions{}, err
	}
	ifMetagenerationMatch, err := parseOptionalInt64(r.URL.Query().Get("ifMetagenerationMatch"))
	if err != nil {
		return objectPreconditions{}, err
	}
	preconditions.GenerationMatch = ifGenerationMatch
	preconditions.MetagenerationMatch = ifMetagenerationMatch
	return preconditions, nil
}

func parseOptionalInt64(raw string) (*int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return nil, core.ErrInvalidArgument
	}
	return &value, nil
}

func checkExistingObjectPreconditions(meta core.ObjectMetadata, preconditions objectPreconditions) error {
	if preconditions.GenerationMatch != nil && meta.Generation != *preconditions.GenerationMatch {
		return errConditionNotMet
	}
	if preconditions.MetagenerationMatch != nil && meta.Metageneration != *preconditions.MetagenerationMatch {
		return errConditionNotMet
	}
	return nil
}

func checkWriteObjectPreconditions(current *core.ObjectMetadata, preconditions objectPreconditions) error {
	if current == nil {
		if preconditions.GenerationMatch != nil && *preconditions.GenerationMatch == 0 && preconditions.MetagenerationMatch == nil {
			return nil
		}
		if preconditions.GenerationMatch != nil || preconditions.MetagenerationMatch != nil {
			return errConditionNotMet
		}
		return nil
	}
	return checkExistingObjectPreconditions(*current, preconditions)
}

func newObjectResponse(meta core.ObjectMetadata, crc32c string) objectResponse {
	generation := objectGeneration(meta)
	return objectResponse{
		Kind:               "storage#object",
		ID:                 meta.Bucket + "/" + meta.Key + "/" + generation,
		Bucket:             meta.Bucket,
		Name:               meta.Key,
		Size:               strconv.FormatInt(meta.Size, 10),
		CRC32C:             crc32c,
		ETag:               strings.Trim(meta.ETag, "\""),
		Generation:         generation,
		Metageneration:     objectMetageneration(meta),
		TimeCreated:        meta.CreatedAt.UTC().Format(time.RFC3339),
		Updated:            meta.ModifiedAt.UTC().Format(time.RFC3339),
		ContentType:        meta.ContentType,
		CacheControl:       meta.CacheControl,
		ContentDisposition: meta.ContentDisposition,
		ContentEncoding:    meta.ContentEncoding,
		ContentLanguage:    meta.ContentLanguage,
		Metadata:           cloneCustomMetadata(meta.CustomMetadata),
	}
}

func applyObjectHeaders(w http.ResponseWriter, meta core.ObjectMetadata, defaultContentType string) {
	contentType := meta.ContentType
	if contentType == "" {
		contentType = defaultContentType
	}
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	if meta.CacheControl != "" {
		w.Header().Set("Cache-Control", meta.CacheControl)
	}
	if meta.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", meta.ContentDisposition)
	}
	if meta.ContentEncoding != "" {
		w.Header().Set("Content-Encoding", meta.ContentEncoding)
	}
	if meta.ContentLanguage != "" {
		w.Header().Set("Content-Language", meta.ContentLanguage)
	}
	for key, value := range meta.CustomMetadata {
		w.Header().Set("x-goog-meta-"+key, value)
	}
}

func cloneCustomMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(metadata))
	for key, value := range metadata {
		cloned[key] = value
	}
	return cloned
}

func objectMetageneration(meta core.ObjectMetadata) string {
	if meta.Metageneration > 0 {
		return strconv.FormatInt(meta.Metageneration, 10)
	}
	return "1"
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
