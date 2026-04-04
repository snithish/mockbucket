package s3

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/httpx"
)

type completeMultipartUploadRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		PartNumber int    `xml:"PartNumber"`
		ETag       string `xml:"ETag"`
	} `xml:"Part"`
}

type s3Bucket struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type listBucketsResult struct {
	XMLName xml.Name `xml:"ListAllMyBucketsResult"`
	Xmlns   string   `xml:"xmlns,attr"`
	Owner   struct {
		ID          string `xml:"ID"`
		DisplayName string `xml:"DisplayName"`
	} `xml:"Owner"`
	Buckets struct {
		Items []s3Bucket `xml:"Bucket"`
	} `xml:"Buckets"`
}

type locationConstraintResponse struct {
	XMLName            xml.Name `xml:"LocationConstraint"`
	Xmlns              string   `xml:"xmlns,attr"`
	LocationConstraint string   `xml:",chardata"`
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

type listMultipartUploadsResult struct {
	XMLName        xml.Name          `xml:"ListMultipartUploadsResult"`
	Xmlns          string            `xml:"xmlns,attr"`
	Bucket         string            `xml:"Bucket"`
	KeyMarker      string            `xml:"KeyMarker,omitempty"`
	UploadIDMarker string            `xml:"UploadIdMarker,omitempty"`
	NextKeyMarker  string            `xml:"NextKeyMarker,omitempty"`
	NextUploadID   string            `xml:"NextUploadIdMarker,omitempty"`
	MaxUploads     int               `xml:"MaxUploads"`
	IsTruncated    bool              `xml:"IsTruncated"`
	Prefix         string            `xml:"Prefix,omitempty"`
	Uploads        []multipartUpload `xml:"Upload,omitempty"`
}

type multipartUpload struct {
	Key       string `xml:"Key"`
	UploadID  string `xml:"UploadId"`
	Initiated string `xml:"Initiated"`
}

type listPartsResult struct {
	XMLName              xml.Name        `xml:"ListPartsResult"`
	Xmlns                string          `xml:"xmlns,attr"`
	Bucket               string          `xml:"Bucket"`
	Key                  string          `xml:"Key"`
	UploadID             string          `xml:"UploadId"`
	PartNumberMarker     int             `xml:"PartNumberMarker"`
	NextPartNumberMarker int             `xml:"NextPartNumberMarker"`
	MaxParts             int             `xml:"MaxParts"`
	IsTruncated          bool            `xml:"IsTruncated"`
	Parts                []multipartPart `xml:"Part,omitempty"`
}

type multipartPart struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

func AddressingStyleMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket, ok := bucketFromVirtualHost(r.Host)
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		clone := r.Clone(r.Context())
		clonedURL := *r.URL
		clone.URL = &clonedURL
		clone.URL.Path = rewriteVirtualHostedPath(clone.URL.Path, bucket)
		if clone.URL.RawPath != "" {
			clone.URL.RawPath = rewriteVirtualHostedPath(clone.URL.RawPath, bucket)
		}
		next.ServeHTTP(w, clone)
	})
}

func openObjectForRead(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) (core.ObjectMetadata, io.ReadCloser, bool) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return core.ObjectMetadata{}, nil, false
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeBucketError(w, err)
		return core.ObjectMetadata{}, nil, false
	}
	meta, err := deps.Metadata.GetObject(r.Context(), bucket, key)
	if err != nil {
		writeError(w, err)
		return core.ObjectMetadata{}, nil, false
	}
	reader, _, err := deps.Objects.OpenObject(r.Context(), bucket, key)
	if err != nil {
		writeError(w, err)
		return core.ObjectMetadata{}, nil, false
	}
	if !evaluateConditionalRead(w, r, meta) {
		_ = reader.Close()
		return core.ObjectMetadata{}, nil, false
	}
	return meta, reader, true
}

func loadMultipartUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) (core.MultipartUpload, bool) {
	uploadID := r.URL.Query().Get("uploadId")
	upload, err := deps.Metadata.GetMultipartUpload(r.Context(), uploadID)
	if err != nil {
		writeError(w, err)
		return core.MultipartUpload{}, false
	}
	if upload.Bucket != bucket || upload.Key != key {
		writeError(w, core.ErrInvalidArgument)
		return core.MultipartUpload{}, false
	}
	return upload, true
}

func decodeCompleteMultipartUpload(w http.ResponseWriter, r *http.Request) (completeMultipartUploadRequest, bool) {
	var payload completeMultipartUploadRequest
	if err := xml.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, core.ErrInvalidArgument)
		return completeMultipartUploadRequest{}, false
	}
	if payload.XMLName.Local != "CompleteMultipartUpload" || len(payload.Parts) == 0 {
		writeError(w, core.ErrInvalidArgument)
		return completeMultipartUploadRequest{}, false
	}
	return payload, true
}

func resolveMultipartParts(w http.ResponseWriter, r *http.Request, deps common.Dependencies, uploadID string, payload completeMultipartUploadRequest) ([]core.MultipartPart, bool) {
	storedParts, err := deps.Metadata.ListMultipartParts(r.Context(), uploadID)
	if err != nil {
		writeError(w, err)
		return nil, false
	}
	partByNumber := make(map[int]core.MultipartPart, len(storedParts))
	for _, part := range storedParts {
		partByNumber[part.PartNumber] = part
	}
	ordered := make([]core.MultipartPart, 0, len(payload.Parts))
	prevPartNumber := 0
	for _, reqPart := range payload.Parts {
		if reqPart.PartNumber <= 0 || reqPart.PartNumber <= prevPartNumber {
			writeError(w, core.ErrInvalidArgument)
			return nil, false
		}
		prevPartNumber = reqPart.PartNumber
		stored, ok := partByNumber[reqPart.PartNumber]
		if !ok {
			writeError(w, core.ErrInvalidArgument)
			return nil, false
		}
		if etag := strings.Trim(reqPart.ETag, `"`); etag != "" && etag != stored.ETag {
			writeError(w, core.ErrInvalidArgument)
			return nil, false
		}
		ordered = append(ordered, stored)
	}
	return ordered, true
}

func finalizeMultipartUpload(ctx context.Context, deps common.Dependencies, uploadID, bucket, key string) error {
	if err := deps.Metadata.DeleteMultipartUpload(ctx, uploadID); err != nil {
		_ = common.DeleteObjectIfExists(ctx, deps, bucket, key)
		return err
	}
	if err := deps.Objects.AbortMultipartUpload(ctx, uploadID); err != nil && err != core.ErrNotFound {
		_ = common.DeleteObjectIfExists(ctx, deps, bucket, key)
		return err
	}
	return nil
}

func newS3Bucket(bucket core.Bucket) s3Bucket {
	return s3Bucket{Name: bucket.Name, CreationDate: bucket.CreatedAt.UTC().Format(time.RFC3339)}
}

func newLocationConstraintResponse() locationConstraintResponse {
	return locationConstraintResponse{Xmlns: xmlNamespace, LocationConstraint: "us-east-1"}
}

func newCopyObjectResult(meta core.ObjectMetadata) copyObjectResult {
	return copyObjectResult{
		Xmlns:        xmlNamespace,
		LastModified: meta.ModifiedAt.UTC().Format(time.RFC3339),
		ETag:         `"` + meta.ETag + `"`,
	}
}

func newCompleteMultipartUploadResult(bucket, key string, meta core.ObjectMetadata) completeMultipartUploadResult {
	return completeMultipartUploadResult{
		Xmlns:    xmlNamespace,
		Location: "/" + bucket + "/" + key,
		Bucket:   bucket,
		Key:      key,
		ETag:     `"` + meta.ETag + `"`,
	}
}

func bucketFromVirtualHost(hostport string) (string, bool) {
	host := hostport
	if strings.Contains(hostport, ":") {
		parsedHost, _, err := net.SplitHostPort(hostport)
		if err == nil {
			host = parsedHost
		}
	}
	host = strings.TrimSpace(host)
	if host == "" || strings.EqualFold(host, "localhost") || net.ParseIP(host) != nil {
		return "", false
	}
	if !strings.HasSuffix(strings.ToLower(host), ".localhost") {
		return "", false
	}
	bucket := strings.TrimSuffix(host, ".localhost")
	if bucket == "" || strings.Contains(bucket, ".") {
		return "", false
	}
	return bucket, true
}

func rewriteVirtualHostedPath(path, bucket string) string {
	if path == "" || path == "/" {
		return "/" + bucket
	}
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == bucket || strings.HasPrefix(trimmed, bucket+"/") {
		return path
	}
	return "/" + bucket + "/" + trimmed
}

func hasLocationQuery(r *http.Request) bool {
	_, ok := r.URL.Query()["location"]
	return ok
}

func hasListObjectsV2Query(r *http.Request) bool {
	return r.URL.Query().Get("list-type") == "2"
}

func hasUploadsQuery(r *http.Request) bool {
	_, ok := r.URL.Query()["uploads"]
	return ok
}

func hasDeleteQuery(r *http.Request) bool {
	_, ok := r.URL.Query()["delete"]
	return ok
}

func hasUploadIDQuery(r *http.Request) bool {
	return r.URL.Query().Get("uploadId") != ""
}

func parseMaxUploads(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("max-uploads")
	if raw == "" {
		return 1000, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, core.ErrInvalidArgument
	}
	if value > 1000 {
		value = 1000
	}
	return value, nil
}

func parseMaxParts(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("max-parts")
	if raw == "" {
		return 1000, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, core.ErrInvalidArgument
	}
	if value > 1000 {
		value = 1000
	}
	return value, nil
}

func parsePartNumberMarker(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("part-number-marker")
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, core.ErrInvalidArgument
	}
	return value, nil
}

func parseCopySource(raw string) (string, string, error) {
	trimmed := strings.TrimPrefix(raw, "/")
	decoded, err := url.PathUnescape(trimmed)
	if err != nil {
		return "", "", err
	}
	parts := strings.SplitN(decoded, "/", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
		return "", "", core.ErrInvalidArgument
	}
	return parts[0], parts[1], nil
}

func setObjectHeaders(w http.ResponseWriter, meta core.ObjectMetadata) {
	if meta.ETag != "" {
		w.Header().Set("ETag", "\""+meta.ETag+"\"")
	}
	if meta.ContentType != "" {
		w.Header().Set("Content-Type", meta.ContentType)
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
		w.Header().Set("x-amz-meta-"+key, value)
	}
	if !meta.ModifiedAt.IsZero() {
		w.Header().Set("Last-Modified", meta.ModifiedAt.UTC().Format(http.TimeFormat))
	}
}

func setObjectHeadersWithLength(w http.ResponseWriter, meta core.ObjectMetadata) {
	setObjectHeaders(w, meta)
	if meta.ContentType == "" {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	if meta.Size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	}
}

func evaluateConditionalRead(w http.ResponseWriter, r *http.Request, meta core.ObjectMetadata) bool {
	if !matchesIfMatch(r.Header.Values("If-Match"), meta.ETag) {
		writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold.")
		return false
	}
	if !matchesIfUnmodifiedSince(r.Header.Get("If-Unmodified-Since"), meta.ModifiedAt) {
		writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold.")
		return false
	}
	if matchesIfNoneMatch(r.Header.Values("If-None-Match"), meta.ETag) {
		w.WriteHeader(http.StatusNotModified)
		return false
	}
	if !matchesIfModifiedSince(r.Header.Get("If-Modified-Since"), meta.ModifiedAt) {
		w.WriteHeader(http.StatusNotModified)
		return false
	}
	return true
}

func matchesIfMatch(values []string, etag string) bool {
	tags := parseETagConditions(values)
	if len(tags) == 0 {
		return true
	}
	for _, tag := range tags {
		if tag == "*" || tag == etag {
			return true
		}
	}
	return false
}

func matchesIfNoneMatch(values []string, etag string) bool {
	tags := parseETagConditions(values)
	if len(tags) == 0 {
		return false
	}
	for _, tag := range tags {
		if tag == "*" || tag == etag {
			return true
		}
	}
	return false
}

func matchesIfModifiedSince(raw string, modifiedAt time.Time) bool {
	since, ok := parseHTTPTime(raw)
	if !ok {
		return true
	}
	return modifiedAt.After(since)
}

func matchesIfUnmodifiedSince(raw string, modifiedAt time.Time) bool {
	since, ok := parseHTTPTime(raw)
	if !ok {
		return true
	}
	return !modifiedAt.After(since)
}

func parseETagConditions(values []string) []string {
	var tags []string
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			tag := strings.TrimSpace(part)
			tag = strings.TrimPrefix(tag, "W/")
			tag = strings.Trim(tag, `"`)
			if tag == "" {
				continue
			}
			tags = append(tags, tag)
		}
	}
	return tags
}

func parseHTTPTime(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	parsed, err := http.ParseTime(raw)
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

func objectMetadataFromRequest(r *http.Request) core.ObjectMetadata {
	return core.ObjectMetadata{
		ContentType:        strings.TrimSpace(r.Header.Get("Content-Type")),
		CacheControl:       strings.TrimSpace(r.Header.Get("Cache-Control")),
		ContentDisposition: strings.TrimSpace(r.Header.Get("Content-Disposition")),
		ContentEncoding:    sanitizeS3ContentEncoding(r.Header.Values("Content-Encoding")),
		ContentLanguage:    strings.TrimSpace(r.Header.Get("Content-Language")),
		CustomMetadata:     extractPrefixedHeaders(r.Header, "x-amz-meta-"),
	}
}

func copyObjectMetadata(src core.ObjectMetadata) core.ObjectMetadata {
	return core.ObjectMetadata{
		ContentType:        src.ContentType,
		CacheControl:       src.CacheControl,
		ContentDisposition: src.ContentDisposition,
		ContentEncoding:    src.ContentEncoding,
		ContentLanguage:    src.ContentLanguage,
		CustomMetadata:     cloneCustomMetadata(src.CustomMetadata),
	}
}

func sanitizeS3ContentEncoding(values []string) string {
	var encodings []string
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			encoding := strings.TrimSpace(part)
			if encoding == "" || strings.EqualFold(encoding, "aws-chunked") {
				continue
			}
			encodings = append(encodings, encoding)
		}
	}
	if len(encodings) == 0 {
		return ""
	}
	return strings.Join(slices.Compact(encodings), ", ")
}

func extractPrefixedHeaders(headers http.Header, prefix string) map[string]string {
	var metadata map[string]string
	for key, values := range headers {
		lowerKey := strings.ToLower(key)
		if !strings.HasPrefix(lowerKey, prefix) || len(values) == 0 {
			continue
		}
		if metadata == nil {
			metadata = make(map[string]string)
		}
		metadata[strings.TrimPrefix(lowerKey, prefix)] = values[0]
	}
	return metadata
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

func writeXML(w http.ResponseWriter, status int, payload any) {
	raw, err := xml.MarshalIndent(payload, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header + string(raw)))
}

func writeError(w http.ResponseWriter, err error) {
	status, code, message := s3ErrorDetails(err)
	writeS3Error(w, status, code, message)
}

func writeBucketError(w http.ResponseWriter, err error) {
	if err == core.ErrNotFound {
		writeS3Error(w, http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist.")
		return
	}
	writeError(w, err)
}

func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	payload := struct {
		XMLName   xml.Name `xml:"Error"`
		Code      string   `xml:"Code"`
		Message   string   `xml:"Message"`
		RequestID string   `xml:"RequestId"`
	}{
		Code:      code,
		Message:   message,
		RequestID: "mockbucket",
	}
	writeXML(w, status, payload)
}

func s3ErrorDetails(err error) (int, string, string) {
	switch {
	case err == nil:
		return http.StatusOK, "", ""
	case err == core.ErrConflict:
		return http.StatusConflict, "Conflict", err.Error()
	case err == core.ErrNotFound:
		return http.StatusNotFound, "NoSuchKey", "The specified key does not exist."
	case err == core.ErrInvalidArgument:
		return http.StatusBadRequest, "InvalidArgument", err.Error()
	case err == core.ErrAccessDenied:
		return http.StatusForbidden, "AccessDenied", "Access Denied"
	case err == core.ErrUnauthenticated:
		return http.StatusUnauthorized, "AccessDenied", "Access Denied"
	case err == core.ErrExpiredToken:
		return http.StatusUnauthorized, "ExpiredToken", "The provided token has expired."
	case err == core.ErrUnsupported:
		return http.StatusNotImplemented, "NotImplemented", err.Error()
	default:
		return httpx.StatusCode(err), "InternalError", err.Error()
	}
}

func parseMaxKeys(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("max-keys")
	if raw == "" {
		return 1000, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, core.ErrInvalidArgument
	}
	return value, nil
}

func parsePartNumber(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("partNumber")
	if raw == "" {
		return 0, core.ErrInvalidArgument
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return 0, core.ErrInvalidArgument
	}
	return value, nil
}

func newUploadID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func parseRange(header string, size int64) (int64, int64, error) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, fmt.Errorf("unsupported range unit")
	}
	spec := strings.TrimPrefix(header, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("malformed range: %s", header)
	}

	startStr, endStr := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])

	var start, end int64
	var err error

	switch {
	case startStr == "" && endStr == "":
		return 0, 0, fmt.Errorf("malformed range: %s", header)
	case startStr == "":
		suffix, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, fmt.Errorf("malformed suffix range")
		}
		start = size - suffix
		if start < 0 {
			start = 0
		}
		end = size - 1
	case endStr == "":
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil || start < 0 {
			return 0, 0, fmt.Errorf("malformed range start")
		}
		end = size - 1
	default:
		start, err = strconv.ParseInt(startStr, 10, 64)
		if err != nil || start < 0 {
			return 0, 0, fmt.Errorf("malformed range start")
		}
		end, err = strconv.ParseInt(endStr, 10, 64)
		if err != nil || end < 0 {
			return 0, 0, fmt.Errorf("malformed range end")
		}
	}

	if start >= size {
		return 0, 0, fmt.Errorf("range start %d exceeds size %d", start, size)
	}
	if end >= size {
		end = size - 1
	}
	if start > end {
		return 0, 0, fmt.Errorf("range start %d > end %d", start, end)
	}
	return start, end, nil
}
