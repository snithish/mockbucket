package s3

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/httpx"
)

const xmlNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies) {
	if !cfg.Frontends.S3 {
		return
	}
	bucketHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := r.PathValue("bucket")
		switch {
		case r.Method == http.MethodPut:
			handleCreateBucket(w, r, deps, bucket)
		case r.Method == http.MethodHead:
			handleHeadBucket(w, r, deps, bucket)
		case r.Method == http.MethodGet && hasLocationQuery(r):
			handleGetBucketLocation(w, r, deps, bucket)
		case r.Method == http.MethodGet && hasListObjectsV2Query(r):
			handleListObjectsV2(w, r, deps, bucket)
		default:
			http.NotFound(w, r)
		}
	})
	objectHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := r.PathValue("bucket")
		key := r.PathValue("key")
		switch {
		case key == "" && r.Method == http.MethodGet && hasListObjectsV2Query(r):
			handleListObjectsV2(w, r, deps, bucket)
		case key == "" && r.Method == http.MethodGet && hasLocationQuery(r):
			handleGetBucketLocation(w, r, deps, bucket)
		case key == "":
			http.NotFound(w, r)
		case r.Method == http.MethodPost && hasUploadsQuery(r):
			handleCreateMultipartUpload(w, r, deps, bucket, key)
		case r.Method == http.MethodPut && hasUploadIDQuery(r):
			handleUploadPart(w, r, deps, bucket, key)
		case r.Method == http.MethodPost && hasUploadIDQuery(r):
			handleCompleteMultipartUpload(w, r, deps, bucket, key)
		case r.Method == http.MethodDelete && hasUploadIDQuery(r):
			handleAbortMultipartUpload(w, r, deps, bucket, key)
		case r.Method == http.MethodPut:
			handlePutObject(w, r, deps, bucket, key)
		case r.Method == http.MethodGet:
			handleGetObject(w, r, deps, bucket, key)
		case r.Method == http.MethodHead:
			handleHeadObject(w, r, deps, bucket, key)
		case r.Method == http.MethodDelete:
			handleDeleteObject(w, r, deps, bucket, key)
		default:
			http.NotFound(w, r)
		}
	})
	mux.Handle("/{bucket}", bucketHandler)
	mux.Handle("/{bucket}/{key...}", objectHandler)
}

func RootHandler(_ config.Config, deps common.Dependencies) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleListBuckets(w, r, deps)
	})
}

func handleListBuckets(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	buckets, err := deps.Metadata.ListBuckets(r.Context())
	if err != nil {
		writeError(w, err)
		return
	}
	type bucket struct {
		Name         string `xml:"Name"`
		CreationDate string `xml:"CreationDate"`
	}
	response := struct {
		XMLName xml.Name `xml:"ListAllMyBucketsResult"`
		Xmlns   string   `xml:"xmlns,attr"`
		Owner   struct {
			ID          string `xml:"ID"`
			DisplayName string `xml:"DisplayName"`
		} `xml:"Owner"`
		Buckets struct {
			Items []bucket `xml:"Bucket"`
		} `xml:"Buckets"`
	}{Xmlns: xmlNamespace}
	response.Owner.ID = "mockbucket"
	response.Owner.DisplayName = "mockbucket"
	for _, item := range buckets {
		response.Buckets.Items = append(response.Buckets.Items, bucket{Name: item.Name, CreationDate: item.CreatedAt.UTC().Format(time.RFC3339)})
	}
	writeXML(w, http.StatusOK, response)
}

func handleCreateBucket(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if err := deps.Metadata.CreateBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func handleHeadBucket(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func handleGetBucketLocation(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	response := struct {
		XMLName            xml.Name `xml:"LocationConstraint"`
		Xmlns              string   `xml:"xmlns,attr"`
		LocationConstraint string   `xml:",chardata"`
	}{Xmlns: xmlNamespace, LocationConstraint: "us-east-1"}
	writeXML(w, http.StatusOK, response)
}

func handlePutObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
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
		_ = deps.Objects.DeleteObject(r.Context(), bucket, key)
		writeError(w, err)
		return
	}
	setObjectHeaders(w, meta)
	w.WriteHeader(http.StatusOK)
}

func handleGetObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
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
	reader, _, err := deps.Objects.OpenObject(r.Context(), bucket, key)
	if err != nil {
		writeError(w, err)
		return
	}
	defer func() { _ = reader.Close() }()
	setObjectHeaders(w, meta)
	w.Header().Set("Content-Type", "application/octet-stream")

	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" || meta.Size <= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, reader)
		return
	}

	start, end, err := parseRange(rangeHeader, meta.Size)
	if err != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, reader)
		return
	}

	if seeker, ok := reader.(io.ReadSeeker); ok {
		_, _ = seeker.Seek(start, io.SeekStart)
	} else {
		_, _ = io.CopyN(io.Discard, reader, start)
	}

	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, meta.Size))
	w.WriteHeader(http.StatusPartialContent)
	_, _ = io.CopyN(w, reader, end-start+1)
}

func handleHeadObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
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
	reader, _, err := deps.Objects.OpenObject(r.Context(), bucket, key)
	if err != nil {
		writeError(w, err)
		return
	}
	_ = reader.Close()
	setObjectHeadersWithLength(w, meta)
	w.WriteHeader(http.StatusOK)
}

func handleDeleteObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
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

func handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	uploadID, err := newUploadID()
	if err != nil {
		writeError(w, err)
		return
	}
	upload := core.MultipartUpload{
		UploadID:  uploadID,
		Bucket:    bucket,
		Key:       key,
		CreatedAt: time.Now().UTC(),
	}
	if err := deps.Metadata.CreateMultipartUpload(r.Context(), upload); err != nil {
		writeError(w, err)
		return
	}
	response := struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}{Xmlns: xmlNamespace, Bucket: bucket, Key: key, UploadID: uploadID}
	writeXML(w, http.StatusOK, response)
}

func handleUploadPart(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	partNumber, err := parsePartNumber(r)
	if err != nil {
		writeError(w, err)
		return
	}
	upload, err := deps.Metadata.GetMultipartUpload(r.Context(), uploadID)
	if err != nil {
		writeError(w, err)
		return
	}
	if upload.Bucket != bucket || upload.Key != key {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	part, err := deps.Objects.PutMultipartPart(r.Context(), uploadID, partNumber, r.Body)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := deps.Metadata.PutMultipartPart(r.Context(), part); err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("ETag", `"`+part.ETag+`"`)
	w.WriteHeader(http.StatusOK)
}

func handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	upload, err := deps.Metadata.GetMultipartUpload(r.Context(), uploadID)
	if err != nil {
		writeError(w, err)
		return
	}
	if upload.Bucket != bucket || upload.Key != key {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	var payload struct {
		Parts []struct {
			PartNumber int    `xml:"PartNumber"`
			ETag       string `xml:"ETag"`
		} `xml:"Part"`
	}
	if err := xml.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if len(payload.Parts) == 0 {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	storedParts, err := deps.Metadata.ListMultipartParts(r.Context(), uploadID)
	if err != nil {
		writeError(w, err)
		return
	}
	partByNumber := make(map[int]core.MultipartPart, len(storedParts))
	for _, part := range storedParts {
		partByNumber[part.PartNumber] = part
	}
	ordered := make([]core.MultipartPart, 0, len(payload.Parts))
	seen := make(map[int]struct{}, len(payload.Parts))
	for _, reqPart := range payload.Parts {
		if reqPart.PartNumber <= 0 {
			writeError(w, core.ErrInvalidArgument)
			return
		}
		if _, ok := seen[reqPart.PartNumber]; ok {
			writeError(w, core.ErrInvalidArgument)
			return
		}
		seen[reqPart.PartNumber] = struct{}{}
		stored, ok := partByNumber[reqPart.PartNumber]
		if !ok {
			writeError(w, core.ErrInvalidArgument)
			return
		}
		if etag := strings.Trim(reqPart.ETag, `"`); etag != "" && etag != stored.ETag {
			writeError(w, core.ErrInvalidArgument)
			return
		}
		ordered = append(ordered, stored)
	}
	meta, err := deps.Objects.CompleteMultipartUpload(r.Context(), bucket, key, ordered)
	if err != nil {
		writeError(w, err)
		return
	}
	if err := deps.Metadata.PutObject(r.Context(), meta); err != nil {
		_ = deps.Objects.DeleteObject(r.Context(), bucket, key)
		writeError(w, err)
		return
	}
	if err := deps.Metadata.DeleteMultipartUpload(r.Context(), uploadID); err != nil {
		_ = deps.Metadata.DeleteObject(r.Context(), bucket, key)
		_ = deps.Objects.DeleteObject(r.Context(), bucket, key)
		writeError(w, err)
		return
	}
	if err := deps.Objects.AbortMultipartUpload(r.Context(), uploadID); err != nil && err != core.ErrNotFound {
		_ = deps.Metadata.DeleteObject(r.Context(), bucket, key)
		_ = deps.Objects.DeleteObject(r.Context(), bucket, key)
		writeError(w, err)
		return
	}
	response := struct {
		XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
		Xmlns    string   `xml:"xmlns,attr"`
		Location string   `xml:"Location"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		ETag     string   `xml:"ETag"`
	}{Xmlns: xmlNamespace, Location: "/" + bucket + "/" + key, Bucket: bucket, Key: key, ETag: `"` + meta.ETag + `"`}
	writeXML(w, http.StatusOK, response)
}

func handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	upload, err := deps.Metadata.GetMultipartUpload(r.Context(), uploadID)
	if err != nil {
		writeError(w, err)
		return
	}
	if upload.Bucket != bucket || upload.Key != key {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if err := deps.Metadata.DeleteMultipartUpload(r.Context(), uploadID); err != nil {
		writeError(w, err)
		return
	}
	_ = deps.Objects.AbortMultipartUpload(r.Context(), uploadID)
	w.WriteHeader(http.StatusNoContent)
}

func handleListObjectsV2(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	maxKeys, err := parseMaxKeys(r)
	if err != nil {
		writeError(w, err)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	continuation := r.URL.Query().Get("continuation-token")
	startAfter := r.URL.Query().Get("start-after")
	after := continuation
	if after == "" {
		after = startAfter
	}
	if after == "" {
		after = r.URL.Query().Get("marker")
	}
	objects := []core.ObjectMetadata{}
	isTruncated := false
	nextContinuation := ""
	if maxKeys != 0 {
		fetchLimit := maxKeys + 1
		items, err := deps.Metadata.ListObjects(r.Context(), bucket, prefix, fetchLimit, after)
		if err != nil {
			writeError(w, err)
			return
		}
		if len(items) > maxKeys {
			isTruncated = true
			items = items[:maxKeys]
		}
		if isTruncated && len(items) > 0 {
			nextContinuation = items[len(items)-1].Key
		}
		objects = items
	}
	type content struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	}
	response := struct {
		XMLName               xml.Name  `xml:"ListBucketResult"`
		Xmlns                 string    `xml:"xmlns,attr"`
		Name                  string    `xml:"Name"`
		Prefix                string    `xml:"Prefix,omitempty"`
		KeyCount              int       `xml:"KeyCount"`
		MaxKeys               int       `xml:"MaxKeys"`
		IsTruncated           bool      `xml:"IsTruncated"`
		Contents              []content `xml:"Contents"`
		ContinuationToken     string    `xml:"ContinuationToken,omitempty"`
		NextContinuationToken string    `xml:"NextContinuationToken,omitempty"`
		StartAfter            string    `xml:"StartAfter,omitempty"`
	}{Xmlns: xmlNamespace, Name: bucket, Prefix: prefix, KeyCount: len(objects), MaxKeys: maxKeys, IsTruncated: isTruncated}
	for _, item := range objects {
		response.Contents = append(response.Contents, content{
			Key:          item.Key,
			LastModified: item.ModifiedAt.UTC().Format(time.RFC3339),
			ETag:         `"` + item.ETag + `"`,
			Size:         item.Size,
			StorageClass: "STANDARD",
		})
	}
	if continuation != "" {
		response.ContinuationToken = continuation
	}
	if startAfter != "" && continuation == "" {
		response.StartAfter = startAfter
	}
	if nextContinuation != "" {
		response.NextContinuationToken = nextContinuation
	}
	writeXML(w, http.StatusOK, response)
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

func hasUploadIDQuery(r *http.Request) bool {
	return r.URL.Query().Get("uploadId") != ""
}

func setObjectHeaders(w http.ResponseWriter, meta core.ObjectMetadata) {
	if meta.ETag != "" {
		w.Header().Set("ETag", "\""+meta.ETag+"\"")
	}
	if !meta.ModifiedAt.IsZero() {
		w.Header().Set("Last-Modified", meta.ModifiedAt.UTC().Format(http.TimeFormat))
	}
}

func setObjectHeadersWithLength(w http.ResponseWriter, meta core.ObjectMetadata) {
	setObjectHeaders(w, meta)
	if meta.Size >= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	}
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
	if err == core.ErrConflict {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	http.Error(w, err.Error(), httpx.StatusCode(err))
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
