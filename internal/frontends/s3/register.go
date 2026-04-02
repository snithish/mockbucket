package s3

import (
	"context"
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
)

const xmlNamespace = "http://s3.amazonaws.com/doc/2006-03-01/"

func Register(mux *http.ServeMux, cfg config.Config, deps common.Dependencies) {
	bucketHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := r.PathValue("bucket")
		switch {
		case r.Method == http.MethodPut:
			handleCreateBucket(w, r, deps, bucket)
		case r.Method == http.MethodPost && hasDeleteQuery(r):
			handleDeleteObjects(w, r, deps, bucket)
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
		case key == "" && r.Method == http.MethodPost && hasDeleteQuery(r):
			handleDeleteObjects(w, r, deps, bucket)
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
	response := listBucketsResult{Xmlns: xmlNamespace}
	response.Owner.ID = "mockbucket"
	response.Owner.DisplayName = "mockbucket"
	for _, item := range buckets {
		response.Buckets.Items = append(response.Buckets.Items, newS3Bucket(item))
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
		writeBucketError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func handleGetBucketLocation(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeBucketError(w, err)
		return
	}
	writeXML(w, http.StatusOK, newLocationConstraintResponse())
}

func handlePutObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeBucketError(w, err)
		return
	}
	if src := r.Header.Get("X-Amz-Copy-Source"); src != "" {
		handleCopyObject(w, r, deps, bucket, key, src)
		return
	}
	body, err := decodeStreamingBody(r)
	if err != nil {
		writeError(w, err)
		return
	}
	defer body.Close()
	meta, err := common.StoreObject(r.Context(), deps, bucket, key, body)
	if err != nil {
		writeError(w, err)
		return
	}
	requestMeta := objectMetadataFromRequest(r)
	meta.ContentType = requestMeta.ContentType
	meta.CacheControl = requestMeta.CacheControl
	meta.ContentDisposition = requestMeta.ContentDisposition
	meta.ContentEncoding = requestMeta.ContentEncoding
	meta.ContentLanguage = requestMeta.ContentLanguage
	meta.CustomMetadata = requestMeta.CustomMetadata
	if err := common.CommitObject(r.Context(), deps, meta); err != nil {
		writeError(w, err)
		return
	}
	setObjectHeaders(w, meta)
	w.WriteHeader(http.StatusOK)
}

func handleGetObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	meta, reader, ok := openObjectForRead(w, r, deps, bucket, key)
	if !ok {
		return
	}
	defer reader.Close()
	setObjectHeaders(w, meta)
	if meta.ContentType == "" {
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" || meta.Size <= 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, reader)
		return
	}

	start, end, err := parseRange(rangeHeader, meta.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", meta.Size))
		writeS3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange", "The requested range is not satisfiable.")
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
	meta, reader, ok := openObjectForRead(w, r, deps, bucket, key)
	if !ok {
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
		writeBucketError(w, err)
		return
	}
	if err := common.DeleteObjectIfExists(r.Context(), deps, bucket, key); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleCopyObject(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key, copySource string) {
	srcBucket, srcKey, err := parseCopySource(copySource)
	if err != nil {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), srcBucket); err != nil {
		writeBucketError(w, err)
		return
	}
	srcMeta, err := deps.Metadata.GetObject(r.Context(), srcBucket, srcKey)
	if err != nil {
		writeError(w, err)
		return
	}
	reader, _, err := deps.Objects.OpenObject(r.Context(), srcBucket, srcKey)
	if err != nil {
		writeError(w, err)
		return
	}
	defer reader.Close()

	meta, err := common.StoreObject(r.Context(), deps, bucket, key, reader)
	if err != nil {
		writeError(w, err)
		return
	}
	if strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Amz-Metadata-Directive")), "REPLACE") {
		requestMeta := objectMetadataFromRequest(r)
		meta.ContentType = requestMeta.ContentType
		meta.CacheControl = requestMeta.CacheControl
		meta.ContentDisposition = requestMeta.ContentDisposition
		meta.ContentEncoding = requestMeta.ContentEncoding
		meta.ContentLanguage = requestMeta.ContentLanguage
		meta.CustomMetadata = requestMeta.CustomMetadata
	} else {
		preserved := copyObjectMetadata(srcMeta)
		meta.ContentType = preserved.ContentType
		meta.CacheControl = preserved.CacheControl
		meta.ContentDisposition = preserved.ContentDisposition
		meta.ContentEncoding = preserved.ContentEncoding
		meta.ContentLanguage = preserved.ContentLanguage
		meta.CustomMetadata = preserved.CustomMetadata
	}
	if err := common.CommitObject(r.Context(), deps, meta); err != nil {
		writeError(w, err)
		return
	}
	setObjectHeaders(w, meta)
	writeXML(w, http.StatusOK, newCopyObjectResult(meta))
}

func handleDeleteObjects(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeBucketError(w, err)
		return
	}
	var payload struct {
		XMLName xml.Name `xml:"Delete"`
		Quiet   bool     `xml:"Quiet"`
		Objects []struct {
			Key string `xml:"Key"`
		} `xml:"Object"`
	}
	if err := xml.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	if payload.XMLName.Local != "Delete" {
		writeError(w, core.ErrInvalidArgument)
		return
	}

	type deleted struct {
		Key string `xml:"Key"`
	}
	type deleteError struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	response := struct {
		XMLName xml.Name      `xml:"DeleteResult"`
		Xmlns   string        `xml:"xmlns,attr"`
		Deleted []deleted     `xml:"Deleted,omitempty"`
		Errors  []deleteError `xml:"Error,omitempty"`
	}{Xmlns: xmlNamespace}

	for _, item := range payload.Objects {
		key := strings.TrimSpace(item.Key)
		if key == "" {
			response.Errors = append(response.Errors, deleteError{
				Code:    "InvalidArgument",
				Message: "Invalid argument.",
			})
			continue
		}
		if err := common.DeleteObjectIfExists(r.Context(), deps, bucket, key); err != nil {
			response.Errors = append(response.Errors, deleteError{
				Key:     key,
				Code:    "InternalError",
				Message: "We encountered an internal error. Please try again.",
			})
			continue
		}
		if !payload.Quiet {
			response.Deleted = append(response.Deleted, deleted{Key: key})
		}
	}

	writeXML(w, http.StatusOK, response)
}

func handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeBucketError(w, err)
		return
	}
	uploadID, err := newUploadID()
	if err != nil {
		writeError(w, err)
		return
	}
	upload := core.MultipartUpload{
		UploadID:           uploadID,
		Bucket:             bucket,
		Key:                key,
		ContentType:        strings.TrimSpace(r.Header.Get("Content-Type")),
		CacheControl:       strings.TrimSpace(r.Header.Get("Cache-Control")),
		ContentDisposition: strings.TrimSpace(r.Header.Get("Content-Disposition")),
		ContentEncoding:    sanitizeS3ContentEncoding(r.Header.Values("Content-Encoding")),
		ContentLanguage:    strings.TrimSpace(r.Header.Get("Content-Language")),
		CustomMetadata:     extractPrefixedHeaders(r.Header, "x-amz-meta-"),
		CreatedAt:          time.Now().UTC(),
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
		writeBucketError(w, err)
		return
	}
	partNumber, err := parsePartNumber(r)
	if err != nil {
		writeError(w, err)
		return
	}
	upload, ok := loadMultipartUpload(w, r, deps, bucket, key)
	if !ok {
		return
	}
	body, err := decodeStreamingBody(r)
	if err != nil {
		writeError(w, err)
		return
	}
	defer body.Close()
	part, err := deps.Objects.PutMultipartPart(r.Context(), upload.UploadID, partNumber, body)
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
		writeBucketError(w, err)
		return
	}
	upload, ok := loadMultipartUpload(w, r, deps, bucket, key)
	if !ok {
		return
	}
	payload, ok := decodeCompleteMultipartUpload(w, r)
	if !ok {
		return
	}
	ordered, ok := resolveMultipartParts(w, r, deps, upload.UploadID, payload)
	if !ok {
		return
	}
	meta, err := deps.Objects.CompleteMultipartUpload(r.Context(), bucket, key, ordered)
	if err != nil {
		writeError(w, err)
		return
	}
	meta.ContentType = upload.ContentType
	meta.CacheControl = upload.CacheControl
	meta.ContentDisposition = upload.ContentDisposition
	meta.ContentEncoding = upload.ContentEncoding
	meta.ContentLanguage = upload.ContentLanguage
	meta.CustomMetadata = cloneCustomMetadata(upload.CustomMetadata)
	if err := common.CommitObject(r.Context(), deps, meta); err != nil {
		writeError(w, err)
		return
	}
	if err := finalizeMultipartUpload(r.Context(), deps, upload.UploadID, bucket, key); err != nil {
		writeError(w, err)
		return
	}
	writeXML(w, http.StatusOK, newCompleteMultipartUploadResult(bucket, key, meta))
}

func handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket, key string) {
	if strings.TrimSpace(key) == "" {
		http.NotFound(w, r)
		return
	}
	upload, ok := loadMultipartUpload(w, r, deps, bucket, key)
	if !ok {
		return
	}
	if err := deps.Metadata.DeleteMultipartUpload(r.Context(), upload.UploadID); err != nil {
		writeError(w, err)
		return
	}
	_ = deps.Objects.AbortMultipartUpload(r.Context(), upload.UploadID)
	w.WriteHeader(http.StatusNoContent)
}

func handleListObjectsV2(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeBucketError(w, err)
		return
	}
	maxKeys, err := parseMaxKeys(r)
	if err != nil {
		writeError(w, err)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
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
	commonPrefixes := []string{}
	isTruncated := false
	nextContinuation := ""
	if maxKeys != 0 {
		if delimiter == "" {
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
		} else {
			contents, prefixes, truncated, nextToken, err := listObjectsV2WithDelimiter(r.Context(), deps, bucket, prefix, delimiter, maxKeys, after)
			if err != nil {
				writeError(w, err)
				return
			}
			objects = contents
			commonPrefixes = prefixes
			isTruncated = truncated
			nextContinuation = nextToken
		}
	}
	type content struct {
		Key          string `xml:"Key"`
		LastModified string `xml:"LastModified"`
		ETag         string `xml:"ETag"`
		Size         int64  `xml:"Size"`
		StorageClass string `xml:"StorageClass"`
	}
	type commonPrefix struct {
		Prefix string `xml:"Prefix"`
	}
	response := struct {
		XMLName               xml.Name       `xml:"ListBucketResult"`
		Xmlns                 string         `xml:"xmlns,attr"`
		Name                  string         `xml:"Name"`
		Prefix                string         `xml:"Prefix,omitempty"`
		Delimiter             string         `xml:"Delimiter,omitempty"`
		KeyCount              int            `xml:"KeyCount"`
		MaxKeys               int            `xml:"MaxKeys"`
		IsTruncated           bool           `xml:"IsTruncated"`
		Contents              []content      `xml:"Contents"`
		CommonPrefixes        []commonPrefix `xml:"CommonPrefixes,omitempty"`
		ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
		NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
		StartAfter            string         `xml:"StartAfter,omitempty"`
	}{Xmlns: xmlNamespace, Name: bucket, Prefix: prefix, Delimiter: delimiter, KeyCount: len(objects) + len(commonPrefixes), MaxKeys: maxKeys, IsTruncated: isTruncated}
	for _, item := range objects {
		response.Contents = append(response.Contents, content{
			Key:          item.Key,
			LastModified: item.ModifiedAt.UTC().Format(time.RFC3339),
			ETag:         `"` + item.ETag + `"`,
			Size:         item.Size,
			StorageClass: "STANDARD",
		})
	}
	for _, item := range commonPrefixes {
		response.CommonPrefixes = append(response.CommonPrefixes, commonPrefix{Prefix: item})
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

func listObjectsV2WithDelimiter(ctx context.Context, deps common.Dependencies, bucket, prefix, delimiter string, maxKeys int, after string) ([]core.ObjectMetadata, []string, bool, string, error) {
	if delimiter == "" {
		return nil, nil, false, "", nil
	}

	type listEntry struct {
		token  string
		object *core.ObjectMetadata
		prefix string
	}

	var (
		entries   []listEntry
		seen      = map[string]struct{}{}
		cursor    = after
		truncated bool
	)

	for {
		items, err := deps.Metadata.ListObjects(ctx, bucket, prefix, 1000, cursor)
		if err != nil {
			return nil, nil, false, "", err
		}
		if len(items) == 0 {
			break
		}
		for _, item := range items {
			token, object, commonPrefix := collapseListItem(item, prefix, delimiter)
			if token == "" {
				continue
			}
			if _, ok := seen[token]; ok {
				cursor = item.Key
				continue
			}
			seen[token] = struct{}{}
			entry := listEntry{token: token, prefix: commonPrefix}
			if object != nil {
				copy := *object
				entry.object = &copy
			}
			entries = append(entries, entry)
			cursor = item.Key
			if len(entries) > maxKeys {
				truncated = true
				break
			}
		}
		if truncated || len(items) < 1000 {
			break
		}
	}

	if truncated {
		entries = entries[:maxKeys]
	}
	nextToken := ""
	if truncated && len(entries) > 0 {
		nextToken = entries[len(entries)-1].token
	}

	objects := make([]core.ObjectMetadata, 0, len(entries))
	commonPrefixes := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.object != nil {
			objects = append(objects, *entry.object)
			continue
		}
		commonPrefixes = append(commonPrefixes, entry.prefix)
	}
	return objects, commonPrefixes, truncated, nextToken, nil
}

func collapseListItem(item core.ObjectMetadata, prefix, delimiter string) (string, *core.ObjectMetadata, string) {
	rest := strings.TrimPrefix(item.Key, prefix)
	if rest == "" {
		return item.Key, &item, ""
	}
	idx := strings.Index(rest, delimiter)
	if idx < 0 {
		return item.Key, &item, ""
	}
	commonPrefix := prefix + rest[:idx+len(delimiter)]
	return commonPrefix, nil, commonPrefix
}
