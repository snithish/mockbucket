package azure_blob

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	azauth "github.com/snithish/mockbucket/internal/auth/azure"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/azure_shared"
	"github.com/snithish/mockbucket/internal/frontends/common"
)

func registerBlobHandlers(mux *http.ServeMux, deps common.Dependencies) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		azure_shared.SetVersionHeader(w)

		if r.URL.Path == "/" && r.Method == http.MethodGet {
			handleListContainers(w, r, deps)
			return
		}

		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		container := parts[0]
		var blobPath string
		if len(parts) > 1 {
			blobPath = parts[1]
		}

		if container == "" {
			handleListContainers(w, r, deps)
			return
		}

		comp := r.URL.Query().Get("comp")
		restype := r.URL.Query().Get("restype")

		if blobPath == "" {
			switch {
			case r.Method == http.MethodHead && (restype == "container" || restype == ""):
				handleGetContainerProperties(w, r, deps, container)
			case r.Method == http.MethodPut && restype == "container":
				handleCreateContainer(w, r, deps, container)
			case r.Method == http.MethodDelete && restype == "container":
				handleDeleteContainer(w, r, deps, container)
			case r.Method == http.MethodGet && comp == "list":
				handleListBlobs(w, r, deps, container)
			case r.Method == http.MethodGet && restype == "container":
				handleListBlobs(w, r, deps, container)
			default:
				writeBlobError(w, http.StatusNotImplemented, "UnsupportedOperation", "The specified operation is not implemented.")
			}
			return
		}

		switch {
		case r.Method == http.MethodPut && restype == "" && comp == "":
			handlePutBlob(w, r, deps, container, blobPath)
		case r.Method == http.MethodGet && restype == "" && comp == "":
			handleGetBlob(w, r, deps, container, blobPath)
		case r.Method == http.MethodHead && restype == "" && comp == "":
			handleGetBlobProperties(w, r, deps, container, blobPath)
		case r.Method == http.MethodDelete && restype == "" && comp == "":
			handleDeleteBlob(w, r, deps, container, blobPath)
		default:
			writeBlobError(w, http.StatusNotImplemented, "UnsupportedOperation", "The specified operation is not implemented.")
		}
	})
}

type ListContainersResponse struct {
	XMLName         xml.Name `xml:"EnumerationResults"`
	ServiceEndpoint string   `xml:"ServiceEndpoint,attr"`
	Containers      struct {
		Container []ContainerItem `xml:"Container"`
	} `xml:"Containers"`
}

type ContainerItem struct {
	Name       string `xml:"Name"`
	Properties struct {
		Created      string `xml:"CreationTime"`
		ETag         string `xml:"Etag"`
		LastModified string `xml:"Last-Modified"`
		LeaseStatus  string `xml:"LeaseStatus"`
		PublicAccess string `xml:"PublicAccess"`
	} `xml:"Properties"`
}

func handleListContainers(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	containers, err := azure_shared.ListBuckets(r.Context(), deps)
	if err != nil {
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	account := azauth.GetAccountFromContext(r.Context())
	resp := ListContainersResponse{
		ServiceEndpoint: fmt.Sprintf("http://%s/", account),
	}
	for _, c := range containers {
		resp.Containers.Container = append(resp.Containers.Container, ContainerItem{
			Name: c.Name,
			Properties: struct {
				Created      string `xml:"CreationTime"`
				ETag         string `xml:"Etag"`
				LastModified string `xml:"Last-Modified"`
				LeaseStatus  string `xml:"LeaseStatus"`
				PublicAccess string `xml:"PublicAccess"`
			}{
				Created:      c.CreatedAt.Format(time.RFC1123),
				ETag:         fmt.Sprintf(`"%d"`, c.CreatedAt.UnixNano()),
				LastModified: c.CreatedAt.Format(time.RFC1123),
				LeaseStatus:  "unlocked",
				PublicAccess: "",
			},
		})
	}

	writeXML(w, http.StatusOK, resp)
}

func handleCreateContainer(w http.ResponseWriter, r *http.Request, deps common.Dependencies, container string) {
	if err := azure_shared.CreateBucket(r.Context(), deps, container); err != nil {
		if err == core.ErrConflict {
			writeBlobError(w, http.StatusConflict, "ContainerAlreadyExists", "The specified container already exists.")
			return
		}
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, time.Now().UnixNano()))
	w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))
	w.WriteHeader(http.StatusCreated)
}

func handleGetContainerProperties(w http.ResponseWriter, r *http.Request, deps common.Dependencies, container string) {
	_, err := azure_shared.GetBucket(r.Context(), deps, container)
	if err != nil {
		if err == core.ErrNotFound {
			writeBlobError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist.")
			return
		}
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("x-ms-blob-public-access", "")
	w.Header().Set("x-ms-has-immutability-policy", "false")
	w.Header().Set("x-ms-has-legal-hold", "false")
	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, time.Now().UnixNano()))
	w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))
	w.WriteHeader(http.StatusOK)
}

func handleDeleteContainer(w http.ResponseWriter, r *http.Request, deps common.Dependencies, container string) {
	if err := azure_shared.DeleteBucket(r.Context(), deps, container); err != nil {
		if err == core.ErrNotFound {
			writeBlobError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist.")
			return
		}
		if err == core.ErrConflict {
			writeBlobError(w, http.StatusConflict, "ContainerNotEmpty", "The specified container is not empty.")
			return
		}
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

type ListBlobsResponse struct {
	XMLName         xml.Name `xml:"EnumerationResults"`
	ServiceEndpoint string   `xml:"ServiceEndpoint,attr"`
	ContainerName   string   `xml:"ContainerName,attr"`
	Prefix          string   `xml:"Prefix"`
	Marker          string   `xml:"Marker"`
	NextMarker      string   `xml:"NextMarker"`
	MaxMarker       string   `xml:"MaxResults"`
	Blobs           struct {
		Blob []BlobItem `xml:"Blob"`
	} `xml:"Blobs"`
}

type BlobItem struct {
	Name       string `xml:"Name"`
	Properties struct {
		Created       string `xml:"Creation-Time"`
		ETag          string `xml:"Etag"`
		LastModified  string `xml:"Last-Modified"`
		ContentLength string `xml:"Content-Length"`
		ContentType   string `xml:"Content-Type"`
		BlobType      string `xml:"BlobType"`
	} `xml:"Properties"`
}

func handleListBlobs(w http.ResponseWriter, r *http.Request, deps common.Dependencies, container string) {
	_, err := azure_shared.GetBucket(r.Context(), deps, container)
	if err != nil {
		if err == core.ErrNotFound {
			writeBlobError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist.")
			return
		}
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	prefix := r.URL.Query().Get("prefix")
	marker := r.URL.Query().Get("marker")
	maxresults := r.URL.Query().Get("maxresults")

	limit := 100
	if maxresults != "" {
		if n := parseMaxResults(maxresults); n > 0 {
			limit = n
		}
	}

	fetchLimit := limit + 1
	objs, err := deps.Metadata.ListObjects(r.Context(), container, prefix, fetchLimit, marker)
	if err != nil {
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	resp := ListBlobsResponse{
		ServiceEndpoint: fmt.Sprintf("http://%s/%s/", azauth.GetAccountFromContext(r.Context()), container),
		ContainerName:   container,
		Prefix:          prefix,
		Marker:          marker,
		MaxMarker:       maxresults,
	}

	userMax := 100
	if maxresults != "" {
		if n := parseMaxResults(maxresults); n > 0 {
			userMax = n
		}
	}

	objsToReturn := objs
	if len(objs) > userMax {
		objsToReturn = objs[:userMax]
	}

	resp.Blobs.Blob = make([]BlobItem, 0, len(objsToReturn))

	for _, obj := range objsToReturn {
		blobName := obj.Key
		if strings.HasPrefix(blobName, prefix) {
			resp.Blobs.Blob = append(resp.Blobs.Blob, BlobItem{
				Name: blobName,
				Properties: struct {
					Created       string `xml:"Creation-Time"`
					ETag          string `xml:"Etag"`
					LastModified  string `xml:"Last-Modified"`
					ContentLength string `xml:"Content-Length"`
					ContentType   string `xml:"Content-Type"`
					BlobType      string `xml:"BlobType"`
				}{
					Created:       obj.CreatedAt.Format(time.RFC1123),
					ETag:          fmt.Sprintf(`"%s"`, obj.ETag),
					LastModified:  obj.ModifiedAt.Format(time.RFC1123),
					ContentLength: fmt.Sprintf("%d", obj.Size),
					ContentType:   "application/octet-stream",
					BlobType:      "BlockBlob",
				},
			})
		}
	}

	returnedCount := len(resp.Blobs.Blob)
	if returnedCount > 0 && (len(objs) > userMax || marker != "") {
		lastKey := resp.Blobs.Blob[returnedCount-1].Name
		w.Header().Set("x-ms-continuation", lastKey)
		resp.NextMarker = lastKey
	}

	w.Header().Set("x-ms-version", "2021-06-08")
	writeXML(w, http.StatusOK, resp)
}

func handlePutBlob(w http.ResponseWriter, r *http.Request, deps common.Dependencies, container, blobPath string) {
	account := azauth.GetAccountFromContext(r.Context())
	containerName := container
	key := blobPath

	_, err := azure_shared.GetBucket(r.Context(), deps, containerName)
	if err != nil {
		if err == core.ErrNotFound {
			writeBlobError(w, http.StatusNotFound, "ContainerNotFound", "The specified container does not exist.")
			return
		}
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	meta, err := deps.Objects.PutObject(r.Context(), containerName, key, r.Body)
	if err != nil {
		_ = deps.Objects.DeleteObject(r.Context(), containerName, key)
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	if err := deps.Metadata.PutObject(r.Context(), meta); err != nil {
		_ = deps.Objects.DeleteObject(r.Context(), containerName, key)
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("x-ms-blob-type", "BlockBlob")
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, meta.ETag))
	w.Header().Set("Last-Modified", meta.ModifiedAt.Format(time.RFC1123))
	w.Header().Set("Content-MD5", meta.ETag)
	w.Header().Set("x-ms-request-id", account)
	w.Header().Set("x-ms-version", "2021-06-08")
	w.WriteHeader(http.StatusCreated)
}

func handleGetBlob(w http.ResponseWriter, r *http.Request, deps common.Dependencies, container, blobPath string) {
	key := blobPath

	obj, err := deps.Metadata.GetObject(r.Context(), container, key)
	if err != nil {
		if err == core.ErrNotFound {
			writeBlobError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist.")
			return
		}
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	data, _, err := deps.Objects.OpenObject(r.Context(), container, key)
	if err != nil {
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer data.Close()

	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	w.Header().Set("Content-Type", "application/octet-stream")
	if obj.Size > 0 {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", obj.Size-1, obj.Size))
	} else {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", obj.Size))
	}
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.Header().Set("Last-Modified", obj.ModifiedAt.Format(time.RFC1123))
	w.Header().Set("x-ms-version", "2021-06-08")
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, data)
}

func handleGetBlobProperties(w http.ResponseWriter, r *http.Request, deps common.Dependencies, container, blobPath string) {
	key := blobPath

	obj, err := deps.Metadata.GetObject(r.Context(), container, key)
	if err != nil {
		if err == core.ErrNotFound {
			writeBlobError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist.")
			return
		}
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.Header().Set("Last-Modified", obj.ModifiedAt.Format(time.RFC1123))
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	w.Header().Set("x-ms-access-tier", "Hot")
	w.Header().Set("x-ms-version", "2021-06-08")
	w.WriteHeader(http.StatusOK)
}

func handleDeleteBlob(w http.ResponseWriter, r *http.Request, deps common.Dependencies, container, blobPath string) {
	key := blobPath

	if err := deps.Metadata.DeleteObject(r.Context(), container, key); err != nil {
		if err == core.ErrNotFound {
			writeBlobError(w, http.StatusNotFound, "BlobNotFound", "The specified blob does not exist.")
			return
		}
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	_ = deps.Objects.DeleteObject(r.Context(), container, key)
	w.WriteHeader(http.StatusAccepted)
}

func writeXML(w http.ResponseWriter, status int, payload any) {
	azure_shared.WriteXML(w, status, payload)
}

func writeBlobError(w http.ResponseWriter, status int, code, message string) {
	azure_shared.WriteBlobError(w, status, code, message)
}

func parseMaxResults(s string) int {
	var n int
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	if n < 1 || n > 5000 {
		return 0
	}
	return n
}
