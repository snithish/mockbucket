package azure_datalake

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	azauth "github.com/snithish/mockbucket/internal/auth/azure"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
)

func registerDFSHandlers(mux *http.ServeMux, deps common.Dependencies, resolver azauth.Authenticator) {
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-ms-version", "2021-06-08")

		resource := r.URL.Query().Get("resource")
		restype := r.URL.Query().Get("restype")
		comp := r.URL.Query().Get("comp")
		action := r.URL.Query().Get("action")

		pathParts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
		fs := pathParts[0]
		var filePath string
		if len(pathParts) > 1 {
			filePath = pathParts[1]
		}

		switch {
		// Blob-style operations (used by Go DataLake SDK)
		case r.Method == http.MethodGet && comp == "list":
			handleListFilesystemsXML(w, r, deps)
		case r.Method == http.MethodPut && restype == "container":
			handleCreateFilesystemXML(w, r, deps, fs)
		case r.Method == http.MethodHead && restype == "container":
			handleGetFilesystemProperties(w, r, deps, fs)
		case r.Method == http.MethodGet && restype == "container":
			handleGetFilesystemProperties(w, r, deps, fs)

		// DFS-style operations
		case r.Method == http.MethodGet && resource == "account":
			handleListFilesystems(w, r, deps)
		case r.Method == http.MethodPut && resource == "filesystem":
			handleCreateFilesystem(w, r, deps, fs)

		// Delete filesystem
		case r.Method == http.MethodDelete && fs != "" && filePath == "":
			handleDeleteFilesystem(w, r, deps, fs)

		// DFS file operations
		case r.Method == http.MethodGet && fs != "" && filePath == "" && resource != "account":
			// List paths (DFS)
			handleListPath(w, r, deps, fs, "")
		case r.Method == http.MethodPut && resource == "file":
			handleCreateFile(w, r, deps, fs, filePath)
		case r.Method == http.MethodPut && resource == "directory":
			handleCreateDirectory(w, r, deps, fs, filePath)
		case r.Method == http.MethodPatch && action == "append":
			handleAppendData(w, r, deps, fs, filePath)
		case r.Method == http.MethodPatch && action == "flush":
			handleFlushData(w, r, deps, fs, filePath)
		case r.Method == http.MethodGet && filePath != "":
			handleReadFile(w, r, deps, fs, filePath)
		case r.Method == http.MethodHead && filePath != "":
			handleGetPathProperties(w, r, deps, fs, filePath)
		case r.Method == http.MethodDelete && filePath != "":
			handleDeletePath(w, r, deps, fs, filePath)
		default:
			if fs != "" && filePath != "" {
				handleDfsPath(w, r, deps, fs, filePath)
				return
			}
			writeDFSError(w, http.StatusNotImplemented, "UnsupportedOperation", "The specified operation is not implemented.")
		}
	})
}

type ListFilesystemsXML struct {
	XMLName   xml.Name        `xml:"EnumerationResults"`
	Endpoint  string          `xml:"ServiceEndpoint,attr"`
	Container []XMLFilesystem `xml:"FileSystems>FileSystem"`
}

type XMLFilesystem struct {
	Name         string `xml:"Name"`
	LastModified string `xml:"Properties>Last-Modified"`
	ETag         string `xml:"Properties>Etag"`
}

func handleListFilesystemsXML(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	containers, err := deps.Metadata.ListBuckets(r.Context())
	if err != nil {
		writeBlobError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Build XML response in the format expected by the SDK
	// The DataLake SDK reuses Blob API which expects Containers/Container tags
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="utf-8"?>`)
	sb.WriteString(`<EnumerationResults ServiceEndpoint="http://localhost:9000/"><Containers>`)
	for _, c := range containers {
		sb.WriteString(`<Container><Name>`)
		sb.WriteString(c.Name)
		sb.WriteString(`</Name><Properties><Last-Modified>`)
		sb.WriteString(c.CreatedAt.Format(time.RFC1123))
		sb.WriteString(`</Last-Modified><Etag>`)
		sb.WriteString(fmt.Sprintf(`"%d"`, c.CreatedAt.UnixNano()))
		sb.WriteString(`</Etag></Properties></Container>`)
	}
	sb.WriteString(`</Containers></EnumerationResults>`)

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(sb.String()))
}

func handleCreateFilesystemXML(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs string) {
	if err := deps.Metadata.CreateBucket(r.Context(), fs); err != nil {
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

func writeBlobError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	resp := struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}{Code: code, Message: message}
	_ = xml.NewEncoder(w).Encode(resp)
}

type FilesystemList struct {
	Filesystems []Filesystem `json:"filesystems"`
}

type Filesystem struct {
	Name       string     `json:"name"`
	Properties Properties `json:"properties"`
}

type Properties struct {
	ETag         string `json:"etag"`
	LastModified string `json:"lastModified"`
}

func handleListFilesystems(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	containers, err := deps.Metadata.ListBuckets(r.Context())
	if err != nil {
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	result := FilesystemList{
		Filesystems: make([]Filesystem, 0, len(containers)),
	}

	for _, c := range containers {
		result.Filesystems = append(result.Filesystems, Filesystem{
			Name: c.Name,
			Properties: Properties{
				ETag:         fmt.Sprintf(`"%d"`, c.CreatedAt.UnixNano()),
				LastModified: c.CreatedAt.Format(time.RFC3339),
			},
		})
	}

	writeJSON(w, http.StatusOK, result)
}

func handleCreateFilesystem(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs string) {
	if err := deps.Metadata.CreateBucket(r.Context(), fs); err != nil {
		if err == core.ErrConflict {
			writeDFSError(w, http.StatusConflict, "FilesystemAlreadyExists", "The specified filesystem already exists.")
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, time.Now().UnixNano()))
	w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))
	w.Header().Set("x-ms-request-id", azauth.GetAccountFromContext(r.Context()))
	w.WriteHeader(http.StatusCreated)
}

func handleDeleteFilesystem(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs string) {
	_, err := deps.Metadata.GetBucket(r.Context(), fs)
	if err != nil {
		if err == core.ErrNotFound {
			writeDFSError(w, http.StatusNotFound, "FilesystemNotFound", "The specified filesystem does not exist.")
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	_ = deps.Metadata.DeleteObject(r.Context(), fs, "")
	w.WriteHeader(http.StatusAccepted)
}

func handleGetFilesystemProperties(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs string) {
	_, err := deps.Metadata.GetBucket(r.Context(), fs)
	if err != nil {
		if err == core.ErrNotFound {
			writeDFSError(w, http.StatusNotFound, "FilesystemNotFound", "The specified filesystem does not exist.")
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%d"`, time.Now().UnixNano()))
	w.Header().Set("Last-Modified", time.Now().Format(time.RFC1123))
	w.WriteHeader(http.StatusOK)
}

func handleDfsPath(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs, filePath string) {
	resource := r.URL.Query().Get("resource")
	action := r.URL.Query().Get("action")

	switch {
	case r.Method == http.MethodPut && resource == "file":
		handleCreateFile(w, r, deps, fs, filePath)
	case r.Method == http.MethodPut && resource == "directory":
		handleCreateDirectory(w, r, deps, fs, filePath)
	case r.Method == http.MethodPatch && action == "append":
		handleAppendData(w, r, deps, fs, filePath)
	case r.Method == http.MethodPatch && action == "flush":
		handleFlushData(w, r, deps, fs, filePath)
	case r.Method == http.MethodGet:
		handleReadFile(w, r, deps, fs, filePath)
	case r.Method == http.MethodHead:
		handleGetPathProperties(w, r, deps, fs, filePath)
	case r.Method == http.MethodDelete:
		handleDeletePath(w, r, deps, fs, filePath)
	case r.Method == http.MethodGet && r.URL.Query().Get("recursive") != "":
		handleListPath(w, r, deps, fs, filePath)
	default:
		writeDFSError(w, http.StatusNotImplemented, "UnsupportedOperation", "The specified operation is not implemented.")
	}
}

func handleCreateFile(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs, filePath string) {
	_, err := deps.Metadata.GetBucket(r.Context(), fs)
	if err != nil {
		if err == core.ErrNotFound {
			writeDFSError(w, http.StatusNotFound, "FilesystemNotFound", "The specified filesystem does not exist.")
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	meta, err := deps.Objects.PutObject(r.Context(), fs, filePath, r.Body)
	if err != nil {
		_ = deps.Objects.DeleteObject(r.Context(), fs, filePath)
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	if err := deps.Metadata.PutObject(r.Context(), meta); err != nil {
		_ = deps.Objects.DeleteObject(r.Context(), fs, filePath)
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, meta.ETag))
	w.Header().Set("Last-Modified", meta.ModifiedAt.Format(time.RFC1123))
	w.Header().Set("x-ms-request-id", azauth.GetAccountFromContext(r.Context()))
	w.WriteHeader(http.StatusCreated)
}

func handleCreateDirectory(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs, dirPath string) {
	_, err := deps.Metadata.GetBucket(r.Context(), fs)
	if err != nil {
		if err == core.ErrNotFound {
			writeDFSError(w, http.StatusNotFound, "FilesystemNotFound", "The specified filesystem does not exist.")
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	if dirPath != "" && !strings.HasSuffix(dirPath, "/") {
		dirPath += "/"
	}

	meta, err := deps.Objects.PutObject(r.Context(), fs, dirPath, strings.NewReader(""))
	if err != nil {
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	if err := deps.Metadata.PutObject(r.Context(), meta); err != nil {
		_ = deps.Objects.DeleteObject(r.Context(), fs, dirPath)
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, meta.ETag))
	w.Header().Set("Last-Modified", meta.ModifiedAt.Format(time.RFC1123))
	w.WriteHeader(http.StatusCreated)
}

func handleAppendData(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs, filePath string) {
	// Read the appended data from the request body
	// and store it immediately for simplicity
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// Get existing data if any
	existing, _, err := deps.Objects.OpenObject(r.Context(), fs, filePath)
	var combined []byte
	if err == nil {
		existingData, err := io.ReadAll(existing)
		existing.Close()
		if err != nil {
			writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
		combined = append(existingData, data...)
	} else {
		combined = data
	}

	// Write the combined data
	meta, err := deps.Objects.PutObject(r.Context(), fs, filePath, strings.NewReader(string(combined)))
	if err != nil {
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	if err := deps.Metadata.PutObject(r.Context(), meta); err != nil {
		_ = deps.Objects.DeleteObject(r.Context(), fs, filePath)
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, meta.ETag))
	w.Header().Set("Last-Modified", meta.ModifiedAt.Format(time.RFC1123))
	w.Header().Set("AppendOffset", fmt.Sprintf("%d", int64(len(combined))-int64(len(data))))
	w.WriteHeader(http.StatusAccepted)
}

func handleFlushData(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs, filePath string) {
	_, err := deps.Metadata.GetBucket(r.Context(), fs)
	if err != nil {
		if err == core.ErrNotFound {
			writeDFSError(w, http.StatusNotFound, "FilesystemNotFound", "The specified filesystem does not exist.")
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	// For flush, we just update the metadata if needed
	// The data was already written by append
	obj, err := deps.Metadata.GetObject(r.Context(), fs, filePath)
	if err != nil {
		if err == core.ErrNotFound {
			// File doesn't exist yet, create empty
			meta, err := deps.Objects.PutObject(r.Context(), fs, filePath, strings.NewReader(""))
			if err != nil {
				writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
				return
			}
			if err := deps.Metadata.PutObject(r.Context(), meta); err != nil {
				writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
				return
			}
			w.Header().Set("ETag", fmt.Sprintf(`"%s"`, meta.ETag))
			w.Header().Set("Last-Modified", meta.ModifiedAt.Format(time.RFC1123))
			w.WriteHeader(http.StatusOK)
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.Header().Set("Last-Modified", obj.ModifiedAt.Format(time.RFC1123))
	w.WriteHeader(http.StatusOK)
}

func handleReadFile(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs, filePath string) {
	obj, err := deps.Metadata.GetObject(r.Context(), fs, filePath)
	if err != nil {
		if err == core.ErrNotFound {
			writeDFSError(w, http.StatusNotFound, "PathNotFound", "The specified path does not exist.")
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	data, _, err := deps.Objects.OpenObject(r.Context(), fs, filePath)
	if err != nil {
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}
	defer data.Close()

	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.Header().Set("Last-Modified", obj.ModifiedAt.Format(time.RFC1123))
	w.Header().Set("x-ms-version", "2021-06-08")
	w.WriteHeader(http.StatusOK)
	io.Copy(w, data)
}

func handleGetPathProperties(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs, filePath string) {
	obj, err := deps.Metadata.GetObject(r.Context(), fs, filePath)
	if err != nil {
		if err == core.ErrNotFound {
			writeDFSError(w, http.StatusNotFound, "PathNotFound", "The specified path does not exist.")
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", obj.Size))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, obj.ETag))
	w.Header().Set("Last-Modified", obj.ModifiedAt.Format(time.RFC1123))
	w.Header().Set("x-ms-resource-type", "file")
	w.Header().Set("x-ms-version", "2021-06-08")
	w.WriteHeader(http.StatusOK)
}

func handleDeletePath(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs, filePath string) {
	if err := deps.Metadata.DeleteObject(r.Context(), fs, filePath); err != nil {
		if err == core.ErrNotFound {
			writeDFSError(w, http.StatusNotFound, "PathNotFound", "The specified path does not exist.")
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	_ = deps.Objects.DeleteObject(r.Context(), fs, filePath)
	w.WriteHeader(http.StatusAccepted)
}

type PathList struct {
	Directories  []PathItem `json:"directories"`
	Files        []PathItem `json:"files"`
	Continuation string     `json:"continuationToken,omitempty"`
}

type PathItem struct {
	Name          string `json:"name"`
	IsDirectory   bool   `json:"isDirectory"`
	ETag          string `json:"etag"`
	LastModified  string `json:"lastModified"`
	ContentLength int64  `json:"contentLength"`
}

func handleListPath(w http.ResponseWriter, r *http.Request, deps common.Dependencies, fs, dirPath string) {
	_, err := deps.Metadata.GetBucket(r.Context(), fs)
	if err != nil {
		if err == core.ErrNotFound {
			writeDFSError(w, http.StatusNotFound, "FilesystemNotFound", "The specified filesystem does not exist.")
			return
		}
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	prefix := dirPath
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	objs, err := deps.Metadata.ListObjects(r.Context(), fs, prefix, 1000, "")
	if err != nil {
		writeDFSError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	result := PathList{
		Directories: make([]PathItem, 0),
		Files:       make([]PathItem, 0),
	}

	for _, obj := range objs {
		item := PathItem{
			Name:          obj.Key,
			IsDirectory:   strings.HasSuffix(obj.Key, "/"),
			ETag:          fmt.Sprintf(`"%s"`, obj.ETag),
			LastModified:  obj.ModifiedAt.Format(time.RFC3339),
			ContentLength: obj.Size,
		}

		if item.IsDirectory {
			result.Directories = append(result.Directories, item)
		} else {
			result.Files = append(result.Files, item)
		}
	}

	writeJSON(w, http.StatusOK, result)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

type DFSError struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

func writeDFSError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-ms-error-code", code)
	w.WriteHeader(status)
	resp := DFSError{Error: message, Code: code}
	_ = json.NewEncoder(w).Encode(resp)
}

func writeDFSDatalakeError(w http.ResponseWriter, status int, code, message string) {
	writeDFSError(w, status, code, message)
}
