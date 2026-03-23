package s3

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"time"

	authaws "github.com/snithish/mockbucket/internal/auth/aws"
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
	bucketHandler := authaws.Authenticate("s3", deps.AWSVerifier, deps.AuthResolver, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket := r.PathValue("bucket")
		switch {
		case r.Method == http.MethodPut:
			handleCreateBucket(w, r, deps, bucket)
		case r.Method == http.MethodHead:
			handleHeadBucket(w, r, deps, bucket)
		case r.Method == http.MethodGet && hasLocationQuery(r):
			handleGetBucketLocation(w, r, deps, bucket)
		default:
			http.NotFound(w, r)
		}
	}))
	mux.Handle("/{bucket}", bucketHandler)
}

func RootHandler(_ config.Config, deps common.Dependencies) http.Handler {
	return authaws.Authenticate("s3", deps.AWSVerifier, deps.AuthResolver, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleListBuckets(w, r, deps)
	}))
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
	resource := bucketResource(bucket)
	if !allow(r, deps, "s3:CreateBucket", resource) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if err := deps.Metadata.CreateBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func handleHeadBucket(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	resource := bucketResource(bucket)
	if !allow(r, deps, "s3:ListBucket", resource) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	if _, err := deps.Metadata.GetBucket(r.Context(), bucket); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func handleGetBucketLocation(w http.ResponseWriter, r *http.Request, deps common.Dependencies, bucket string) {
	resource := bucketResource(bucket)
	if !allow(r, deps, "s3:GetBucketLocation", resource) {
		writeError(w, core.ErrAccessDenied)
		return
	}
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

func allow(r *http.Request, deps common.Dependencies, action, resource string) bool {
	subject, ok := httpx.SubjectFromContext(r.Context())
	if !ok {
		return false
	}
	return deps.Policy.Allowed(action, resource, subject.Policies)
}

func hasLocationQuery(r *http.Request) bool {
	_, ok := r.URL.Query()["location"]
	return ok
}

func bucketResource(bucket string) string {
	return fmt.Sprintf("arn:mockbucket:s3:::%s", bucket)
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
