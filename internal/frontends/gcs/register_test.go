package gcs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"strings"
	"testing"
)

func TestRegisterRejectsUnauthenticatedBucketList(t *testing.T) {
	// This checks that the bucket-list endpoint rejects requests with no bearer token.
	fixture := newGCSTestFixture(t)

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b", nil)
	rec := httptest.NewRecorder()
	fixture.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestRegisterRejectsInvalidBearerToken(t *testing.T) {
	// This checks that bearer tokens must resolve to a seeded service account.
	fixture := newGCSTestFixture(t)

	req := httptest.NewRequest(http.MethodGet, "/storage/v1/b", nil)
	req.Header.Set("Authorization", "Bearer bad-token")
	rec := httptest.NewRecorder()
	fixture.mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestRegisterTokenEndpointClientCredentials(t *testing.T) {
	// This checks that client-credentials exchange returns a bearer token accepted by the bucket-list API.
	fixture := newGCSTestFixture(t)
	fixture.seedServiceAccount(t, "sa@mock.iam.gserviceaccount.com", "admin", "static-sa-token")

	form := url.Values{}
	form.Set("grant_type", "client_credentials")
	form.Set("client_id", "sa@mock.iam.gserviceaccount.com")
	req := httptest.NewRequest(http.MethodPost, "/oauth2/v4/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp := fixture.serve(req)
	if got, want := resp.Code, http.StatusOK; got != want {
		t.Fatalf("token status = %d, want %d, body = %q", got, want, resp.Body.String())
	}

	var tok struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if tok.AccessToken == "" || tok.TokenType != "Bearer" {
		t.Fatalf("unexpected token response = %+v", tok)
	}

	listReq := fixture.requestWithToken(http.MethodGet, "/storage/v1/b", tok.AccessToken, nil)
	listResp := fixture.serve(listReq)
	if got, want := listResp.Code, http.StatusOK; got != want {
		t.Fatalf("list status = %d, want %d, body = %q", got, want, listResp.Body.String())
	}
}

func TestRegisterTokenEndpointJWTBearer(t *testing.T) {
	// This checks that the JWT bearer flow accepts SDK-style assertions for a seeded service account.
	fixture := newGCSTestFixture(t)
	fixture.seedServiceAccount(t, "sa@mock.iam.gserviceaccount.com", "admin", "static-jwt-token")

	claims := `{"iss":"sa@mock.iam.gserviceaccount.com","aud":"http://example.test/oauth2/v4/token"}`
	payload := base64.RawURLEncoding.EncodeToString([]byte(claims))
	assertion := "eyJhbGciOiJSUzI1NiJ9." + payload + ".fakesig"

	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("assertion", assertion)
	req := httptest.NewRequest(http.MethodPost, "/oauth2/v4/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp := fixture.serve(req)
	if got, want := resp.Code, http.StatusOK; got != want {
		t.Fatalf("token status = %d, want %d, body = %q", got, want, resp.Body.String())
	}

	var tok struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if tok.AccessToken == "" {
		t.Fatal("access_token is empty")
	}
}

func TestRegisterTokenEndpointFailureModes(t *testing.T) {
	// This checks that unsupported token-endpoint inputs fail with the expected status codes and error text.
	fixture := newGCSTestFixture(t)
	fixture.seedServiceAccount(t, "second@mock.iam.gserviceaccount.com", "second-user", "second-token")
	claims := `{"aud":"http://example.test/oauth2/v4/token"}`
	assertionNoIssuer := "eyJhbGciOiJSUzI1NiJ9." + base64.RawURLEncoding.EncodeToString([]byte(claims)) + ".fakesig"

	tests := []struct {
		name        string
		method      string
		body        string
		contentType string
		wantStatus  int
		wantText    string
	}{
		{
			name:       "method not allowed",
			method:     http.MethodGet,
			wantStatus: http.StatusMethodNotAllowed,
			wantText:   "method not allowed",
		},
		{
			name:        "unsupported grant type",
			method:      http.MethodPost,
			body:        "grant_type=password",
			contentType: "application/x-www-form-urlencoded",
			wantStatus:  http.StatusBadRequest,
			wantText:    "unsupported grant_type",
		},
		{
			name:        "client credentials missing client id",
			method:      http.MethodPost,
			body:        "grant_type=client_credentials",
			contentType: "application/x-www-form-urlencoded",
			wantStatus:  http.StatusBadRequest,
			wantText:    "client_id is required",
		},
		{
			name:        "jwt bearer missing iss and sub",
			method:      http.MethodPost,
			body:        "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=" + url.QueryEscape(assertionNoIssuer),
			contentType: "application/x-www-form-urlencoded",
			wantStatus:  http.StatusBadRequest,
			wantText:    "invalid assertion",
		},
		{
			name:        "unknown client email with multiple service accounts",
			method:      http.MethodPost,
			body:        "grant_type=client_credentials&client_id=missing%40mock.iam.gserviceaccount.com",
			contentType: "application/x-www-form-urlencoded",
			wantStatus:  http.StatusUnauthorized,
			wantText:    "invalid_client",
		},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(tt.method, "/oauth2/v4/token", strings.NewReader(tt.body))
		if tt.contentType != "" {
			req.Header.Set("Content-Type", tt.contentType)
		}
		resp := fixture.serve(req)
		if got, want := resp.Code, tt.wantStatus; got != want {
			t.Fatalf("%s status = %d, want %d, body = %q", tt.name, got, want, resp.Body.String())
		}
		if !strings.Contains(resp.Body.String(), tt.wantText) {
			t.Fatalf("%s body = %q, want to contain %q", tt.name, resp.Body.String(), tt.wantText)
		}
	}
}

func TestRegisterTokenEndpointFallsBackToSingleServiceAccount(t *testing.T) {
	// This checks that JWT bearer exchange succeeds when exactly one service account is configured, even if the issuer email differs.
	fixture := newGCSTestFixture(t)
	if err := fixture.metadata.DeleteServiceAccounts(context.Background()); err != nil {
		t.Fatalf("DeleteServiceAccounts() error = %v", err)
	}
	fixture.seedServiceAccount(t, "sa@mock.iam.gserviceaccount.com", "admin", "static-jwt-token")

	claims := `{"iss":"unexpected@mock.iam.gserviceaccount.com","aud":"http://example.test/oauth2/v4/token"}`
	payload := base64.RawURLEncoding.EncodeToString([]byte(claims))
	assertion := "eyJhbGciOiJSUzI1NiJ9." + payload + ".fakesig"

	form := url.Values{}
	form.Set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
	form.Set("assertion", assertion)
	req := httptest.NewRequest(http.MethodPost, "/oauth2/v4/token", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp := fixture.serve(req)
	if got, want := resp.Code, http.StatusOK; got != want {
		t.Fatalf("token status = %d, want %d, body = %q", got, want, resp.Body.String())
	}
}

func TestRegisterCreateBucketConflict(t *testing.T) {
	// This checks that creating an already-existing bucket returns the GCS conflict status.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	req := fixture.authedRequest(http.MethodPost, "/storage/v1/b", strings.NewReader(`{"name":"demo"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := fixture.serve(req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestRegisterDeleteEmptyBucket(t *testing.T) {
	// This checks that an empty bucket can be deleted through the GCS bucket endpoint.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "empty-bucket")

	req := fixture.authedRequest(http.MethodDelete, "/storage/v1/b/empty-bucket", nil)
	rec := fixture.serve(req)

	if got, want := rec.Code, http.StatusNoContent; got != want {
		t.Fatalf("status = %d, want %d, body = %q", got, want, rec.Body.String())
	}
}

func TestRegisterAuthenticatedBucketAndObjectFlow(t *testing.T) {
	// This checks that bucket creation, upload, bucket lookup, and media download share the same authenticated request path.
	fixture := newGCSTestFixture(t)
	fixture.seedServiceAccount(t, "flow@mock.iam.gserviceaccount.com", "flow-user", "gcs-flow-token")

	createReq := fixture.requestWithToken(http.MethodPost, "/storage/v1/b", "gcs-flow-token", strings.NewReader(`{"name":"flow-bucket"}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := fixture.serve(createReq)
	if got, want := createRec.Code, http.StatusOK; got != want {
		t.Fatalf("create status = %d, want %d, body = %q", got, want, createRec.Body.String())
	}

	uploadReq := fixture.requestWithToken(http.MethodPost, "/upload/storage/v1/b/flow-bucket/o?uploadType=media&name=hello.txt", "gcs-flow-token", strings.NewReader("hello gcs"))
	uploadRec := fixture.serve(uploadReq)
	if got, want := uploadRec.Code, http.StatusOK; got != want {
		t.Fatalf("upload status = %d, want %d, body = %q", got, want, uploadRec.Body.String())
	}

	bucketReq := fixture.requestWithToken(http.MethodGet, "/storage/v1/b/flow-bucket", "gcs-flow-token", nil)
	bucketRec := fixture.serve(bucketReq)
	if got, want := bucketRec.Code, http.StatusOK; got != want {
		t.Fatalf("bucket status = %d, want %d, body = %q", got, want, bucketRec.Body.String())
	}

	objectReq := fixture.requestWithToken(http.MethodGet, "/storage/v1/b/flow-bucket/o/hello.txt?alt=media", "gcs-flow-token", nil)
	objectRec := fixture.serve(objectReq)
	if got, want := objectRec.Code, http.StatusOK; got != want {
		t.Fatalf("object status = %d, want %d, body = %q", got, want, objectRec.Body.String())
	}
	if got, want := objectRec.Body.String(), "hello gcs"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestRegisterGetMissingObjectReturnsNotFound(t *testing.T) {
	// This checks that missing objects return the GCS notFound JSON payload.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	req := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o/missing.txt?alt=media", nil)
	rec := fixture.serve(req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	if got := rec.Header().Get("Content-Type"); !strings.Contains(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if !strings.Contains(rec.Body.String(), `"reason":"notFound"`) {
		t.Fatalf("body = %q, want notFound reason", rec.Body.String())
	}
}

func TestRegisterUploadTypeResumableReturnsBadRequest(t *testing.T) {
	// This checks that resumable upload sessions accept probes and then finalize on a follow-up PUT.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	req := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=resumable&name=file.txt", strings.NewReader(`{"name":"file.txt"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := fixture.serve(req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	location := rec.Header().Get("Location")
	if location == "" {
		t.Fatal("Location header is empty")
	}

	// The resumable session should answer status probes before any bytes have been committed.
	statusReq := httptest.NewRequest(http.MethodPut, location, nil)
	statusReq.Header.Set("Content-Range", "bytes */1")
	statusRec := fixture.serve(statusReq)

	if statusRec.Code != http.StatusPermanentRedirect {
		t.Fatalf("status probe code = %d, want %d", statusRec.Code, http.StatusPermanentRedirect)
	}

	// A follow-up PUT without Content-Range finalizes the upload in one shot.
	putReq := httptest.NewRequest(http.MethodPut, location, strings.NewReader("payload"))
	putRec := fixture.serve(putReq)

	if putRec.Code != http.StatusOK {
		t.Fatalf("put status = %d, want %d, body = %q", putRec.Code, http.StatusOK, putRec.Body.String())
	}
}

func TestRegisterResumableUploadSupportsChunkedCompletion(t *testing.T) {
	// This checks that resumable uploads advertise intermediate ranges and publish the full object on completion.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	initReq := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=resumable&name=chunks.txt", strings.NewReader(`{"name":"chunks.txt"}`))
	initReq.Header.Set("Content-Type", "application/json")
	initRec := fixture.serve(initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d", initRec.Code, http.StatusOK)
	}
	location := initRec.Header().Get("Location")
	if location == "" {
		t.Fatal("Location header is empty")
	}

	// The first chunk should remain incomplete and advertise the committed byte range back to the client.
	firstReq := httptest.NewRequest(http.MethodPut, location, strings.NewReader("abc"))
	firstReq.Header.Set("Content-Range", "bytes 0-2/6")
	firstRec := fixture.serve(firstReq)
	if firstRec.Code != http.StatusPermanentRedirect {
		t.Fatalf("first chunk status = %d, want %d, body = %q", firstRec.Code, http.StatusPermanentRedirect, firstRec.Body.String())
	}
	if got := firstRec.Header().Get("Range"); got != "bytes=0-2" {
		t.Fatalf("first chunk Range = %q, want %q", got, "bytes=0-2")
	}

	// The final chunk should promote the resumable upload into a readable object.
	secondReq := httptest.NewRequest(http.MethodPut, location, strings.NewReader("def"))
	secondReq.Header.Set("Content-Range", "bytes 3-5/6")
	secondRec := fixture.serve(secondReq)
	if secondRec.Code != http.StatusOK {
		t.Fatalf("second chunk status = %d, want %d, body = %q", secondRec.Code, http.StatusOK, secondRec.Body.String())
	}

	getReq := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o/chunks.txt?alt=media", nil)
	getRec := fixture.serve(getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", getRec.Code, http.StatusOK)
	}
	if got := getRec.Body.String(); got != "abcdef" {
		t.Fatalf("body = %q, want %q", got, "abcdef")
	}
}

func TestRegisterResumableUploadSupportsZeroByteFinalize(t *testing.T) {
	// This checks that resumable uploads can finalize a zero-byte object without writing any payload bytes.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	initReq := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=resumable&name=_SUCCESS", strings.NewReader(`{"name":"_SUCCESS"}`))
	initReq.Header.Set("Content-Type", "application/json")
	initRec := fixture.serve(initReq)
	if initRec.Code != http.StatusOK {
		t.Fatalf("init status = %d, want %d", initRec.Code, http.StatusOK)
	}
	location := initRec.Header().Get("Location")
	if location == "" {
		t.Fatal("Location header is empty")
	}

	finalizeReq := httptest.NewRequest(http.MethodPut, location, nil)
	finalizeReq.Header.Set("Content-Range", "bytes */0")
	finalizeRec := fixture.serve(finalizeReq)
	if finalizeRec.Code != http.StatusOK {
		t.Fatalf("finalize status = %d, want %d, body = %q", finalizeRec.Code, http.StatusOK, finalizeRec.Body.String())
	}

	getReq := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o/_SUCCESS?alt=media", nil)
	getRec := fixture.serve(getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", getRec.Code, http.StatusOK)
	}
	if got := getRec.Body.String(); got != "" {
		t.Fatalf("body = %q, want empty", got)
	}
}

func TestRegisterUploadTypeUnsupportedReturnsBadRequest(t *testing.T) {
	// This checks that unknown upload types are rejected before object creation starts.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	req := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=unknown&name=file.txt", strings.NewReader("body"))
	rec := fixture.serve(req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
}

func TestRegisterRewriteObjectCopiesSource(t *testing.T) {
	// This checks that rewrite copies the source payload and reports the destination object metadata.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	putReq := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name=src.txt", strings.NewReader("payload"))
	putRec := fixture.serve(putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d, body = %q", putRec.Code, http.StatusOK, putRec.Body.String())
	}

	rewriteReq := fixture.authedRequest(http.MethodPost, "/storage/v1/b/demo/o/src.txt/rewriteTo/b/demo/o/dst.txt", nil)
	rewriteRec := fixture.serve(rewriteReq)

	if rewriteRec.Code != http.StatusOK {
		t.Fatalf("rewrite status = %d, want %d, body = %q", rewriteRec.Code, http.StatusOK, rewriteRec.Body.String())
	}
	var resp struct {
		Done     bool `json:"done"`
		Resource struct {
			Name           string `json:"name"`
			Size           string `json:"size"`
			Generation     string `json:"generation"`
			Metageneration string `json:"metageneration"`
		} `json:"resource"`
	}
	if err := json.NewDecoder(rewriteRec.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if !resp.Done {
		t.Fatal("done = false, want true")
	}
	if resp.Resource.Name != "dst.txt" {
		t.Fatalf("resource.name = %q, want %q", resp.Resource.Name, "dst.txt")
	}
	if resp.Resource.Size != "7" {
		t.Fatalf("resource.size = %q, want %q", resp.Resource.Size, "7")
	}
	if resp.Resource.Generation == "" || resp.Resource.Generation == "0" {
		t.Fatalf("resource.generation = %q, want non-zero", resp.Resource.Generation)
	}
	if resp.Resource.Metageneration != "1" {
		t.Fatalf("resource.metageneration = %q, want %q", resp.Resource.Metageneration, "1")
	}

	getReq := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o/dst.txt?alt=media", nil)
	getRec := fixture.serve(getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", getRec.Code, http.StatusOK)
	}
	if got := getRec.Body.String(); got != "payload" {
		t.Fatalf("body = %q, want %q", got, "payload")
	}
}

func TestRegisterMetadataRoundTripAndRewritePreservation(t *testing.T) {
	// This checks that multipart upload metadata survives metadata reads, media downloads, and rewrite operations.
	fixture := newGCSTestFixture(t)
	fixture.seedServiceAccount(t, "meta@mock.iam.gserviceaccount.com", "meta-user", "gcs-meta-token")
	fixture.mustCreateBucket(t, "meta-bucket")

	var payload bytes.Buffer
	writer := multipart.NewWriter(&payload)

	metaHeader := textproto.MIMEHeader{}
	metaHeader.Set("Content-Type", "application/json; charset=UTF-8")
	metaPart, err := writer.CreatePart(metaHeader)
	if err != nil {
		t.Fatalf("CreatePart(metadata) error = %v", err)
	}
	if err := json.NewEncoder(metaPart).Encode(map[string]any{
		"name":               "meta.txt",
		"contentType":        "text/plain",
		"cacheControl":       "max-age=60",
		"contentDisposition": "attachment; filename=meta.txt",
		"contentEncoding":    "gzip",
		"contentLanguage":    "en",
		"metadata": map[string]string{
			"team": "platform",
		},
	}); err != nil {
		t.Fatalf("Encode(metadata) error = %v", err)
	}

	bodyHeader := textproto.MIMEHeader{}
	bodyHeader.Set("Content-Type", "application/octet-stream")
	bodyPart, err := writer.CreatePart(bodyHeader)
	if err != nil {
		t.Fatalf("CreatePart(body) error = %v", err)
	}
	if _, err := bodyPart.Write([]byte("hello")); err != nil {
		t.Fatalf("Write(body) error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	uploadReq := fixture.requestWithToken(http.MethodPost, "/upload/storage/v1/b/meta-bucket/o?uploadType=multipart", "gcs-meta-token", &payload)
	uploadReq.Header.Set("Content-Type", "multipart/related; boundary="+writer.Boundary())
	uploadRec := fixture.serve(uploadReq)
	if got, want := uploadRec.Code, http.StatusOK; got != want {
		t.Fatalf("upload status = %d, want %d, body = %q", got, want, uploadRec.Body.String())
	}

	metaReq := fixture.requestWithToken(http.MethodGet, "/storage/v1/b/meta-bucket/o/meta.txt", "gcs-meta-token", nil)
	metaRec := fixture.serve(metaReq)
	var objectMeta struct {
		ContentType        string            `json:"contentType"`
		CacheControl       string            `json:"cacheControl"`
		ContentDisposition string            `json:"contentDisposition"`
		ContentEncoding    string            `json:"contentEncoding"`
		ContentLanguage    string            `json:"contentLanguage"`
		Metadata           map[string]string `json:"metadata"`
	}
	if err := json.NewDecoder(metaRec.Body).Decode(&objectMeta); err != nil {
		t.Fatalf("Decode(metadata) error = %v", err)
	}
	if got, want := objectMeta.ContentType, "text/plain"; got != want {
		t.Fatalf("content type = %q, want %q", got, want)
	}
	if got, want := objectMeta.Metadata["team"], "platform"; got != want {
		t.Fatalf("metadata team = %q, want %q", got, want)
	}

	mediaReq := fixture.requestWithToken(http.MethodGet, "/storage/v1/b/meta-bucket/o/meta.txt?alt=media", "gcs-meta-token", nil)
	mediaRec := fixture.serve(mediaReq)
	if got, want := mediaRec.Header().Get("Content-Type"), "text/plain"; got != want {
		t.Fatalf("media content type = %q, want %q", got, want)
	}
	if got, want := mediaRec.Header().Get("x-goog-meta-team"), "platform"; got != want {
		t.Fatalf("media custom metadata = %q, want %q", got, want)
	}

	rewriteReq := fixture.requestWithToken(http.MethodPost, "/storage/v1/b/meta-bucket/o/meta.txt/rewriteTo/b/meta-bucket/o/meta-copy.txt", "gcs-meta-token", nil)
	rewriteRec := fixture.serve(rewriteReq)
	if got, want := rewriteRec.Code, http.StatusOK; got != want {
		t.Fatalf("rewrite status = %d, want %d, body = %q", got, want, rewriteRec.Body.String())
	}

	copyReq := fixture.requestWithToken(http.MethodGet, "/storage/v1/b/meta-bucket/o/meta-copy.txt", "gcs-meta-token", nil)
	copyRec := fixture.serve(copyReq)
	var copiedMeta struct {
		ContentType string            `json:"contentType"`
		Metadata    map[string]string `json:"metadata"`
	}
	if err := json.NewDecoder(copyRec.Body).Decode(&copiedMeta); err != nil {
		t.Fatalf("Decode(copy metadata) error = %v", err)
	}
	if got, want := copiedMeta.ContentType, "text/plain"; got != want {
		t.Fatalf("copy content type = %q, want %q", got, want)
	}
	if got, want := copiedMeta.Metadata["team"], "platform"; got != want {
		t.Fatalf("copy metadata team = %q, want %q", got, want)
	}
}

func TestRegisterComposeObjectConcatenatesSources(t *testing.T) {
	// This checks that compose concatenates the source objects into the requested destination object.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	for _, item := range []struct {
		key  string
		body string
	}{
		{key: "part-1.txt", body: "hello "},
		{key: "part-2.txt", body: "world"},
	} {
		putReq := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name="+item.key, strings.NewReader(item.body))
		putRec := fixture.serve(putReq)
		if putRec.Code != http.StatusOK {
			t.Fatalf("upload %q status = %d, want %d, body = %q", item.key, putRec.Code, http.StatusOK, putRec.Body.String())
		}
	}

	composeReq := fixture.authedRequest(http.MethodPost, "/storage/v1/b/demo/o/composed.txt/compose", strings.NewReader(`{"sourceObjects":[{"name":"part-1.txt"},{"name":"part-2.txt"}]}`))
	composeReq.Header.Set("Content-Type", "application/json")
	composeRec := fixture.serve(composeReq)
	if composeRec.Code != http.StatusOK {
		t.Fatalf("compose status = %d, want %d, body = %q", composeRec.Code, http.StatusOK, composeRec.Body.String())
	}

	getReq := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o/composed.txt?alt=media", nil)
	getRec := fixture.serve(getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", getRec.Code, http.StatusOK)
	}
	if got, want := getRec.Body.String(), "hello world"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestRegisterGenerationPreconditions(t *testing.T) {
	// This checks that generation and metageneration preconditions gate reads, overwrites, and deletes.
	fixture := newGCSTestFixture(t)
	fixture.seedServiceAccount(t, "phase4@mock.iam.gserviceaccount.com", "phase4-user", "gcs-phase4-token")
	fixture.mustCreateBucket(t, "phase4-bucket")

	firstUploadReq := fixture.requestWithToken(http.MethodPost, "/upload/storage/v1/b/phase4-bucket/o?uploadType=media&name=versioned.txt&ifGenerationMatch=0", "gcs-phase4-token", strings.NewReader("v1"))
	firstUploadRec := fixture.serve(firstUploadReq)
	if got, want := firstUploadRec.Code, http.StatusOK; got != want {
		t.Fatalf("first upload status = %d, want %d, body = %q", got, want, firstUploadRec.Body.String())
	}
	var firstObject struct {
		Generation     string `json:"generation"`
		Metageneration string `json:"metageneration"`
	}
	if err := json.NewDecoder(firstUploadRec.Body).Decode(&firstObject); err != nil {
		t.Fatalf("Decode(first upload) error = %v", err)
	}
	if firstObject.Generation == "" || firstObject.Generation == "0" {
		t.Fatalf("first generation = %q, want non-zero", firstObject.Generation)
	}
	if got, want := firstObject.Metageneration, "1"; got != want {
		t.Fatalf("first metageneration = %q, want %q", got, want)
	}

	getReq := fixture.requestWithToken(http.MethodGet, "/storage/v1/b/phase4-bucket/o/versioned.txt?ifGenerationMatch="+url.QueryEscape(firstObject.Generation)+"&ifMetagenerationMatch=1", "gcs-phase4-token", nil)
	getRec := fixture.serve(getReq)
	if got, want := getRec.Code, http.StatusOK; got != want {
		t.Fatalf("get status = %d, want %d, body = %q", got, want, getRec.Body.String())
	}

	failedGetReq := fixture.requestWithToken(http.MethodGet, "/storage/v1/b/phase4-bucket/o/versioned.txt?ifGenerationMatch=999", "gcs-phase4-token", nil)
	failedGetRec := fixture.serve(failedGetReq)
	if got, want := failedGetRec.Code, http.StatusPreconditionFailed; got != want {
		t.Fatalf("failed get status = %d, want %d, body = %q", got, want, failedGetRec.Body.String())
	}

	secondUploadReq := fixture.requestWithToken(http.MethodPost, "/upload/storage/v1/b/phase4-bucket/o?uploadType=media&name=versioned.txt&ifGenerationMatch="+url.QueryEscape(firstObject.Generation), "gcs-phase4-token", strings.NewReader("v2"))
	secondUploadRec := fixture.serve(secondUploadReq)
	if got, want := secondUploadRec.Code, http.StatusOK; got != want {
		t.Fatalf("second upload status = %d, want %d, body = %q", got, want, secondUploadRec.Body.String())
	}
	var secondObject struct {
		Generation     string `json:"generation"`
		Metageneration string `json:"metageneration"`
	}
	if err := json.NewDecoder(secondUploadRec.Body).Decode(&secondObject); err != nil {
		t.Fatalf("Decode(second upload) error = %v", err)
	}
	if got, want := secondObject.Metageneration, "1"; got != want {
		t.Fatalf("second metageneration = %q, want %q", got, want)
	}
	if secondObject.Generation == firstObject.Generation {
		t.Fatalf("generation after overwrite = %q, want different from %q", secondObject.Generation, firstObject.Generation)
	}

	staleUploadReq := fixture.requestWithToken(http.MethodPost, "/upload/storage/v1/b/phase4-bucket/o?uploadType=media&name=versioned.txt&ifGenerationMatch="+url.QueryEscape(firstObject.Generation), "gcs-phase4-token", strings.NewReader("v3"))
	staleUploadRec := fixture.serve(staleUploadReq)
	if got, want := staleUploadRec.Code, http.StatusPreconditionFailed; got != want {
		t.Fatalf("stale upload status = %d, want %d, body = %q", got, want, staleUploadRec.Body.String())
	}

	deleteReq := fixture.requestWithToken(http.MethodDelete, "/storage/v1/b/phase4-bucket/o/versioned.txt?ifGenerationMatch="+url.QueryEscape(secondObject.Generation)+"&ifMetagenerationMatch=1", "gcs-phase4-token", nil)
	deleteRec := fixture.serve(deleteReq)
	if got, want := deleteRec.Code, http.StatusNoContent; got != want {
		t.Fatalf("delete status = %d, want %d, body = %q", got, want, deleteRec.Body.String())
	}
}

func TestRegisterSignedURLPutStoresObject(t *testing.T) {
	// This checks that signed URL uploads persist the object and preserve the declared content type.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	req := httptest.NewRequest(http.MethodPut, "/demo/signed.txt?GoogleAccessId=gcs@mock.iam.gserviceaccount.com&Signature=fake", strings.NewReader("signed-body"))
	req.Header.Set("Content-Type", "text/plain")
	rec := fixture.serve(req)
	if rec.Code != http.StatusOK {
		t.Fatalf("signed PUT status = %d, want %d, body = %q", rec.Code, http.StatusOK, rec.Body.String())
	}

	getReq := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o/signed.txt?alt=media", nil)
	getRec := fixture.serve(getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d", getRec.Code, http.StatusOK)
	}
	if got, want := getRec.Body.String(), "signed-body"; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
	if got, want := getRec.Header().Get("Content-Type"), "text/plain"; got != want {
		t.Fatalf("content type = %q, want %q", got, want)
	}
}

func TestRegisterGetObjectMetadataIncludesGeneration(t *testing.T) {
	// This checks that object metadata responses include stable generation and metageneration fields.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	putReq := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name=file.txt", strings.NewReader("payload"))
	putRec := fixture.serve(putReq)
	if putRec.Code != http.StatusOK {
		t.Fatalf("upload status = %d, want %d, body = %q", putRec.Code, http.StatusOK, putRec.Body.String())
	}

	getReq := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o/file.txt", nil)
	getRec := fixture.serve(getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want %d, body = %q", getRec.Code, http.StatusOK, getRec.Body.String())
	}

	var resp struct {
		ID             string `json:"id"`
		Generation     string `json:"generation"`
		Metageneration string `json:"metageneration"`
	}
	if err := json.NewDecoder(getRec.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if resp.ID == "" {
		t.Fatal("id is empty")
	}
	if resp.Generation == "" || resp.Generation == "0" {
		t.Fatalf("generation = %q, want non-zero", resp.Generation)
	}
	if resp.Metageneration != "1" {
		t.Fatalf("metageneration = %q, want %q", resp.Metageneration, "1")
	}
}

func TestRegisterDeleteMissingObjectIsIdempotent(t *testing.T) {
	// This checks that deleting a missing object still returns the GCS no-content success status.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	req := fixture.authedRequest(http.MethodDelete, "/storage/v1/b/demo/o/missing/", nil)
	rec := fixture.serve(req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d, body = %q", rec.Code, http.StatusNoContent, rec.Body.String())
	}
}

func TestRegisterDeleteObjectRemovesPrefixDescendants(t *testing.T) {
	// This checks that deleting a directory-style object removes the marker and every descendant under the prefix.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	items := []struct {
		key  string
		body string
	}{
		{key: "tree", body: ""},
		{key: "tree/child.txt", body: "payload"},
		{key: "tree/nested/grandchild.txt", body: "payload"},
	}
	for _, item := range items {
		putReq := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name="+item.key, strings.NewReader(item.body))
		putRec := fixture.serve(putReq)
		if putRec.Code != http.StatusOK {
			t.Fatalf("upload %q status = %d, want %d, body = %q", item.key, putRec.Code, http.StatusOK, putRec.Body.String())
		}
	}

	req := fixture.authedRequest(http.MethodDelete, "/storage/v1/b/demo/o/tree", nil)
	rec := fixture.serve(req)

	// GCS-style directory deletes should clear both the marker object and every descendant under that prefix.
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d, body = %q", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	listReq := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o?prefix=tree", nil)
	listRec := fixture.serve(listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", listRec.Code, http.StatusOK)
	}
	var resp struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	if err := json.NewDecoder(listRec.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(resp.Items) != 0 {
		t.Fatalf("items = %+v, want empty", resp.Items)
	}
}

func TestRegisterZeroByteDirectoryMarkerAllowsChildren(t *testing.T) {
	// This checks that zero-byte directory markers do not prevent creating child objects beneath the prefix.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	markerReq := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name=tree", strings.NewReader(""))
	markerRec := fixture.serve(markerReq)
	if markerRec.Code != http.StatusOK {
		t.Fatalf("marker status = %d, want %d, body = %q", markerRec.Code, http.StatusOK, markerRec.Body.String())
	}

	childReq := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name=tree/child.txt", strings.NewReader("payload"))
	childRec := fixture.serve(childReq)
	if childRec.Code != http.StatusOK {
		t.Fatalf("child status = %d, want %d, body = %q", childRec.Code, http.StatusOK, childRec.Body.String())
	}
}

func TestRegisterListObjectsDelimiterReturnsPrefixes(t *testing.T) {
	// This checks that delimiter listing returns both direct children and rolled-up prefixes.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	for _, key := range []string{"dir/file.txt", "dir/sub/nested.txt"} {
		putReq := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name="+key, strings.NewReader("payload"))
		putRec := fixture.serve(putReq)
		if putRec.Code != http.StatusOK {
			t.Fatalf("upload %q status = %d, want %d, body = %q", key, putRec.Code, http.StatusOK, putRec.Body.String())
		}
	}

	req := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o?prefix=dir/&delimiter=/", nil)
	rec := fixture.serve(req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %q", rec.Code, http.StatusOK, rec.Body.String())
	}
	var resp struct {
		Prefixes []string `json:"prefixes"`
		Items    []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].Name != "dir/file.txt" {
		t.Fatalf("items = %+v, want dir/file.txt", resp.Items)
	}
	if len(resp.Prefixes) != 1 || resp.Prefixes[0] != "dir/sub/" {
		t.Fatalf("prefixes = %v, want %v", resp.Prefixes, []string{"dir/sub/"})
	}
}

func TestRegisterListObjectsUsesStartOffsetAndOpaqueToken(t *testing.T) {
	// This checks that object listing uses opaque page tokens while still honoring startOffset pagination.
	fixture := newGCSTestFixture(t)
	fixture.mustCreateBucket(t, "demo")

	for _, key := range []string{
		"logs/2024-01.txt",
		"logs/2024-02.txt",
		"logs/2024-03.txt",
		"logs/2024-04.txt",
	} {
		putReq := fixture.authedRequest(http.MethodPost, "/upload/storage/v1/b/demo/o?uploadType=media&name="+key, strings.NewReader("x"))
		putRec := fixture.serve(putReq)
		if got, want := putRec.Code, http.StatusOK; got != want {
			t.Fatalf("upload %q status = %d, want %d, body = %q", key, got, want, putRec.Body.String())
		}
	}

	firstReq := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o?prefix=logs/&maxResults=2", nil)
	firstRec := fixture.serve(firstReq)
	if got, want := firstRec.Code, http.StatusOK; got != want {
		t.Fatalf("first page status = %d, want %d, body = %q", got, want, firstRec.Body.String())
	}
	var first struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
		NextPageToken string `json:"nextPageToken"`
	}
	if err := json.NewDecoder(firstRec.Body).Decode(&first); err != nil {
		t.Fatalf("Decode(first page) error = %v", err)
	}
	if len(first.Items) != 2 {
		t.Fatalf("first page items = %d, want 2", len(first.Items))
	}
	if first.NextPageToken == "" {
		t.Fatal("nextPageToken is empty")
	}
	if first.NextPageToken == first.Items[len(first.Items)-1].Name {
		t.Fatalf("nextPageToken = %q, want opaque token", first.NextPageToken)
	}

	secondReq := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o?prefix=logs/&maxResults=2&pageToken="+url.QueryEscape(first.NextPageToken), nil)
	secondRec := fixture.serve(secondReq)
	if got, want := secondRec.Code, http.StatusOK; got != want {
		t.Fatalf("second page status = %d, want %d, body = %q", got, want, secondRec.Body.String())
	}
	var second struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	if err := json.NewDecoder(secondRec.Body).Decode(&second); err != nil {
		t.Fatalf("Decode(second page) error = %v", err)
	}
	if len(second.Items) != 2 {
		t.Fatalf("second page items = %d, want 2", len(second.Items))
	}
	if got, want := second.Items[0].Name, "logs/2024-03.txt"; got != want {
		t.Fatalf("second page first item = %q, want %q", got, want)
	}

	offsetReq := fixture.authedRequest(http.MethodGet, "/storage/v1/b/demo/o?prefix=logs/&startOffset=logs/2024-02.txt", nil)
	offsetRec := fixture.serve(offsetReq)
	if got, want := offsetRec.Code, http.StatusOK; got != want {
		t.Fatalf("startOffset status = %d, want %d, body = %q", got, want, offsetRec.Body.String())
	}
	var offset struct {
		Items []struct {
			Name string `json:"name"`
		} `json:"items"`
	}
	if err := json.NewDecoder(offsetRec.Body).Decode(&offset); err != nil {
		t.Fatalf("Decode(startOffset) error = %v", err)
	}
	if len(offset.Items) != 2 {
		t.Fatalf("startOffset items = %d, want 2", len(offset.Items))
	}
	if got, want := offset.Items[0].Name, "logs/2024-03.txt"; got != want {
		t.Fatalf("startOffset first item = %q, want %q", got, want)
	}
}

func TestDeleteObjectTreeRemovesPagedDescendants(t *testing.T) {
	// This checks that recursive delete walks across multiple metadata pages until every descendant is removed.
	fixture := newGCSTestFixture(t)
	ctx := context.Background()
	fixture.mustCreateBucket(t, "demo")

	// This forces recursive delete to advance across more than one metadata page.
	for i := 0; i < 1005; i++ {
		key := fmt.Sprintf("tree/file-%04d.txt", i)
		meta, _, err := putObjectWithCRC32C(ctx, fixture.deps, "demo", key, strings.NewReader("payload"))
		if err != nil {
			t.Fatalf("putObjectWithCRC32C(%q) error = %v", key, err)
		}
		if err := fixture.deps.Metadata.PutObject(ctx, meta); err != nil {
			t.Fatalf("PutObject(metadata, %q) error = %v", key, err)
		}
	}

	if err := deleteObjectTree(ctx, fixture.deps, "demo", "tree"); err != nil {
		t.Fatalf("deleteObjectTree() error = %v", err)
	}

	items, err := fixture.metadata.ListObjects(ctx, "demo", "tree", 2000, "")
	if err != nil {
		t.Fatalf("ListObjects() error = %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("len(ListObjects()) = %d, want 0", len(items))
	}
}
