package aws

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/httpx"
)

type CredentialResolver interface {
	ResolveAWSCredential(ctx context.Context, accessKeyID, sessionToken string) (core.CredentialIdentity, error)
}

type Verifier struct{}

type Signature struct {
	AccessKeyID   string
	Date          string
	Region        string
	Service       string
	SignedHeaders []string
	Signature     string
}

func Authenticate(service string, verifier Verifier, resolver CredentialResolver, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sig, err := ParseAuthorization(r.Header.Get("Authorization"))
		if err != nil {
			http.Error(w, err.Error(), httpx.StatusCode(err))
			return
		}
		credential, err := resolver.ResolveAWSCredential(r.Context(), sig.AccessKeyID, strings.TrimSpace(r.Header.Get("X-Amz-Security-Token")))
		if err != nil {
			http.Error(w, err.Error(), httpx.StatusCode(err))
			return
		}
		if err := verifier.Verify(r, credential, sig, service); err != nil {
			http.Error(w, err.Error(), httpx.StatusCode(err))
			return
		}
		next.ServeHTTP(w, r.WithContext(httpx.ContextWithSubject(r.Context(), credential.Subject)))
	})
}

func ParseAuthorization(header string) (Signature, error) {
	header = strings.TrimSpace(header)
	if header == "" {
		return Signature{}, core.ErrUnauthenticated
	}
	if !strings.HasPrefix(header, "AWS4-HMAC-SHA256 ") {
		return Signature{}, core.ErrInvalidArgument
	}
	parts := strings.Split(strings.TrimPrefix(header, "AWS4-HMAC-SHA256 "), ",")
	values := map[string]string{}
	for _, part := range parts {
		key, value, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			return Signature{}, core.ErrInvalidArgument
		}
		values[key] = value
	}
	credential := values["Credential"]
	signedHeaders := values["SignedHeaders"]
	signature := values["Signature"]
	if credential == "" || signedHeaders == "" || signature == "" {
		return Signature{}, core.ErrInvalidArgument
	}
	scope := strings.Split(credential, "/")
	if len(scope) != 5 || scope[4] != "aws4_request" {
		return Signature{}, core.ErrInvalidArgument
	}
	return Signature{
		AccessKeyID:   scope[0],
		Date:          scope[1],
		Region:        scope[2],
		Service:       scope[3],
		SignedHeaders: strings.Split(signedHeaders, ";"),
		Signature:     signature,
	}, nil
}

func (Verifier) Verify(r *http.Request, credential core.CredentialIdentity, sig Signature, service string) error {
	if sig.Service != service {
		return core.ErrSignatureMismatch
	}
	dateHeader := strings.TrimSpace(r.Header.Get("X-Amz-Date"))
	if dateHeader == "" {
		return core.ErrInvalidArgument
	}
	if _, err := time.Parse("20060102T150405Z", dateHeader); err != nil {
		return core.ErrInvalidArgument
	}
	payloadHash, err := payloadHash(r)
	if err != nil {
		return err
	}
	canonicalRequest, hashedCanonical, err := canonicalRequest(r, sig.SignedHeaders, payloadHash)
	if err != nil {
		return err
	}
	_ = canonicalRequest
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		dateHeader,
		strings.Join([]string{sig.Date, sig.Region, sig.Service, "aws4_request"}, "/"),
		hashedCanonical,
	}, "\n")
	expected := hex.EncodeToString(signingHMAC(credential.SecretKey, sig.Date, sig.Region, sig.Service, stringToSign))
	if subtle.ConstantTimeCompare([]byte(strings.ToLower(expected)), []byte(strings.ToLower(sig.Signature))) != 1 {
		return core.ErrSignatureMismatch
	}
	return nil
}

func canonicalRequest(r *http.Request, signedHeaders []string, payloadHash string) (string, string, error) {
	canonicalHeaders, err := buildCanonicalHeaders(r, signedHeaders)
	if err != nil {
		return "", "", err
	}
	request := strings.Join([]string{
		r.Method,
		canonicalURI(r.URL.Path),
		canonicalQueryString(r.URL.Query()),
		canonicalHeaders,
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
	sum := sha256.Sum256([]byte(request))
	return request, hex.EncodeToString(sum[:]), nil
}

func buildCanonicalHeaders(r *http.Request, signedHeaders []string) (string, error) {
	var lines []string
	for _, name := range signedHeaders {
		lower := strings.ToLower(strings.TrimSpace(name))
		if lower == "" {
			return "", core.ErrInvalidArgument
		}
		value := headerValue(r, lower)
		if value == "" {
			return "", core.ErrInvalidArgument
		}
		lines = append(lines, lower+":"+strings.Join(strings.Fields(value), " "))
	}
	return strings.Join(lines, "\n") + "\n", nil
}

func headerValue(r *http.Request, lower string) string {
	if lower == "host" {
		return r.Host
	}
	return r.Header.Get(lower)
}

func canonicalURI(path string) string {
	if path == "" {
		return "/"
	}
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		segments[i] = escapeRFC3986(segment)
	}
	result := strings.Join(segments, "/")
	if !strings.HasPrefix(result, "/") {
		return "/" + result
	}
	return result
}

func canonicalQueryString(values url.Values) string {
	if len(values) == 0 {
		return ""
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var parts []string
	for _, key := range keys {
		vals := append([]string(nil), values[key]...)
		sort.Strings(vals)
		if len(vals) == 0 {
			parts = append(parts, escapeRFC3986(key)+"=")
			continue
		}
		for _, value := range vals {
			parts = append(parts, escapeRFC3986(key)+"="+escapeRFC3986(value))
		}
	}
	return strings.Join(parts, "&")
}

func escapeRFC3986(value string) string {
	escaped := url.QueryEscape(value)
	escaped = strings.ReplaceAll(escaped, "+", "%20")
	escaped = strings.ReplaceAll(escaped, "*", "%2A")
	escaped = strings.ReplaceAll(escaped, "%7E", "~")
	return escaped
}

func payloadHash(r *http.Request) (string, error) {
	provided := strings.TrimSpace(r.Header.Get("X-Amz-Content-Sha256"))
	if provided == "UNSIGNED-PAYLOAD" {
		return provided, nil
	}
	if provided != "" {
		return provided, nil
	}
	var body []byte
	if r.Body != nil {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return "", fmt.Errorf("read request body: %w", err)
		}
		body = data
		r.Body = io.NopCloser(bytes.NewReader(data))
	}
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:]), nil
}

func signingHMAC(secret, date, region, service, stringToSign string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), date)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	return hmacSHA256(kSigning, stringToSign)
}

func hmacSHA256(key []byte, message string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(message))
	return mac.Sum(nil)
}
