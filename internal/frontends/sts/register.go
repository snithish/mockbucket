package sts

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/httpx"
)

const xmlNamespace = "https://sts.amazonaws.com/doc/2011-06-15/"

func RootHandler(deps common.Dependencies) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleAction(w, r, deps)
	})
}

func handleAction(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	action, err := requestAction(r)
	if err != nil {
		writeError(w, err)
		return
	}
	switch action {
	case "AssumeRole":
		handleAssumeRole(w, r, deps)
	case "GetCallerIdentity":
		handleGetCallerIdentity(w, r, deps)
	case "GetSessionToken":
		handleGetSessionToken(w, r, deps)
	default:
		http.NotFound(w, r)
	}
}

func handleAssumeRole(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	roleARN := r.Form.Get("RoleArn")
	sessionName := r.Form.Get("RoleSessionName")
	roleName, err := roleNameFromARN(roleARN)
	if err != nil || sessionName == "" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	accessKeyID := extractAccessKeyID(r.Header.Get("Authorization"))
	duration, err := durationFromRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}
	session, err := deps.SessionManager.AssumeRoleWithDuration(r.Context(), roleName, sessionName, accessKeyID, duration)
	if err != nil {
		writeError(w, err)
		return
	}
	response := assumeRoleResponse{}
	response.Xmlns = xmlNamespace
	response.Result.Credentials.AccessKeyID = session.AccessKeyID
	response.Result.Credentials.SecretAccessKey = session.SecretKey
	response.Result.Credentials.SessionToken = session.Token
	response.Result.Credentials.Expiration = session.ExpiresAt.UTC().Format("2006-01-02T15:04:05Z")
	response.Result.AssumedRoleUser.Arn = fmt.Sprintf("arn:mockbucket:sts:::assumed-role/%s/%s", roleName, sessionName)
	response.Result.AssumedRoleUser.AssumedRoleID = fmt.Sprintf("%s:%s", session.AccessKeyID, sessionName)
	response.Metadata.RequestID = httpx.RequestIDFromContext(r.Context())
	writeXML(w, http.StatusOK, response)
}

func handleGetCallerIdentity(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	identity, err := callerIdentityFromRequest(r, deps)
	if err != nil {
		writeError(w, err)
		return
	}
	response := getCallerIdentityResponse{}
	response.Xmlns = xmlNamespace
	response.Result.Account = identity.Account
	response.Result.Arn = identity.ARN
	response.Result.UserID = identity.UserID
	response.Metadata.RequestID = httpx.RequestIDFromContext(r.Context())
	writeXML(w, http.StatusOK, response)
}

func handleGetSessionToken(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	accessKeyID := extractAccessKeyID(r.Header.Get("Authorization"))
	if accessKeyID == "" || sessionTokenFromRequest(r) != "" {
		writeError(w, core.ErrUnauthenticated)
		return
	}
	duration, err := durationFromRequest(r)
	if err != nil {
		writeError(w, err)
		return
	}
	session, err := deps.SessionManager.IssueSessionToken(r.Context(), accessKeyID, duration)
	if err != nil {
		writeError(w, err)
		return
	}
	response := getSessionTokenResponse{}
	response.Xmlns = xmlNamespace
	response.Result.Credentials.AccessKeyID = session.AccessKeyID
	response.Result.Credentials.SecretAccessKey = session.SecretKey
	response.Result.Credentials.SessionToken = session.Token
	response.Result.Credentials.Expiration = session.ExpiresAt.UTC().Format("2006-01-02T15:04:05Z")
	response.Metadata.RequestID = httpx.RequestIDFromContext(r.Context())
	writeXML(w, http.StatusOK, response)
}

type assumeRoleResponse struct {
	XMLName xml.Name `xml:"AssumeRoleResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		Credentials struct {
			AccessKeyID     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
			SessionToken    string `xml:"SessionToken"`
			Expiration      string `xml:"Expiration"`
		} `xml:"Credentials"`
		AssumedRoleUser struct {
			Arn           string `xml:"Arn"`
			AssumedRoleID string `xml:"AssumedRoleId"`
		} `xml:"AssumedRoleUser"`
	} `xml:"AssumeRoleResult"`
	Metadata struct {
		RequestID string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

type getCallerIdentityResponse struct {
	XMLName xml.Name `xml:"GetCallerIdentityResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		Account string `xml:"Account"`
		Arn     string `xml:"Arn"`
		UserID  string `xml:"UserId"`
	} `xml:"GetCallerIdentityResult"`
	Metadata struct {
		RequestID string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

type getSessionTokenResponse struct {
	XMLName xml.Name `xml:"GetSessionTokenResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		Credentials struct {
			AccessKeyID     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
			SessionToken    string `xml:"SessionToken"`
			Expiration      string `xml:"Expiration"`
		} `xml:"Credentials"`
	} `xml:"GetSessionTokenResult"`
	Metadata struct {
		RequestID string `xml:"RequestId"`
	} `xml:"ResponseMetadata"`
}

type callerIdentity struct {
	Account string
	ARN     string
	UserID  string
}

func requestAction(r *http.Request) (string, error) {
	if err := r.ParseForm(); err != nil {
		return "", core.ErrInvalidArgument
	}
	return strings.TrimSpace(r.Form.Get("Action")), nil
}

func durationFromRequest(r *http.Request) (time.Duration, error) {
	raw := strings.TrimSpace(r.Form.Get("DurationSeconds"))
	if raw == "" {
		return 0, nil
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return 0, core.ErrInvalidArgument
	}
	return time.Duration(seconds) * time.Second, nil
}

func roleNameFromARN(roleARN string) (string, error) {
	if !strings.Contains(roleARN, ":role/") {
		return "", core.ErrInvalidArgument
	}
	parts := strings.SplitN(roleARN, ":role/", 2)
	name := strings.TrimSpace(parts[1])
	if name == "" || strings.Contains(name, "/") {
		return "", core.ErrInvalidArgument
	}
	return name, nil
}

func callerIdentityFromRequest(r *http.Request, deps common.Dependencies) (callerIdentity, error) {
	accessKeyID := extractAccessKeyID(r.Header.Get("Authorization"))
	if accessKeyID == "" {
		return callerIdentity{}, core.ErrUnauthenticated
	}
	sessionToken := sessionTokenFromRequest(r)
	if sessionToken != "" {
		session, err := deps.SessionManager.LookupSession(r.Context(), sessionToken)
		if err != nil {
			return callerIdentity{}, err
		}
		if session.AccessKeyID != accessKeyID {
			return callerIdentity{}, core.ErrUnauthenticated
		}
		if session.RoleName != "" {
			return callerIdentity{
				Account: "000000000000",
				ARN:     fmt.Sprintf("arn:mockbucket:sts:::assumed-role/%s/%s", session.RoleName, session.SessionName),
				UserID:  fmt.Sprintf("%s:%s", session.AccessKeyID, session.SessionName),
			}, nil
		}
		name := session.PrincipalName
		if name == "" {
			name = accessKeyID
		}
		return callerIdentity{
			Account: "000000000000",
			ARN:     fmt.Sprintf("arn:mockbucket:iam:::user/%s", name),
			UserID:  name,
		}, nil
	}
	if _, err := deps.SessionManager.Store.FindAccessKey(r.Context(), accessKeyID); err != nil {
		return callerIdentity{}, err
	}
	return callerIdentity{
		Account: "000000000000",
		ARN:     fmt.Sprintf("arn:mockbucket:iam:::user/%s", accessKeyID),
		UserID:  accessKeyID,
	}, nil
}

func sessionTokenFromRequest(r *http.Request) string {
	token := strings.TrimSpace(r.Header.Get("X-Amz-Security-Token"))
	if token != "" {
		return token
	}
	return strings.TrimSpace(r.Form.Get("SecurityToken"))
}

// extractAccessKeyID parses the access key ID field from an AWS SigV4
// Authorization header. It does not verify request signatures.
// Format: "AWS4-HMAC-SHA256 Credential=ACCESS_KEY/20240101/..."
// Returns empty string if the header is missing or malformed.
func extractAccessKeyID(authHeader string) string {
	if !strings.HasPrefix(authHeader, "AWS4-HMAC-SHA256") {
		return ""
	}
	idx := strings.Index(authHeader, "Credential=")
	if idx < 0 {
		return ""
	}
	rest := authHeader[idx+len("Credential="):]
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return rest
	}
	return rest[:slash]
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
	http.Error(w, err.Error(), httpx.StatusCode(err))
}
