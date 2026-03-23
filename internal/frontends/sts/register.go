package sts

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	authaws "github.com/snithish/mockbucket/internal/auth/aws"
	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/httpx"
)

const xmlNamespace = "https://sts.amazonaws.com/doc/2011-06-15/"

func Register(_ *http.ServeMux, _ config.Config, _ common.Dependencies) {}

func RootHandler(_ config.Config, deps common.Dependencies) http.Handler {
	return authaws.Authenticate("sts", deps.AWSVerifier, deps.AuthResolver, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleAssumeRole(w, r, deps)
	}))
}

func IsQueryRequest(r *http.Request) bool {
	action, _ := requestAction(r)
	return action != ""
}

func handleAssumeRole(w http.ResponseWriter, r *http.Request, deps common.Dependencies) {
	action, err := requestAction(r)
	if err != nil {
		writeError(w, err)
		return
	}
	if action != "AssumeRole" {
		http.NotFound(w, r)
		return
	}
	roleARN := r.Form.Get("RoleArn")
	sessionName := r.Form.Get("RoleSessionName")
	roleName, err := roleNameFromARN(roleARN)
	if err != nil || sessionName == "" {
		writeError(w, core.ErrInvalidArgument)
		return
	}
	subject, ok := httpx.SubjectFromContext(r.Context())
	if !ok {
		writeError(w, core.ErrUnauthenticated)
		return
	}
	principalName := subject.PrincipalName
	if principalName == "" {
		principalName = subject.Name()
	}
	resource := fmt.Sprintf("arn:mockbucket:iam:::role/%s", roleName)
	if !deps.Policy.Allowed("sts:AssumeRole", resource, subject.Policies) {
		writeError(w, core.ErrAccessDenied)
		return
	}
	session, err := deps.SessionManager.AssumeRole(r.Context(), principalName, roleName, sessionName)
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

func requestAction(r *http.Request) (string, error) {
	if err := r.ParseForm(); err != nil {
		return "", core.ErrInvalidArgument
	}
	return strings.TrimSpace(r.Form.Get("Action")), nil
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
