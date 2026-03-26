package azure_shared

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"net/http"

	azauth "github.com/snithish/mockbucket/internal/auth/azure"
	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/frontends/common"
)

const APIVersion = "2021-06-08"

func BuildAuthResolver(cfg config.Config) (azauth.Authenticator, map[string]struct{}) {
	accountMap := map[string]azauth.AccountConfig{}
	for _, acc := range cfg.Seed.Azure.Accounts {
		accountMap[acc.Name] = azauth.AccountConfig{
			Name: acc.Name,
			Key:  decodeAccountKey(acc.Key),
		}
	}

	accounts := make([]azauth.AccountConfig, 0, len(accountMap))
	accountNames := make(map[string]struct{}, len(accountMap))
	for _, account := range accountMap {
		accounts = append(accounts, account)
		accountNames[account.Name] = struct{}{}
	}

	return azauth.NewAuthResolver(accounts), accountNames
}

func decodeAccountKey(raw string) []byte {
	keyBytes, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return []byte(raw)
	}
	return keyBytes
}

func SetVersionHeader(w http.ResponseWriter) {
	w.Header().Set("x-ms-version", APIVersion)
}

func WriteXML(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	raw, _ := xml.MarshalIndent(payload, "", "  ")
	_, _ = w.Write([]byte(xml.Header + string(raw)))
}

type XMLError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func WriteBlobError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("x-ms-error-code", code)
	w.WriteHeader(status)
	resp := XMLError{Code: code, Message: message}
	raw, _ := xml.MarshalIndent(resp, "", "  ")
	_, _ = w.Write([]byte(xml.Header + string(raw)))
}

func WriteDataLakeError(w http.ResponseWriter, status int, code, message string) {
	WriteBlobError(w, status, code, message)
}

func ListBuckets(ctx context.Context, deps common.Dependencies) ([]core.Bucket, error) {
	return deps.Metadata.ListBuckets(ctx)
}

func CreateBucket(ctx context.Context, deps common.Dependencies, name string) error {
	return deps.Metadata.CreateBucket(ctx, name)
}

func GetBucket(ctx context.Context, deps common.Dependencies, name string) (core.Bucket, error) {
	return deps.Metadata.GetBucket(ctx, name)
}

func DeleteBucket(ctx context.Context, deps common.Dependencies, name string) error {
	return deps.Metadata.DeleteBucket(ctx, name)
}
