package seed

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"
)

type ServiceAccountJSON struct {
	Type        string `json:"type"`
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
	ClientID    string `json:"client_id"`
	TokenURI    string `json:"token_uri"`
	ProjectID   string `json:"project_id"`
}

func GenerateServiceAccountJSON(host string, port int, clientEmail string) (ServiceAccountJSON, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return ServiceAccountJSON{}, fmt.Errorf("generate rsa key: %w", err)
	}

	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	clientID := generateClientID()
	clientEmail = strings.TrimSpace(clientEmail)
	if clientEmail == "" {
		return ServiceAccountJSON{}, fmt.Errorf("client email is required")
	}

	tokenURI := fmt.Sprintf("http://%s:%d/oauth2/v4/token", host, port)

	return ServiceAccountJSON{
		Type:        "service_account",
		ClientEmail: clientEmail,
		PrivateKey:  string(privateKeyPEM),
		ClientID:    clientID,
		TokenURI:    tokenURI,
		ProjectID:   "mockbucket",
	}, nil
}

func GenerateServiceAccounts(host string, port int, clientEmails []string) ([]ServiceAccountJSON, error) {
	var accounts []ServiceAccountJSON
	for _, clientEmail := range clientEmails {
		acc, err := GenerateServiceAccountJSON(host, port, clientEmail)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, acc)
	}
	return accounts, nil
}

func WriteServiceAccountJSON(path string, sa ServiceAccountJSON) error {
	data := fmt.Sprintf(`{
  "type": "service_account",
  "client_email": "%s",
  "private_key": "%s",
  "client_id": "%s",
  "token_uri": "%s",
  "project_id": "%s"
}`, sa.ClientEmail, escapeForJSON(sa.PrivateKey), sa.ClientID, sa.TokenURI, sa.ProjectID)

	return os.WriteFile(path, []byte(data), 0o600)
}

func escapeForJSON(s string) string {
	s = escapeBackslash(s)
	s = escapeQuote(s)
	return s
}

func escapeBackslash(s string) string {
	result := ""
	for _, c := range s {
		if c == '\\' {
			result += "\\\\"
		} else {
			result += string(c)
		}
	}
	return result
}

func escapeQuote(s string) string {
	result := ""
	for _, c := range s {
		if c == '"' {
			result += "\\\""
		} else {
			result += string(c)
		}
	}
	return result
}

func generateClientID() string {
	bi := big.Int{}
	buf := make([]byte, 12)
	_, _ = rand.Read(buf)
	bi.SetBytes(buf)
	return bi.String()
}

type ServiceAccountKey struct {
	ClientEmail   string    `json:"client_email"`
	PublicKey     string    `json:"public_key"`
	PrivateKey    string    `json:"private_key"`
	ClientID      string    `json:"client_id"`
	TokenURI      string    `json:"token_uri"`
	ProjectID     string    `json:"project_id"`
	PrincipalName string    `json:"principal_name"`
	CreatedAt     time.Time `json:"created_at"`
}

func PublicKeyFromPrivateKey(privateKey *rsa.PrivateKey) (string, error) {
	publicKey := &privateKey.PublicKey
	keyBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return "", fmt.Errorf("marshal public key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(keyBytes), nil
}
