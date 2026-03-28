package seed

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"strings"
)

type ServiceAccountJSON struct {
	Type         string `json:"type"`
	ClientEmail  string `json:"client_email"`
	PrivateKey   string `json:"private_key"`
	PrivateKeyID string `json:"private_key_id"`
	ClientID     string `json:"client_id"`
	TokenURI     string `json:"token_uri"`
	ProjectID    string `json:"project_id"`
	Principal    string `json:"-"`
}

func GenerateServiceAccountJSON(host string, port int, clientEmail string) (ServiceAccountJSON, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return ServiceAccountJSON{}, fmt.Errorf("generate rsa key: %w", err)
	}
	privateKeyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return ServiceAccountJSON{}, fmt.Errorf("marshal pkcs8 private key: %w", err)
	}

	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: privateKeyDER,
	})

	clientID := generateClientID()
	clientEmail = strings.TrimSpace(clientEmail)
	if clientEmail == "" {
		return ServiceAccountJSON{}, fmt.Errorf("client email is required")
	}

	tokenURI := fmt.Sprintf("http://%s:%d/oauth2/v4/token", host, port)

	return ServiceAccountJSON{
		Type:         "service_account",
		ClientEmail:  clientEmail,
		PrivateKey:   string(privateKeyPEM),
		PrivateKeyID: generatePrivateKeyID(),
		ClientID:     clientID,
		TokenURI:     tokenURI,
		ProjectID:    "mockbucket",
	}, nil
}

func generateClientID() string {
	bi := big.Int{}
	buf := make([]byte, 12)
	_, _ = rand.Read(buf)
	bi.SetBytes(buf)
	return bi.String()
}

func generatePrivateKeyID() string {
	buf := make([]byte, 20)
	_, _ = rand.Read(buf)
	return hex.EncodeToString(buf)
}
