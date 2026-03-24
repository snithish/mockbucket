package core

import "time"

type ObjectMetadata struct {
	Bucket     string
	Key        string
	Path       string
	ETag       string
	Size       int64
	CreatedAt  time.Time
	ModifiedAt time.Time
}

type Bucket struct {
	Name      string
	CreatedAt time.Time
}

type Effect string

const (
	EffectAllow Effect = "Allow"
	EffectDeny  Effect = "Deny"
)

type PolicyStatement struct {
	Effect    Effect   `json:"effect" yaml:"effect"`
	Actions   []string `json:"actions" yaml:"actions"`
	Resources []string `json:"resources" yaml:"resources"`
}

type PolicyDocument struct {
	Statements []PolicyStatement `json:"statements" yaml:"statements"`
}

type TrustStatement struct {
	Effect     Effect   `json:"effect" yaml:"effect"`
	Principals []string `json:"principals" yaml:"principals"`
	Actions    []string `json:"actions" yaml:"actions"`
}

type TrustPolicyDocument struct {
	Statements []TrustStatement `json:"statements" yaml:"statements"`
}

type AccessKey struct {
	ID            string    `json:"id" yaml:"id"`
	Secret        string    `json:"secret" yaml:"secret"`
	PrincipalName string    `json:"principal_name" yaml:"principal_name"`
	CreatedAt     time.Time `json:"created_at" yaml:"-"`
}

type Principal struct {
	Name     string           `json:"name" yaml:"name"`
	Policies []PolicyDocument `json:"policies" yaml:"policies"`
}

type Role struct {
	Name     string              `json:"name" yaml:"name"`
	Trust    TrustPolicyDocument `json:"trust" yaml:"trust"`
	Policies []PolicyDocument    `json:"policies" yaml:"policies"`
}

type Session struct {
	Token         string
	AccessKeyID   string
	SecretKey     string
	PrincipalName string
	RoleName      string
	SessionName   string
	ExpiresAt     time.Time
	CreatedAt     time.Time
}

type MultipartUpload struct {
	UploadID  string
	Bucket    string
	Key       string
	CreatedAt time.Time
}

type MultipartPart struct {
	UploadID   string
	PartNumber int
	ETag       string
	Size       int64
	Path       string
	CreatedAt  time.Time
}

type CredentialIdentity struct {
	AccessKeyID   string
	SecretKey     string
	SessionToken  string
	PrincipalName string
	Subject       Subject
}

type Subject struct {
	PrincipalName string
	RoleName      string
	Policies      []PolicyDocument
}

func (s Subject) Name() string {
	if s.RoleName != "" {
		return s.RoleName
	}
	return s.PrincipalName
}

type ServiceAccount struct {
	ClientEmail string `json:"client_email" yaml:"client_email"`
	Principal   string `json:"principal" yaml:"principal"`
	Token       string `json:"token" yaml:"token"`
}
