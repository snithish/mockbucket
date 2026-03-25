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

type AccessKey struct {
	ID            string    `json:"id" yaml:"id"`
	Secret        string    `json:"secret" yaml:"secret"`
	PrincipalName string    `json:"principal_name" yaml:"principal_name"`
	CreatedAt     time.Time `json:"created_at" yaml:"-"`
}

type Principal struct {
	Name string `json:"name" yaml:"name"`
}

type Role struct {
	Name string `json:"name" yaml:"name"`
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

type Subject struct {
	PrincipalName string
	RoleName      string
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
