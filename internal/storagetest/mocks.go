package storagetest

import (
	"context"
	"errors"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/storage"
)

type NoopObjectStore struct{}

func (NoopObjectStore) PutObject(context.Context, string, string, storage.ObjectSource) (core.ObjectMetadata, error) {
	return core.ObjectMetadata{}, nil
}

func (NoopObjectStore) OpenObject(context.Context, string, string) (storage.ObjectReader, core.ObjectMetadata, error) {
	return nil, core.ObjectMetadata{}, core.ErrNotFound
}

func (NoopObjectStore) DeleteObject(context.Context, string, string) error { return nil }

func (NoopObjectStore) PutMultipartPart(context.Context, string, int, storage.ObjectSource) (core.MultipartPart, error) {
	return core.MultipartPart{}, nil
}

func (NoopObjectStore) CompleteMultipartUpload(context.Context, string, string, []core.MultipartPart) (core.ObjectMetadata, error) {
	return core.ObjectMetadata{}, nil
}

func (NoopObjectStore) AbortMultipartUpload(context.Context, string) error { return nil }

type FailingMetadataStore struct {
	Bucket    string
	BucketErr error
	PutErr    error
	DeleteErr error
}

func (m *FailingMetadataStore) Ping(context.Context) error                 { return nil }
func (m *FailingMetadataStore) EnsureBucket(context.Context, string) error { return nil }
func (m *FailingMetadataStore) CreateBucket(context.Context, string) error { return nil }
func (m *FailingMetadataStore) DeleteBucket(context.Context, string) error { return nil }
func (m *FailingMetadataStore) GetBucket(context.Context, string) (core.Bucket, error) {
	if m.BucketErr != nil {
		return core.Bucket{}, m.BucketErr
	}
	return core.Bucket{Name: m.Bucket}, nil
}
func (m *FailingMetadataStore) ListBuckets(context.Context) ([]core.Bucket, error) { return nil, nil }
func (m *FailingMetadataStore) PutObject(context.Context, core.ObjectMetadata) error {
	return m.PutErr
}
func (m *FailingMetadataStore) GetObject(context.Context, string, string) (core.ObjectMetadata, error) {
	return core.ObjectMetadata{}, core.ErrNotFound
}
func (m *FailingMetadataStore) DeleteObject(context.Context, string, string) error {
	return m.DeleteErr
}
func (m *FailingMetadataStore) ListObjects(context.Context, string, string, int, string) ([]core.ObjectMetadata, error) {
	return nil, nil
}
func (m *FailingMetadataStore) UpsertRole(context.Context, core.Role) error { return nil }
func (m *FailingMetadataStore) FindAccessKey(context.Context, string) (core.AccessKey, error) {
	return core.AccessKey{}, core.ErrNotFound
}
func (m *FailingMetadataStore) GetRole(context.Context, string) (core.Role, error) {
	return core.Role{}, core.ErrNotFound
}
func (m *FailingMetadataStore) CreateSession(context.Context, core.Session) error { return nil }
func (m *FailingMetadataStore) GetSession(context.Context, string) (core.Session, error) {
	return core.Session{}, core.ErrNotFound
}
func (m *FailingMetadataStore) DeleteExpiredSessions(context.Context, time.Time) error { return nil }
func (m *FailingMetadataStore) CreateMultipartUpload(context.Context, core.MultipartUpload) error {
	return nil
}
func (m *FailingMetadataStore) GetMultipartUpload(context.Context, string) (core.MultipartUpload, error) {
	return core.MultipartUpload{}, core.ErrNotFound
}
func (m *FailingMetadataStore) PutMultipartPart(context.Context, core.MultipartPart) error {
	return nil
}
func (m *FailingMetadataStore) ListMultipartParts(context.Context, string) ([]core.MultipartPart, error) {
	return nil, nil
}
func (m *FailingMetadataStore) DeleteMultipartUpload(context.Context, string) error { return nil }
func (m *FailingMetadataStore) UpsertServiceAccount(context.Context, core.ServiceAccount) error {
	return nil
}
func (m *FailingMetadataStore) FindServiceAccountByToken(context.Context, string) (core.ServiceAccount, error) {
	return core.ServiceAccount{}, core.ErrNotFound
}
func (m *FailingMetadataStore) FindServiceAccountByEmail(context.Context, string) (core.ServiceAccount, error) {
	return core.ServiceAccount{}, core.ErrNotFound
}
func (m *FailingMetadataStore) ListServiceAccounts(context.Context) ([]core.ServiceAccount, error) {
	return nil, nil
}
func (m *FailingMetadataStore) DeleteServiceAccounts(context.Context) error { return nil }

type MultipartMetadataStore struct {
	Bucket               string
	Key                  string
	UploadID             string
	Parts                []core.MultipartPart
	DeleteMultipartErr   error
	AllowMetadataDeletes bool
}

func (m *MultipartMetadataStore) Ping(context.Context) error                 { return nil }
func (m *MultipartMetadataStore) EnsureBucket(context.Context, string) error { return nil }
func (m *MultipartMetadataStore) CreateBucket(context.Context, string) error { return nil }
func (m *MultipartMetadataStore) DeleteBucket(context.Context, string) error { return nil }
func (m *MultipartMetadataStore) GetBucket(context.Context, string) (core.Bucket, error) {
	return core.Bucket{Name: m.Bucket}, nil
}
func (m *MultipartMetadataStore) ListBuckets(context.Context) ([]core.Bucket, error) { return nil, nil }
func (m *MultipartMetadataStore) PutObject(context.Context, core.ObjectMetadata) error {
	return nil
}
func (m *MultipartMetadataStore) GetObject(context.Context, string, string) (core.ObjectMetadata, error) {
	return core.ObjectMetadata{}, core.ErrNotFound
}
func (m *MultipartMetadataStore) DeleteObject(context.Context, string, string) error {
	if m.AllowMetadataDeletes {
		return nil
	}
	return errors.New("delete object failed")
}
func (m *MultipartMetadataStore) ListObjects(context.Context, string, string, int, string) ([]core.ObjectMetadata, error) {
	return nil, nil
}
func (m *MultipartMetadataStore) UpsertRole(context.Context, core.Role) error { return nil }
func (m *MultipartMetadataStore) FindAccessKey(context.Context, string) (core.AccessKey, error) {
	return core.AccessKey{}, core.ErrNotFound
}
func (m *MultipartMetadataStore) GetRole(context.Context, string) (core.Role, error) {
	return core.Role{}, core.ErrNotFound
}
func (m *MultipartMetadataStore) CreateSession(context.Context, core.Session) error { return nil }
func (m *MultipartMetadataStore) GetSession(context.Context, string) (core.Session, error) {
	return core.Session{}, core.ErrNotFound
}
func (m *MultipartMetadataStore) DeleteExpiredSessions(context.Context, time.Time) error { return nil }
func (m *MultipartMetadataStore) CreateMultipartUpload(context.Context, core.MultipartUpload) error {
	return nil
}
func (m *MultipartMetadataStore) GetMultipartUpload(context.Context, string) (core.MultipartUpload, error) {
	return core.MultipartUpload{UploadID: m.UploadID, Bucket: m.Bucket, Key: m.Key}, nil
}
func (m *MultipartMetadataStore) PutMultipartPart(context.Context, core.MultipartPart) error {
	return nil
}
func (m *MultipartMetadataStore) ListMultipartParts(context.Context, string) ([]core.MultipartPart, error) {
	return m.Parts, nil
}
func (m *MultipartMetadataStore) DeleteMultipartUpload(context.Context, string) error {
	return m.DeleteMultipartErr
}
func (m *MultipartMetadataStore) UpsertServiceAccount(context.Context, core.ServiceAccount) error {
	return nil
}
func (m *MultipartMetadataStore) FindServiceAccountByToken(context.Context, string) (core.ServiceAccount, error) {
	return core.ServiceAccount{}, core.ErrNotFound
}
func (m *MultipartMetadataStore) FindServiceAccountByEmail(context.Context, string) (core.ServiceAccount, error) {
	return core.ServiceAccount{}, core.ErrNotFound
}
func (m *MultipartMetadataStore) ListServiceAccounts(context.Context) ([]core.ServiceAccount, error) {
	return nil, nil
}
func (m *MultipartMetadataStore) DeleteServiceAccounts(context.Context) error { return nil }
