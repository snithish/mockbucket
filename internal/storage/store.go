package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"github.com/snithish/mockbucket/internal/core"
)

type ObjectStore interface {
	PutObject(ctx context.Context, bucket, key string, src ObjectSource) (core.ObjectMetadata, error)
	OpenObject(ctx context.Context, bucket, key string) (ObjectReader, core.ObjectMetadata, error)
	DeleteObject(ctx context.Context, bucket, key string) error
}

type MetadataStore interface {
	Ping(ctx context.Context) error
	EnsureBucket(ctx context.Context, name string) error
	CreateBucket(ctx context.Context, name string) error
	GetBucket(ctx context.Context, name string) (core.Bucket, error)
	ListBuckets(ctx context.Context) ([]core.Bucket, error)
	PutObject(ctx context.Context, meta core.ObjectMetadata) error
	GetObject(ctx context.Context, bucket, key string) (core.ObjectMetadata, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ListObjects(ctx context.Context, bucket, prefix string, limit int, after string) ([]core.ObjectMetadata, error)
	UpsertPrincipal(ctx context.Context, principal core.Principal) error
	UpsertRole(ctx context.Context, role core.Role) error
	FindAccessKey(ctx context.Context, accessKeyID string) (core.AccessKey, []core.PolicyDocument, error)
	GetRole(ctx context.Context, name string) (core.Role, error)
	CreateSession(ctx context.Context, session core.Session) error
	GetSession(ctx context.Context, token string) (core.Session, []core.PolicyDocument, error)
	DeleteExpiredSessions(ctx context.Context, now time.Time) error
	Close() error
}

type SQLiteStore struct {
	db *sql.DB
}

func OpenSQLite(path string) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	store := &SQLiteStore{db: db}
	if err := store.initSchema(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) Ping(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) initSchema() error {
	statements := []string{
		`PRAGMA journal_mode=WAL;`,
		`CREATE TABLE IF NOT EXISTS buckets (name TEXT PRIMARY KEY, created_at TIMESTAMP NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS objects (
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			path TEXT NOT NULL,
			size INTEGER NOT NULL,
			etag TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			modified_at TIMESTAMP NOT NULL,
			PRIMARY KEY (bucket, key)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_objects_bucket_key ON objects(bucket, key);`,
		`CREATE TABLE IF NOT EXISTS principals (name TEXT PRIMARY KEY, policies_json TEXT NOT NULL, created_at TIMESTAMP NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS access_keys (id TEXT PRIMARY KEY, secret TEXT NOT NULL, principal_name TEXT NOT NULL, created_at TIMESTAMP NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS roles (name TEXT PRIMARY KEY, trust_json TEXT NOT NULL, policies_json TEXT NOT NULL, created_at TIMESTAMP NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS sessions (
			token TEXT PRIMARY KEY,
			access_key_id TEXT NOT NULL,
			secret_key TEXT NOT NULL,
			principal_name TEXT NOT NULL,
			role_name TEXT NOT NULL,
			session_name TEXT NOT NULL,
			expires_at TIMESTAMP NOT NULL,
			created_at TIMESTAMP NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at);`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	return nil
}

func (s *SQLiteStore) EnsureBucket(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO buckets(name, created_at) VALUES(?, ?) ON CONFLICT(name) DO NOTHING`, name, time.Now().UTC())
	return err
}

func (s *SQLiteStore) CreateBucket(ctx context.Context, name string) error {
	res, err := s.db.ExecContext(ctx, `INSERT INTO buckets(name, created_at) VALUES(?, ?) ON CONFLICT(name) DO NOTHING`, name, time.Now().UTC())
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return core.ErrConflict
	}
	return nil
}

func (s *SQLiteStore) GetBucket(ctx context.Context, name string) (core.Bucket, error) {
	var bucket core.Bucket
	row := s.db.QueryRowContext(ctx, `SELECT name, created_at FROM buckets WHERE name = ?`, name)
	if err := row.Scan(&bucket.Name, &bucket.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return core.Bucket{}, core.ErrNotFound
		}
		return core.Bucket{}, err
	}
	return bucket, nil
}

func (s *SQLiteStore) ListBuckets(ctx context.Context) ([]core.Bucket, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, created_at FROM buckets ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var buckets []core.Bucket
	for rows.Next() {
		var bucket core.Bucket
		if err := rows.Scan(&bucket.Name, &bucket.CreatedAt); err != nil {
			return nil, err
		}
		buckets = append(buckets, bucket)
	}
	return buckets, rows.Err()
}

func (s *SQLiteStore) PutObject(ctx context.Context, meta core.ObjectMetadata) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO objects(bucket, key, path, size, etag, created_at, modified_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(bucket, key) DO UPDATE SET
			path = excluded.path,
			size = excluded.size,
			etag = excluded.etag,
			modified_at = excluded.modified_at`,
		meta.Bucket, meta.Key, meta.Path, meta.Size, meta.ETag, meta.CreatedAt, meta.ModifiedAt,
	)
	return err
}

func (s *SQLiteStore) GetObject(ctx context.Context, bucket, key string) (core.ObjectMetadata, error) {
	var meta core.ObjectMetadata
	row := s.db.QueryRowContext(ctx, `SELECT bucket, key, path, size, etag, created_at, modified_at FROM objects WHERE bucket = ? AND key = ?`, bucket, key)
	if err := row.Scan(&meta.Bucket, &meta.Key, &meta.Path, &meta.Size, &meta.ETag, &meta.CreatedAt, &meta.ModifiedAt); err != nil {
		if err == sql.ErrNoRows {
			return core.ObjectMetadata{}, core.ErrNotFound
		}
		return core.ObjectMetadata{}, err
	}
	return meta, nil
}

func (s *SQLiteStore) DeleteObject(ctx context.Context, bucket, key string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM objects WHERE bucket = ? AND key = ?`, bucket, key)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return core.ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) ListObjects(ctx context.Context, bucket, prefix string, limit int, after string) ([]core.ObjectMetadata, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT bucket, key, path, size, etag, created_at, modified_at
		FROM objects
		WHERE bucket = ? AND key LIKE ? AND key > ?
		ORDER BY key
		LIMIT ?`, bucket, prefix+`%`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var objects []core.ObjectMetadata
	for rows.Next() {
		var meta core.ObjectMetadata
		if err := rows.Scan(&meta.Bucket, &meta.Key, &meta.Path, &meta.Size, &meta.ETag, &meta.CreatedAt, &meta.ModifiedAt); err != nil {
			return nil, err
		}
		objects = append(objects, meta)
	}
	return objects, rows.Err()
}

func (s *SQLiteStore) UpsertPrincipal(ctx context.Context, principal core.Principal) error {
	policiesJSON, err := json.Marshal(principal.Policies)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO principals(name, policies_json, created_at) VALUES(?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET policies_json = excluded.policies_json`,
		principal.Name, string(policiesJSON), now,
	); err != nil {
		return err
	}
	for _, key := range principal.AccessKeys {
		if _, err := s.db.ExecContext(ctx, `
			INSERT INTO access_keys(id, secret, principal_name, created_at) VALUES(?, ?, ?, ?)
			ON CONFLICT(id) DO UPDATE SET secret = excluded.secret, principal_name = excluded.principal_name`,
			key.ID, key.Secret, principal.Name, now,
		); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQLiteStore) UpsertRole(ctx context.Context, role core.Role) error {
	trustJSON, err := json.Marshal(role.Trust)
	if err != nil {
		return err
	}
	policiesJSON, err := json.Marshal(role.Policies)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO roles(name, trust_json, policies_json, created_at) VALUES(?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET trust_json = excluded.trust_json, policies_json = excluded.policies_json`,
		role.Name, string(trustJSON), string(policiesJSON), time.Now().UTC(),
	)
	return err
}

func (s *SQLiteStore) FindAccessKey(ctx context.Context, accessKeyID string) (core.AccessKey, []core.PolicyDocument, error) {
	var key core.AccessKey
	var policiesJSON string
	row := s.db.QueryRowContext(ctx, `
		SELECT ak.id, ak.secret, ak.principal_name, ak.created_at, p.policies_json
		FROM access_keys ak
		JOIN principals p ON p.name = ak.principal_name
		WHERE ak.id = ?`, accessKeyID)
	if err := row.Scan(&key.ID, &key.Secret, &key.PrincipalName, &key.CreatedAt, &policiesJSON); err != nil {
		if err == sql.ErrNoRows {
			return core.AccessKey{}, nil, core.ErrNotFound
		}
		return core.AccessKey{}, nil, err
	}
	var policies []core.PolicyDocument
	if err := json.Unmarshal([]byte(policiesJSON), &policies); err != nil {
		return core.AccessKey{}, nil, err
	}
	return key, policies, nil
}

func (s *SQLiteStore) GetRole(ctx context.Context, name string) (core.Role, error) {
	var role core.Role
	var trustJSON, policiesJSON string
	row := s.db.QueryRowContext(ctx, `SELECT name, trust_json, policies_json FROM roles WHERE name = ?`, name)
	if err := row.Scan(&role.Name, &trustJSON, &policiesJSON); err != nil {
		if err == sql.ErrNoRows {
			return core.Role{}, core.ErrNotFound
		}
		return core.Role{}, err
	}
	if err := json.Unmarshal([]byte(trustJSON), &role.Trust); err != nil {
		return core.Role{}, err
	}
	if err := json.Unmarshal([]byte(policiesJSON), &role.Policies); err != nil {
		return core.Role{}, err
	}
	return role, nil
}

func (s *SQLiteStore) CreateSession(ctx context.Context, session core.Session) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sessions(token, access_key_id, secret_key, principal_name, role_name, session_name, expires_at, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)`,
		session.Token, session.AccessKeyID, session.SecretKey, session.PrincipalName, session.RoleName, session.SessionName, session.ExpiresAt, session.CreatedAt,
	)
	return err
}

func (s *SQLiteStore) GetSession(ctx context.Context, token string) (core.Session, []core.PolicyDocument, error) {
	var session core.Session
	var policiesJSON string
	row := s.db.QueryRowContext(ctx, `
		SELECT s.token, s.access_key_id, s.secret_key, s.principal_name, s.role_name, s.session_name, s.expires_at, s.created_at, r.policies_json
		FROM sessions s
		JOIN roles r ON r.name = s.role_name
		WHERE s.token = ?`, token)
	if err := row.Scan(&session.Token, &session.AccessKeyID, &session.SecretKey, &session.PrincipalName, &session.RoleName, &session.SessionName, &session.ExpiresAt, &session.CreatedAt, &policiesJSON); err != nil {
		if err == sql.ErrNoRows {
			return core.Session{}, nil, core.ErrNotFound
		}
		return core.Session{}, nil, err
	}
	var policies []core.PolicyDocument
	if err := json.Unmarshal([]byte(policiesJSON), &policies); err != nil {
		return core.Session{}, nil, err
	}
	return session, policies, nil
}

func (s *SQLiteStore) GetSessionByAccessKey(ctx context.Context, accessKeyID string) (core.Session, []core.PolicyDocument, error) {
	var session core.Session
	var policiesJSON string
	row := s.db.QueryRowContext(ctx, `
		SELECT s.token, s.access_key_id, s.secret_key, s.principal_name, s.role_name, s.session_name, s.expires_at, s.created_at, r.policies_json
		FROM sessions s
		JOIN roles r ON r.name = s.role_name
		WHERE s.access_key_id = ?`, accessKeyID)
	if err := row.Scan(&session.Token, &session.AccessKeyID, &session.SecretKey, &session.PrincipalName, &session.RoleName, &session.SessionName, &session.ExpiresAt, &session.CreatedAt, &policiesJSON); err != nil {
		if err == sql.ErrNoRows {
			return core.Session{}, nil, core.ErrNotFound
		}
		return core.Session{}, nil, err
	}
	var policies []core.PolicyDocument
	if err := json.Unmarshal([]byte(policiesJSON), &policies); err != nil {
		return core.Session{}, nil, err
	}
	return session, policies, nil
}

func (s *SQLiteStore) DeleteExpiredSessions(ctx context.Context, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE expires_at <= ?`, now)
	return err
}

func isUniqueConstraint(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "unique")
}
