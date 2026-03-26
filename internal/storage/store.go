package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	PutMultipartPart(ctx context.Context, uploadID string, partNumber int, src ObjectSource) (core.MultipartPart, error)
	CompleteMultipartUpload(ctx context.Context, bucket, key string, parts []core.MultipartPart) (core.ObjectMetadata, error)
	AbortMultipartUpload(ctx context.Context, uploadID string) error
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
	UpsertRole(ctx context.Context, role core.Role) error
	FindAccessKey(ctx context.Context, accessKeyID string) (core.AccessKey, error)
	GetRole(ctx context.Context, name string) (core.Role, error)
	CreateSession(ctx context.Context, session core.Session) error
	GetSession(ctx context.Context, token string) (core.Session, error)
	DeleteExpiredSessions(ctx context.Context, now time.Time) error
	CreateMultipartUpload(ctx context.Context, upload core.MultipartUpload) error
	GetMultipartUpload(ctx context.Context, uploadID string) (core.MultipartUpload, error)
	PutMultipartPart(ctx context.Context, part core.MultipartPart) error
	ListMultipartParts(ctx context.Context, uploadID string) ([]core.MultipartPart, error)
	DeleteMultipartUpload(ctx context.Context, uploadID string) error
	UpsertServiceAccount(ctx context.Context, sa core.ServiceAccount) error
	FindServiceAccountByToken(ctx context.Context, token string) (core.ServiceAccount, error)
	FindServiceAccountByEmail(ctx context.Context, email string) (core.ServiceAccount, error)
	ListServiceAccounts(ctx context.Context) ([]core.ServiceAccount, error)
	DeleteServiceAccounts(ctx context.Context) error
	Close() error
}

type SQLiteStore struct {
	db *sql.DB
}

type SeedState struct {
	Buckets         []string
	Roles           []core.Role
	Objects         []SeedObject
	AccessKeys      []SeedAccessKey
	ServiceAccounts []core.ServiceAccount
}

type SeedAccessKey struct {
	ID           string
	Secret       string
	AllowedRoles []string
}

type SeedObject struct {
	Bucket  string
	Key     string
	Content string
}

type sqlRunner interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
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

func (s *SQLiteStore) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *SQLiteStore) ApplySeedState(ctx context.Context, state SeedState, objects ObjectStore) error {
	var written []core.ObjectMetadata
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		existingRoles, err := listRoles(ctx, tx)
		if err != nil {
			return err
		}
		existingKeys, err := listAccessKeys(ctx, tx)
		if err != nil {
			return err
		}
		existingSAs, err := listServiceAccounts(ctx, tx)
		if err != nil {
			return err
		}

		desiredRoles := map[string]struct{}{}
		desiredKeys := map[string]struct{}{}
		desiredTokens := map[string]struct{}{}

		for _, bucket := range state.Buckets {
			if err := createBucket(ctx, tx, bucket); err != nil && !errors.Is(err, core.ErrConflict) {
				return err
			}
		}
		for _, key := range state.AccessKeys {
			desiredKeys[key.ID] = struct{}{}
			if err := upsertAccessKey(ctx, tx, key); err != nil {
				return err
			}
		}
		for _, role := range state.Roles {
			desiredRoles[role.Name] = struct{}{}
			if err := upsertRole(ctx, tx, role); err != nil {
				return err
			}
		}
		for _, sa := range state.ServiceAccounts {
			desiredTokens[sa.Token] = struct{}{}
			if err := upsertServiceAccount(ctx, tx, sa); err != nil {
				return err
			}
		}
		for _, key := range existingKeys {
			if _, ok := desiredKeys[key.ID]; ok {
				continue
			}
			if err := deleteAccessKey(ctx, tx, key.ID); err != nil && !errors.Is(err, core.ErrNotFound) {
				return err
			}
		}
		for _, sa := range existingSAs {
			if _, ok := desiredTokens[sa.Token]; ok {
				continue
			}
			if err := deleteServiceAccount(ctx, tx, sa.Token); err != nil && !errors.Is(err, core.ErrNotFound) {
				return err
			}
		}
		for _, name := range existingRoles {
			if _, ok := desiredRoles[name]; ok {
				continue
			}
			if err := deleteRole(ctx, tx, name); err != nil && !errors.Is(err, core.ErrNotFound) {
				return err
			}
		}
		for _, object := range state.Objects {
			meta, err := objects.PutObject(ctx, object.Bucket, object.Key, strings.NewReader(object.Content))
			if err != nil {
				return err
			}
			written = append(written, meta)
			if err := putObject(ctx, tx, meta); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		for _, meta := range written {
			_ = objects.DeleteObject(ctx, meta.Bucket, meta.Key)
		}
		return err
	}
	return nil
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
		`CREATE TABLE IF NOT EXISTS access_keys (id TEXT PRIMARY KEY, secret TEXT NOT NULL, allowed_roles_json TEXT NOT NULL DEFAULT '[]', created_at TIMESTAMP NOT NULL);`,
		`CREATE TABLE IF NOT EXISTS roles (name TEXT PRIMARY KEY, created_at TIMESTAMP NOT NULL);`,
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
		`CREATE TABLE IF NOT EXISTS multipart_uploads (
			upload_id TEXT PRIMARY KEY,
			bucket TEXT NOT NULL,
			key TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_multipart_uploads_bucket_key ON multipart_uploads(bucket, key);`,
		`CREATE TABLE IF NOT EXISTS multipart_parts (
			upload_id TEXT NOT NULL,
			part_number INTEGER NOT NULL,
			etag TEXT NOT NULL,
			size INTEGER NOT NULL,
			path TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL,
			PRIMARY KEY (upload_id, part_number)
		);`,
		`CREATE TABLE IF NOT EXISTS service_accounts (
			token TEXT PRIMARY KEY,
			client_email TEXT NOT NULL,
			principal_name TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL
		);`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return fmt.Errorf("init schema: %w", err)
		}
	}
	return s.migrateSchema()
}

func (s *SQLiteStore) migrateSchema() error {
	if err := s.migrateAccessKeys(); err != nil {
		return err
	}
	if err := s.dropRedundantIndexes(); err != nil {
		return err
	}
	return nil
}

func (s *SQLiteStore) migrateAccessKeys() error {
	columns, err := tableColumns(s.db, "access_keys")
	if err != nil {
		return err
	}
	_, hasAllowedRoles := columns["allowed_roles_json"]
	_, hasPrincipalName := columns["principal_name"]
	if !hasAllowedRoles {
		if _, err := s.db.Exec(`ALTER TABLE access_keys ADD COLUMN allowed_roles_json TEXT NOT NULL DEFAULT '[]'`); err != nil {
			return fmt.Errorf("migrate access_keys: %w", err)
		}
		hasAllowedRoles = true
	}
	if hasPrincipalName {
		if err := s.rebuildAccessKeysTable(hasAllowedRoles); err != nil {
			return fmt.Errorf("migrate access_keys: %w", err)
		}
	}
	return nil
}

func (s *SQLiteStore) rebuildAccessKeysTable(hasAllowedRoles bool) error {
	return s.withTx(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec(`
			CREATE TABLE access_keys_new (
				id TEXT PRIMARY KEY,
				secret TEXT NOT NULL,
				allowed_roles_json TEXT NOT NULL DEFAULT '[]',
				created_at TIMESTAMP NOT NULL
			)`); err != nil {
			return err
		}
		insertStmt := `
			INSERT INTO access_keys_new(id, secret, allowed_roles_json, created_at)
			SELECT id, secret, '[]', created_at
			FROM access_keys`
		if hasAllowedRoles {
			insertStmt = `
				INSERT INTO access_keys_new(id, secret, allowed_roles_json, created_at)
				SELECT id, secret, COALESCE(NULLIF(TRIM(allowed_roles_json), ''), '[]'), created_at
				FROM access_keys`
		}
		if _, err := tx.Exec(insertStmt); err != nil {
			return err
		}
		if _, err := tx.Exec(`DROP TABLE access_keys`); err != nil {
			return err
		}
		if _, err := tx.Exec(`ALTER TABLE access_keys_new RENAME TO access_keys`); err != nil {
			return err
		}
		return nil
	})
}

func (s *SQLiteStore) dropRedundantIndexes() error {
	statements := []string{
		`DROP INDEX IF EXISTS idx_objects_bucket_key`,
		`DROP INDEX IF EXISTS idx_multipart_parts_upload_id`,
	}
	for _, statement := range statements {
		if _, err := s.db.Exec(statement); err != nil {
			return err
		}
	}
	return nil
}

func tableColumns(db *sql.DB, table string) (map[string]struct{}, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := map[string]struct{}{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull, pk int
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		columns[name] = struct{}{}
	}
	return columns, rows.Err()
}

func (s *SQLiteStore) EnsureBucket(ctx context.Context, name string) error {
	return ensureBucket(ctx, s.db, name)
}

func (s *SQLiteStore) CreateBucket(ctx context.Context, name string) error {
	return createBucket(ctx, s.db, name)
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
	return putObject(ctx, s.db, meta)
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
	escapedPrefix := escapeLike(prefix)
	rows, err := s.db.QueryContext(ctx, `
		SELECT bucket, key, path, size, etag, created_at, modified_at
		FROM objects
		WHERE bucket = ? AND key LIKE ? ESCAPE '!' AND key > ?
		ORDER BY key
		LIMIT ?`, bucket, escapedPrefix+`%`, after, limit)
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

func (s *SQLiteStore) UpsertRole(ctx context.Context, role core.Role) error {
	return upsertRole(ctx, s.db, role)
}

func (s *SQLiteStore) ListRoles(ctx context.Context) ([]string, error) {
	return listRoles(ctx, s.db)
}

func (s *SQLiteStore) ListAccessKeys(ctx context.Context) ([]core.AccessKey, error) {
	return listAccessKeys(ctx, s.db)
}

func (s *SQLiteStore) FindAccessKey(ctx context.Context, accessKeyID string) (core.AccessKey, error) {
	var key core.AccessKey
	var rolesJSON string
	row := s.db.QueryRowContext(ctx, `
		SELECT ak.id, ak.secret, ak.allowed_roles_json, ak.created_at
		FROM access_keys ak
		WHERE ak.id = ?`, accessKeyID)
	if err := row.Scan(&key.ID, &key.Secret, &rolesJSON, &key.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return core.AccessKey{}, core.ErrNotFound
		}
		return core.AccessKey{}, err
	}
	if err := json.Unmarshal([]byte(rolesJSON), &key.AllowedRoles); err != nil {
		return core.AccessKey{}, fmt.Errorf("parse allowed_roles: %w", err)
	}
	return key, nil
}

func (s *SQLiteStore) GetRole(ctx context.Context, name string) (core.Role, error) {
	var role core.Role
	row := s.db.QueryRowContext(ctx, `SELECT name FROM roles WHERE name = ?`, name)
	if err := row.Scan(&role.Name); err != nil {
		if err == sql.ErrNoRows {
			return core.Role{}, core.ErrNotFound
		}
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
	if isUniqueConstraint(err) {
		return core.ErrConflict
	}
	return err
}

func (s *SQLiteStore) GetSession(ctx context.Context, token string) (core.Session, error) {
	var session core.Session
	err := s.db.QueryRowContext(ctx, `
		SELECT s.token, s.access_key_id, s.secret_key, s.principal_name, s.role_name, s.session_name, s.expires_at, s.created_at
		FROM sessions s
		WHERE s.token = ?`, token).Scan(&session.Token, &session.AccessKeyID, &session.SecretKey, &session.PrincipalName, &session.RoleName, &session.SessionName, &session.ExpiresAt, &session.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return core.Session{}, core.ErrNotFound
		}
		return core.Session{}, err
	}
	return session, nil
}

func (s *SQLiteStore) DeleteExpiredSessions(ctx context.Context, now time.Time) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE expires_at <= ?`, now)
	return err
}

func (s *SQLiteStore) CreateMultipartUpload(ctx context.Context, upload core.MultipartUpload) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO multipart_uploads(upload_id, bucket, key, created_at)
		VALUES(?, ?, ?, ?)`,
		upload.UploadID, upload.Bucket, upload.Key, upload.CreatedAt,
	)
	if isUniqueConstraint(err) {
		return core.ErrConflict
	}
	return err
}

func (s *SQLiteStore) GetMultipartUpload(ctx context.Context, uploadID string) (core.MultipartUpload, error) {
	var upload core.MultipartUpload
	row := s.db.QueryRowContext(ctx, `
		SELECT upload_id, bucket, key, created_at
		FROM multipart_uploads
		WHERE upload_id = ?`, uploadID)
	if err := row.Scan(&upload.UploadID, &upload.Bucket, &upload.Key, &upload.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return core.MultipartUpload{}, core.ErrNotFound
		}
		return core.MultipartUpload{}, err
	}
	return upload, nil
}

func (s *SQLiteStore) PutMultipartPart(ctx context.Context, part core.MultipartPart) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO multipart_parts(upload_id, part_number, etag, size, path, created_at)
		VALUES(?, ?, ?, ?, ?, ?)
		ON CONFLICT(upload_id, part_number) DO UPDATE SET
			etag = excluded.etag,
			size = excluded.size,
			path = excluded.path,
			created_at = excluded.created_at`,
		part.UploadID, part.PartNumber, part.ETag, part.Size, part.Path, part.CreatedAt,
	)
	return err
}

func (s *SQLiteStore) ListMultipartParts(ctx context.Context, uploadID string) ([]core.MultipartPart, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT upload_id, part_number, etag, size, path, created_at
		FROM multipart_parts
		WHERE upload_id = ?
		ORDER BY part_number`, uploadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var parts []core.MultipartPart
	for rows.Next() {
		var part core.MultipartPart
		if err := rows.Scan(&part.UploadID, &part.PartNumber, &part.ETag, &part.Size, &part.Path, &part.CreatedAt); err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	return parts, rows.Err()
}

func (s *SQLiteStore) DeleteMultipartUpload(ctx context.Context, uploadID string) error {
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM multipart_uploads WHERE upload_id = ?`, uploadID); err != nil {
			return err
		}
		return nil
	})
}

func (s *SQLiteStore) UpsertServiceAccount(ctx context.Context, sa core.ServiceAccount) error {
	return upsertServiceAccount(ctx, s.db, sa)
}

func (s *SQLiteStore) FindServiceAccountByToken(ctx context.Context, token string) (core.ServiceAccount, error) {
	var sa core.ServiceAccount
	row := s.db.QueryRowContext(ctx, `
		SELECT sa.token, sa.client_email, sa.principal_name
		FROM service_accounts sa
		WHERE sa.token = ?`, token)
	if err := row.Scan(&sa.Token, &sa.ClientEmail, &sa.Principal); err != nil {
		if err == sql.ErrNoRows {
			return core.ServiceAccount{}, core.ErrNotFound
		}
		return core.ServiceAccount{}, err
	}
	return sa, nil
}

func (s *SQLiteStore) FindServiceAccountByEmail(ctx context.Context, email string) (core.ServiceAccount, error) {
	var sa core.ServiceAccount
	row := s.db.QueryRowContext(ctx, `
		SELECT token, client_email, principal_name
		FROM service_accounts
		WHERE client_email = ?`, email)
	if err := row.Scan(&sa.Token, &sa.ClientEmail, &sa.Principal); err != nil {
		if err == sql.ErrNoRows {
			return core.ServiceAccount{}, core.ErrNotFound
		}
		return core.ServiceAccount{}, err
	}
	return sa, nil
}

func (s *SQLiteStore) ListServiceAccounts(ctx context.Context) ([]core.ServiceAccount, error) {
	return listServiceAccounts(ctx, s.db)
}

func (s *SQLiteStore) DeleteServiceAccounts(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM service_accounts`)
	return err
}

func isUniqueConstraint(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "unique")
}

func escapeLike(input string) string {
	replacer := strings.NewReplacer(
		`!`, `!!`,
		`%`, `!%`,
		`_`, `!_`,
	)
	return replacer.Replace(input)
}

func ensureBucket(ctx context.Context, runner sqlRunner, name string) error {
	_, err := runner.ExecContext(ctx, `INSERT INTO buckets(name, created_at) VALUES(?, ?) ON CONFLICT(name) DO NOTHING`, name, time.Now().UTC())
	return err
}

func createBucket(ctx context.Context, runner sqlRunner, name string) error {
	res, err := runner.ExecContext(ctx, `INSERT INTO buckets(name, created_at) VALUES(?, ?) ON CONFLICT(name) DO NOTHING`, name, time.Now().UTC())
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return core.ErrConflict
	}
	return nil
}

func putObject(ctx context.Context, runner sqlRunner, meta core.ObjectMetadata) error {
	_, err := runner.ExecContext(ctx, `
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

func upsertAccessKey(ctx context.Context, runner sqlRunner, key SeedAccessKey) error {
	rolesJSON, err := json.Marshal(key.AllowedRoles)
	if err != nil {
		return err
	}
	_, err = runner.ExecContext(ctx, `
		INSERT INTO access_keys(id, secret, allowed_roles_json, created_at) VALUES(?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET secret = excluded.secret, allowed_roles_json = excluded.allowed_roles_json`,
		key.ID, key.Secret, string(rolesJSON), time.Now().UTC(),
	)
	return err
}

func upsertRole(ctx context.Context, runner sqlRunner, role core.Role) error {
	_, err := runner.ExecContext(ctx, `
		INSERT INTO roles(name, created_at) VALUES(?, ?)
		ON CONFLICT(name) DO NOTHING`,
		role.Name, time.Now().UTC(),
	)
	return err
}

func listRoles(ctx context.Context, runner sqlRunner) ([]string, error) {
	rows, err := runner.QueryContext(ctx, `SELECT name FROM roles ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, rows.Err()
}

func listAccessKeys(ctx context.Context, runner sqlRunner) ([]core.AccessKey, error) {
	rows, err := runner.QueryContext(ctx, `SELECT id, secret, allowed_roles_json, created_at FROM access_keys ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []core.AccessKey
	for rows.Next() {
		var key core.AccessKey
		var rolesJSON string
		if err := rows.Scan(&key.ID, &key.Secret, &rolesJSON, &key.CreatedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(rolesJSON), &key.AllowedRoles); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func deleteAccessKey(ctx context.Context, runner sqlRunner, accessKeyID string) error {
	res, err := runner.ExecContext(ctx, `DELETE FROM access_keys WHERE id = ?`, accessKeyID)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return core.ErrNotFound
	}
	return nil
}

func deleteRole(ctx context.Context, runner sqlRunner, name string) error {
	if _, err := runner.ExecContext(ctx, `DELETE FROM sessions WHERE role_name = ?`, name); err != nil {
		return err
	}
	res, err := runner.ExecContext(ctx, `DELETE FROM roles WHERE name = ?`, name)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return core.ErrNotFound
	}
	return nil
}

func upsertServiceAccount(ctx context.Context, runner sqlRunner, sa core.ServiceAccount) error {
	_, err := runner.ExecContext(ctx, `
		INSERT INTO service_accounts(token, client_email, principal_name, created_at) VALUES(?, ?, ?, ?)
		ON CONFLICT(token) DO UPDATE SET client_email = excluded.client_email, principal_name = excluded.principal_name`,
		sa.Token, sa.ClientEmail, sa.Principal, time.Now().UTC(),
	)
	return err
}

func deleteServiceAccount(ctx context.Context, runner sqlRunner, token string) error {
	res, err := runner.ExecContext(ctx, `DELETE FROM service_accounts WHERE token = ?`, token)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return core.ErrNotFound
	}
	return nil
}

func listServiceAccounts(ctx context.Context, runner sqlRunner) ([]core.ServiceAccount, error) {
	rows, err := runner.QueryContext(ctx, `SELECT token, client_email, principal_name FROM service_accounts ORDER BY token`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sas []core.ServiceAccount
	for rows.Next() {
		var sa core.ServiceAccount
		if err := rows.Scan(&sa.Token, &sa.ClientEmail, &sa.Principal); err != nil {
			return nil, err
		}
		sas = append(sas, sa)
	}
	return sas, rows.Err()
}
