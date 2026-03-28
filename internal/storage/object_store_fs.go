package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/snithish/mockbucket/internal/core"
)

type ObjectSource interface {
	io.Reader
}

type ObjectReader interface {
	io.ReadCloser
}

type FilesystemObjectStore struct {
	rootDir string
}

func NewFilesystemObjectStore(rootDir string) (*FilesystemObjectStore, error) {
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		return nil, fmt.Errorf("create root dir: %w", err)
	}
	return &FilesystemObjectStore{rootDir: rootDir}, nil
}

func (s *FilesystemObjectStore) PutObject(ctx context.Context, bucket, key string, src ObjectSource) (core.ObjectMetadata, error) {
	path, err := s.objectPath(bucket, key)
	if err != nil {
		return core.ObjectMetadata{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return core.ObjectMetadata{}, fmt.Errorf("create object dir: %w", err)
	}
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return core.ObjectMetadata{}, fmt.Errorf("create temp object: %w", err)
	}
	hash := md5.New()
	written, copyErr := io.Copy(file, io.TeeReader(&contextReader{ctx: ctx, r: src}, hash))
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return core.ObjectMetadata{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return core.ObjectMetadata{}, closeErr
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return core.ObjectMetadata{}, fmt.Errorf("rename temp object: %w", err)
	}
	now := time.Now().UTC()
	return core.ObjectMetadata{Bucket: bucket, Key: key, Path: path, Size: written, ETag: hex.EncodeToString(hash.Sum(nil)), CreatedAt: now, ModifiedAt: now}, nil
}

func (s *FilesystemObjectStore) OpenObject(ctx context.Context, bucket, key string) (ObjectReader, core.ObjectMetadata, error) {
	path, err := s.objectPath(bucket, key)
	if err != nil {
		return nil, core.ObjectMetadata{}, err
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, core.ObjectMetadata{}, core.ErrNotFound
		}
		return nil, core.ObjectMetadata{}, err
	}
	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, core.ObjectMetadata{}, err
	}
	return &ctxReadCloser{ctx: ctx, file: file}, core.ObjectMetadata{Bucket: bucket, Key: key, Path: path, Size: stat.Size(), ModifiedAt: stat.ModTime().UTC()}, nil
}

func (s *FilesystemObjectStore) DeleteObject(ctx context.Context, bucket, key string) error {
	path, err := s.objectPath(bucket, key)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			return core.ErrNotFound
		}
		return err
	}
	return nil
}

func (s *FilesystemObjectStore) PutMultipartPart(ctx context.Context, uploadID string, partNumber int, src ObjectSource) (core.MultipartPart, error) {
	path, err := s.multipartPartPath(uploadID, partNumber)
	if err != nil {
		return core.MultipartPart{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return core.MultipartPart{}, fmt.Errorf("create part dir: %w", err)
	}
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return core.MultipartPart{}, fmt.Errorf("create temp part: %w", err)
	}
	hash := md5.New()
	written, copyErr := io.Copy(file, io.TeeReader(&contextReader{ctx: ctx, r: src}, hash))
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return core.MultipartPart{}, copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return core.MultipartPart{}, closeErr
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return core.MultipartPart{}, fmt.Errorf("rename temp part: %w", err)
	}
	return core.MultipartPart{
		UploadID:   uploadID,
		PartNumber: partNumber,
		ETag:       hex.EncodeToString(hash.Sum(nil)),
		Size:       written,
		Path:       path,
		CreatedAt:  time.Now().UTC(),
	}, nil
}

func (s *FilesystemObjectStore) CompleteMultipartUpload(ctx context.Context, bucket, key string, parts []core.MultipartPart) (core.ObjectMetadata, error) {
	if len(parts) == 0 {
		return core.ObjectMetadata{}, core.ErrInvalidArgument
	}
	path, err := s.objectPath(bucket, key)
	if err != nil {
		return core.ObjectMetadata{}, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return core.ObjectMetadata{}, fmt.Errorf("create object dir: %w", err)
	}
	tmpPath := path + ".tmp"
	file, err := os.Create(tmpPath)
	if err != nil {
		return core.ObjectMetadata{}, fmt.Errorf("create temp object: %w", err)
	}
	var total int64
	for _, part := range parts {
		partFile, err := os.Open(part.Path)
		if err != nil {
			_ = file.Close()
			_ = os.Remove(tmpPath)
			return core.ObjectMetadata{}, err
		}
		written, copyErr := io.Copy(file, &contextReader{ctx: ctx, r: partFile})
		closeErr := partFile.Close()
		if copyErr != nil {
			_ = file.Close()
			_ = os.Remove(tmpPath)
			return core.ObjectMetadata{}, copyErr
		}
		if closeErr != nil {
			_ = file.Close()
			_ = os.Remove(tmpPath)
			return core.ObjectMetadata{}, closeErr
		}
		total += written
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return core.ObjectMetadata{}, err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return core.ObjectMetadata{}, fmt.Errorf("rename temp object: %w", err)
	}
	etag, err := multipartETag(parts)
	if err != nil {
		return core.ObjectMetadata{}, err
	}
	now := time.Now().UTC()
	return core.ObjectMetadata{
		Bucket:     bucket,
		Key:        key,
		Path:       path,
		ETag:       etag,
		Size:       total,
		CreatedAt:  now,
		ModifiedAt: now,
	}, nil
}

func (s *FilesystemObjectStore) AbortMultipartUpload(ctx context.Context, uploadID string) error {
	path, err := s.multipartUploadDir(uploadID)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(path); err != nil {
		if os.IsNotExist(err) {
			return core.ErrNotFound
		}
		return err
	}
	return nil
}

func multipartETag(parts []core.MultipartPart) (string, error) {
	if len(parts) == 1 {
		return parts[0].ETag, nil
	}
	concat := make([]byte, 0, 16*len(parts))
	for _, part := range parts {
		raw, err := hex.DecodeString(part.ETag)
		if err != nil {
			return "", err
		}
		concat = append(concat, raw...)
	}
	sum := md5.Sum(concat)
	return hex.EncodeToString(sum[:]) + "-" + strconv.Itoa(len(parts)), nil
}

func (s *FilesystemObjectStore) multipartUploadDir(uploadID string) (string, error) {
	if strings.TrimSpace(uploadID) == "" {
		return "", core.ErrInvalidArgument
	}
	if strings.Contains(uploadID, string(os.PathSeparator)) {
		return "", core.ErrInvalidArgument
	}
	path := filepath.Join(s.rootDir, ".multipart", uploadID)
	cleanRoot := filepath.Clean(s.rootDir) + string(os.PathSeparator)
	cleanPath := filepath.Clean(path)
	if !strings.HasPrefix(cleanPath+string(os.PathSeparator), cleanRoot) {
		return "", core.ErrInvalidArgument
	}
	return cleanPath, nil
}

func (s *FilesystemObjectStore) multipartPartPath(uploadID string, partNumber int) (string, error) {
	if partNumber <= 0 {
		return "", core.ErrInvalidArgument
	}
	dir, err := s.multipartUploadDir(uploadID)
	if err != nil {
		return "", err
	}
	name := fmt.Sprintf("%06d.part", partNumber)
	return filepath.Join(dir, name), nil
}

func (s *FilesystemObjectStore) objectPath(bucket, key string) (string, error) {
	if strings.TrimSpace(bucket) == "" || strings.TrimSpace(key) == "" {
		return "", core.ErrInvalidArgument
	}
	hasTrailingSlash := strings.HasSuffix(key, "/")
	segments := strings.Split(key, "/")
	encoded := make([]string, 0, len(segments)+1)
	encoded = append(encoded, url.PathEscape(bucket))
	for _, segment := range segments {
		if segment == "" {
			continue
		}
		if segment == "." || segment == ".." || !utf8.ValidString(segment) {
			return "", core.ErrInvalidArgument
		}
		encoded = append(encoded, url.PathEscape(segment))
	}
	if hasTrailingSlash {
		encoded = append(encoded, ".__mockbucket_dir__")
	}
	path := filepath.Join(append([]string{s.rootDir}, encoded...)...)
	cleanRoot := filepath.Clean(s.rootDir) + string(os.PathSeparator)
	cleanPath := filepath.Clean(path)
	if !strings.HasPrefix(cleanPath+string(os.PathSeparator), cleanRoot) && cleanPath != filepath.Clean(s.rootDir) {
		return "", core.ErrInvalidArgument
	}
	return cleanPath, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

type ctxReadCloser struct {
	ctx  context.Context
	file *os.File
}

func (r *ctxReadCloser) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.file.Read(p)
}

func (r *ctxReadCloser) Close() error {
	return r.file.Close()
}
