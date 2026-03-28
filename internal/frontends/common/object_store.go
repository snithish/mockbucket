package common

import (
	"context"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/storage"
)

func StoreObject(ctx context.Context, deps Dependencies, bucket, key string, src storage.ObjectSource) (core.ObjectMetadata, error) {
	meta, err := deps.Objects.PutObject(ctx, bucket, key, src)
	if err != nil {
		return core.ObjectMetadata{}, err
	}
	if err := CommitObject(ctx, deps, meta); err != nil {
		return core.ObjectMetadata{}, err
	}
	return meta, nil
}

func CommitObject(ctx context.Context, deps Dependencies, meta core.ObjectMetadata) error {
	if err := deps.Metadata.PutObject(ctx, meta); err != nil {
		_ = deps.Objects.DeleteObject(ctx, meta.Bucket, meta.Key)
		return err
	}
	return nil
}

func DeleteObjectIfExists(ctx context.Context, deps Dependencies, bucket, key string) error {
	if err := deps.Metadata.DeleteObject(ctx, bucket, key); err != nil && err != core.ErrNotFound {
		return err
	}
	if err := deps.Objects.DeleteObject(ctx, bucket, key); err != nil && err != core.ErrNotFound {
		return err
	}
	return nil
}
