package seed

import (
	"bytes"
	"context"

	"github.com/snithish/mockbucket/internal/storage"
)

type ApplyTarget interface {
	storage.MetadataStore
}

func Apply(ctx context.Context, doc Document, metadata ApplyTarget, objects storage.ObjectStore) error {
	for _, bucket := range doc.Buckets {
		if err := metadata.EnsureBucket(ctx, bucket); err != nil {
			return err
		}
	}
	for _, principal := range doc.Principals {
		if err := metadata.UpsertPrincipal(ctx, principal); err != nil {
			return err
		}
	}
	for _, role := range doc.Roles {
		if err := metadata.UpsertRole(ctx, role); err != nil {
			return err
		}
	}
	for _, object := range doc.Objects {
		meta, err := objects.PutObject(ctx, object.Bucket, object.Key, bytes.NewBufferString(object.Content))
		if err != nil {
			return err
		}
		if err := metadata.PutObject(ctx, meta); err != nil {
			return err
		}
	}
	return nil
}
