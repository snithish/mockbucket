package seed

import (
	"context"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/storage"
)

func Apply(ctx context.Context, doc Document, metadata *storage.SQLiteStore, objects storage.ObjectStore) error {
	state := storage.SeedState{
		Buckets:    append([]string(nil), doc.Buckets...),
		Principals: append([]core.Principal(nil), doc.Principals...),
		Roles:      append([]core.Role(nil), doc.Roles...),
		Objects:    make([]storage.SeedObject, 0, len(doc.Objects)),
	}
	for _, object := range doc.Objects {
		state.Objects = append(state.Objects, storage.SeedObject{
			Bucket:  object.Bucket,
			Key:     object.Key,
			Content: object.Content,
		})
	}
	for _, key := range doc.S3.AccessKeys {
		state.AccessKeys = append(state.AccessKeys, storage.SeedAccessKey{
			ID:        key.ID,
			Secret:    key.Secret,
			Principal: key.Principal,
		})
	}
	state.ServiceAccounts = append([]core.ServiceAccount(nil), doc.GCS.Accounts...)
	return metadata.ApplySeedState(ctx, state, objects)
}
