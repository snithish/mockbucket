package seed

import (
	"context"
	"fmt"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/storage"
)

func Apply(ctx context.Context, doc Document, metadata *storage.SQLiteStore, objects storage.ObjectStore) error {
	state := storage.SeedState{
		Buckets: append([]string(nil), doc.Buckets...),
		Roles:   make([]core.Role, 0, len(doc.Roles)),
		Objects: make([]storage.SeedObject, 0, len(doc.Objects)),
	}
	for _, r := range doc.Roles {
		state.Roles = append(state.Roles, core.Role{Name: r.Name})
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
			ID:           key.ID,
			Secret:       key.Secret,
			AllowedRoles: key.AllowedRoles,
		})
	}
	for _, t := range doc.GCS.Tokens {
		state.ServiceAccounts = append(state.ServiceAccounts, core.ServiceAccount{
			Principal: t.Principal,
			Token:     t.Token,
		})
	}
	for _, sc := range doc.GCS.ServiceCredentials {
		state.ServiceAccounts = append(state.ServiceAccounts, core.ServiceAccount{
			ClientEmail: sc.ClientEmail,
			Principal:   sc.Principal,
			Token:       fmt.Sprintf("jwt:%s", sc.ClientEmail),
		})
	}
	return metadata.ApplySeedState(ctx, state, objects)
}
