package common

import (
	"github.com/snithish/mockbucket/internal/iam"
	"github.com/snithish/mockbucket/internal/storage"
)

type Dependencies struct {
	Metadata       storage.FrontendMetadataStore
	Objects        storage.ObjectStore
	AuthResolver   iam.Resolver
	SessionManager iam.SessionManager
}
