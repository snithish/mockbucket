package common

import (
	authaws "github.com/snithish/mockbucket/internal/auth/aws"
	"github.com/snithish/mockbucket/internal/iam"
	"github.com/snithish/mockbucket/internal/storage"
)

type Dependencies struct {
	Metadata       storage.MetadataStore
	Objects        storage.ObjectStore
	AuthResolver   iam.Resolver
	Policy         iam.Evaluator
	SessionManager iam.SessionManager
	AWSVerifier    authaws.Verifier
}
