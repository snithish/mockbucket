package seed

import (
	"testing"

	"github.com/snithish/mockbucket/internal/core"
)

func TestValidateRejectsUnknownReferences(t *testing.T) {
	doc := Document{
		Buckets:    []string{"demo"},
		Principals: []core.Principal{{Name: "admin"}},
		Roles: []core.Role{{
			Name: "reader",
			Trust: core.TrustPolicyDocument{Statements: []core.TrustStatement{{
				Effect:     core.EffectAllow,
				Principals: []string{"missing"},
				Actions:    []string{"sts:AssumeRole"},
			}}},
		}},
		Objects: []ObjectSeed{{Bucket: "missing", Key: "object.txt", Content: "x"}},
	}
	if err := doc.Validate(); err == nil {
		t.Fatal("Validate() error = nil, want error")
	}
}
