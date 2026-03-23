package iam

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/storage"
)

func TestEvaluatorHonorsExplicitDeny(t *testing.T) {
	evaluator := Evaluator{}
	policies := []core.PolicyDocument{{Statements: []core.PolicyStatement{{Effect: core.EffectAllow, Actions: []string{"s3:*"}, Resources: []string{"*"}}, {Effect: core.EffectDeny, Actions: []string{"s3:DeleteObject"}, Resources: []string{"*"}}}}}
	if evaluator.Allowed("s3:GetObject", "arn:mockbucket:s3:::demo/key", policies) != true {
		t.Fatal("Allowed(get) = false, want true")
	}
	if evaluator.Allowed("s3:DeleteObject", "arn:mockbucket:s3:::demo/key", policies) != false {
		t.Fatal("Allowed(delete) = true, want false")
	}
}

func TestSessionManagerAssumeRole(t *testing.T) {
	ctx := context.Background()
	store, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.UpsertRole(ctx, core.Role{Name: "reader", Trust: core.TrustPolicyDocument{Statements: []core.TrustStatement{{Effect: core.EffectAllow, Principals: []string{"admin"}, Actions: []string{"sts:AssumeRole"}}}}, Policies: []core.PolicyDocument{{Statements: []core.PolicyStatement{{Effect: core.EffectAllow, Actions: []string{"s3:GetObject"}, Resources: []string{"*"}}}}}}); err != nil {
		t.Fatalf("UpsertRole() error = %v", err)
	}
	manager := SessionManager{Store: store, TrustEvaluator: TrustEvaluator{}, DefaultDuration: time.Hour}
	session, err := manager.AssumeRole(ctx, "admin", "reader", "cli")
	if err != nil {
		t.Fatalf("AssumeRole() error = %v", err)
	}
	subject, err := manager.ResolveSession(ctx, session.Token)
	if err != nil {
		t.Fatalf("ResolveSession() error = %v", err)
	}
	if subject.RoleName != "reader" {
		t.Fatalf("role name = %q, want reader", subject.RoleName)
	}
}
