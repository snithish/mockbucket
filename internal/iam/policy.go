package iam

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/storage"
)

type Evaluator struct{}

func (Evaluator) Allowed(action, resource string, policies []core.PolicyDocument) bool {
	decision := false
	for _, doc := range policies {
		for _, statement := range doc.Statements {
			if !matchesAny(action, statement.Actions) || !matchesAny(resource, statement.Resources) {
				continue
			}
			if statement.Effect == core.EffectDeny {
				return false
			}
			if statement.Effect == core.EffectAllow {
				decision = true
			}
		}
	}
	return decision
}

type TrustEvaluator struct{}

func (TrustEvaluator) Allows(principalName, action string, trust core.TrustPolicyDocument) bool {
	decision := false
	for _, statement := range trust.Statements {
		if !matchesAny(principalName, statement.Principals) || !matchesAny(action, statement.Actions) {
			continue
		}
		if statement.Effect == core.EffectDeny {
			return false
		}
		if statement.Effect == core.EffectAllow {
			decision = true
		}
	}
	return decision
}

type SessionManager struct {
	Store           storage.MetadataStore
	TrustEvaluator  TrustEvaluator
	DefaultDuration time.Duration
}

func (m SessionManager) AssumeRole(ctx context.Context, principalName, roleName, sessionName string) (core.Session, error) {
	role, err := m.Store.GetRole(ctx, roleName)
	if err != nil {
		return core.Session{}, err
	}
	if !m.TrustEvaluator.Allows(principalName, "sts:AssumeRole", role.Trust) {
		return core.Session{}, core.ErrAccessDenied
	}
	token, err := randomHex(16)
	if err != nil {
		return core.Session{}, err
	}
	accessKeyID, err := randomHex(8)
	if err != nil {
		return core.Session{}, err
	}
	secretKey, err := randomHex(16)
	if err != nil {
		return core.Session{}, err
	}
	now := time.Now().UTC()
	session := core.Session{
		Token:         token,
		AccessKeyID:   accessKeyID,
		SecretKey:     secretKey,
		PrincipalName: principalName,
		RoleName:      roleName,
		SessionName:   sessionName,
		CreatedAt:     now,
		ExpiresAt:     now.Add(m.DefaultDuration),
	}
	if err := m.Store.CreateSession(ctx, session); err != nil {
		return core.Session{}, err
	}
	return session, nil
}

func (m SessionManager) ResolveSession(ctx context.Context, token string) (core.Subject, error) {
	session, policies, err := m.Store.GetSession(ctx, token)
	if err != nil {
		return core.Subject{}, err
	}
	if time.Now().UTC().After(session.ExpiresAt) {
		return core.Subject{}, core.ErrExpiredToken
	}
	return core.Subject{PrincipalName: session.PrincipalName, RoleName: session.RoleName, Policies: policies}, nil
}

type Resolver struct {
	Store          storage.MetadataStore
	SessionManager SessionManager
}

func (r Resolver) ResolveAccessKey(ctx context.Context, accessKeyID string) (core.Subject, error) {
	key, policies, err := r.Store.FindAccessKey(ctx, accessKeyID)
	if err != nil {
		return core.Subject{}, err
	}
	return core.Subject{PrincipalName: key.PrincipalName, Policies: policies}, nil
}

func (r Resolver) ResolveBearerToken(ctx context.Context, token string) (core.Subject, error) {
	// Try static service account token first.
	sa, policies, err := r.Store.FindServiceAccountByToken(ctx, token)
	if err == nil {
		return core.Subject{PrincipalName: sa.Principal, Policies: policies}, nil
	}
	if !errors.Is(err, core.ErrNotFound) {
		return core.Subject{}, err
	}
	// Fall back to session-based tokens.
	return r.SessionManager.ResolveSession(ctx, token)
}

func (r Resolver) ResolveAWSCredential(ctx context.Context, accessKeyID, sessionToken string) (core.CredentialIdentity, error) {
	if sessionToken != "" {
		session, policies, err := r.Store.GetSession(ctx, sessionToken)
		if err != nil {
			return core.CredentialIdentity{}, err
		}
		if session.AccessKeyID != accessKeyID {
			return core.CredentialIdentity{}, core.ErrAccessDenied
		}
		if time.Now().UTC().After(session.ExpiresAt) {
			return core.CredentialIdentity{}, core.ErrExpiredToken
		}
		subject := core.Subject{PrincipalName: session.PrincipalName, RoleName: session.RoleName, Policies: policies}
		return core.CredentialIdentity{
			AccessKeyID:   session.AccessKeyID,
			SecretKey:     session.SecretKey,
			SessionToken:  session.Token,
			PrincipalName: session.PrincipalName,
			Subject:       subject,
		}, nil
	}

	key, policies, err := r.Store.FindAccessKey(ctx, accessKeyID)
	if err != nil {
		return core.CredentialIdentity{}, err
	}
	subject := core.Subject{PrincipalName: key.PrincipalName, Policies: policies}
	return core.CredentialIdentity{
		AccessKeyID:   key.ID,
		SecretKey:     key.Secret,
		PrincipalName: key.PrincipalName,
		Subject:       subject,
	}, nil
}

func matchesAny(value string, patterns []string) bool {
	for _, pattern := range patterns {
		if wildcardMatch(value, pattern) {
			return true
		}
	}
	return false
}

func wildcardMatch(value, pattern string) bool {
	if pattern == "*" {
		return true
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return value == pattern
	}
	if !strings.HasPrefix(value, parts[0]) {
		return false
	}
	value = value[len(parts[0]):]
	for i := 1; i < len(parts)-1; i++ {
		idx := strings.Index(value, parts[i])
		if idx < 0 {
			return false
		}
		value = value[idx+len(parts[i]):]
	}
	return strings.HasSuffix(value, parts[len(parts)-1])
}

func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
