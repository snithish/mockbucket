package iam

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"

	"github.com/snithish/mockbucket/internal/core"
	"github.com/snithish/mockbucket/internal/storage"
)

type sessionStore interface {
	storage.AccessKeyStore
	storage.RoleStore
	storage.SessionStateStore
}

type resolverStore interface {
	storage.ServiceAccountLookupStore
}

type SessionManager struct {
	Store           sessionStore
	DefaultDuration time.Duration
}

func (m SessionManager) AssumeRole(ctx context.Context, roleName, sessionName, accessKeyID string) (core.Session, error) {
	if _, err := m.Store.GetRole(ctx, roleName); err != nil {
		return core.Session{}, err
	}
	if accessKeyID != "" {
		key, err := m.Store.FindAccessKey(ctx, accessKeyID)
		if err == nil && len(key.AllowedRoles) > 0 {
			allowed := false
			for _, r := range key.AllowedRoles {
				if r == roleName {
					allowed = true
					break
				}
			}
			if !allowed {
				return core.Session{}, core.ErrAccessDenied
			}
		}
	}
	token, err := randomHex(16)
	if err != nil {
		return core.Session{}, err
	}
	accessKey, err := randomHex(8)
	if err != nil {
		return core.Session{}, err
	}
	secretKey, err := randomHex(16)
	if err != nil {
		return core.Session{}, err
	}
	now := time.Now().UTC()
	session := core.Session{
		Token:       token,
		AccessKeyID: accessKey,
		SecretKey:   secretKey,
		RoleName:    roleName,
		SessionName: sessionName,
		CreatedAt:   now,
		ExpiresAt:   now.Add(m.DefaultDuration),
	}
	if err := m.Store.CreateSession(ctx, session); err != nil {
		return core.Session{}, err
	}
	return session, nil
}

func (m SessionManager) IssueTokenForPrincipal(ctx context.Context, principalName string) (core.Session, error) {
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
		CreatedAt:     now,
		ExpiresAt:     now.Add(m.DefaultDuration),
	}
	if err := m.Store.CreateSession(ctx, session); err != nil {
		return core.Session{}, err
	}
	return session, nil
}

func (m SessionManager) ResolveSession(ctx context.Context, token string) (core.Subject, error) {
	session, err := m.Store.GetSession(ctx, token)
	if err != nil {
		return core.Subject{}, err
	}
	if time.Now().UTC().After(session.ExpiresAt) {
		return core.Subject{}, core.ErrExpiredToken
	}
	return core.Subject{PrincipalName: session.PrincipalName, RoleName: session.RoleName}, nil
}

type Resolver struct {
	Store          resolverStore
	SessionManager SessionManager
}

func (r Resolver) ResolveBearerToken(ctx context.Context, token string) (core.Subject, error) {
	sa, err := r.Store.FindServiceAccountByToken(ctx, token)
	if err == nil {
		return core.Subject{PrincipalName: sa.Principal}, nil
	}
	if !errors.Is(err, core.ErrNotFound) {
		return core.Subject{}, err
	}
	return r.SessionManager.ResolveSession(ctx, token)
}

func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
