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
	storage.ServiceAccountStore
}

type SessionManager struct {
	Store           sessionStore
	DefaultDuration time.Duration
}

func (m SessionManager) AssumeRole(ctx context.Context, roleName, sessionName, accessKeyID string) (core.Session, error) {
	return m.AssumeRoleWithDuration(ctx, roleName, sessionName, accessKeyID, 0)
}

func (m SessionManager) AssumeRoleWithDuration(ctx context.Context, roleName, sessionName, accessKeyID string, requestedDuration time.Duration) (core.Session, error) {
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
	duration := clampDuration(requestedDuration, m.DefaultDuration, 15*time.Minute, 12*time.Hour)
	session := core.Session{
		Token:       token,
		AccessKeyID: accessKey,
		SecretKey:   secretKey,
		RoleName:    roleName,
		SessionName: sessionName,
		CreatedAt:   now,
		ExpiresAt:   now.Add(duration),
	}
	if err := m.Store.CreateSession(ctx, session); err != nil {
		return core.Session{}, err
	}
	return session, nil
}

func (m SessionManager) IssueTokenForPrincipal(ctx context.Context, principalName string) (core.Session, error) {
	return m.IssueTokenForPrincipalWithDuration(ctx, principalName, 0)
}

func (m SessionManager) IssueTokenForPrincipalWithDuration(ctx context.Context, principalName string, requestedDuration time.Duration) (core.Session, error) {
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
	duration := clampDuration(requestedDuration, m.DefaultDuration, 15*time.Minute, 36*time.Hour)
	session := core.Session{
		Token:         token,
		AccessKeyID:   accessKeyID,
		SecretKey:     secretKey,
		PrincipalName: principalName,
		CreatedAt:     now,
		ExpiresAt:     now.Add(duration),
	}
	if err := m.Store.CreateSession(ctx, session); err != nil {
		return core.Session{}, err
	}
	return session, nil
}

func (m SessionManager) IssueSessionToken(ctx context.Context, accessKeyID string, requestedDuration time.Duration) (core.Session, error) {
	if _, err := m.Store.FindAccessKey(ctx, accessKeyID); err != nil {
		return core.Session{}, err
	}
	return m.IssueTokenForPrincipalWithDuration(ctx, accessKeyID, requestedDuration)
}

func (m SessionManager) LookupSession(ctx context.Context, token string) (core.Session, error) {
	session, err := m.Store.GetSession(ctx, token)
	if err != nil {
		return core.Session{}, err
	}
	if time.Now().UTC().After(session.ExpiresAt) {
		return core.Session{}, core.ErrExpiredToken
	}
	return session, nil
}

func (m SessionManager) ResolveSession(ctx context.Context, token string) (core.Subject, error) {
	session, err := m.LookupSession(ctx, token)
	if err != nil {
		return core.Subject{}, err
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

func (r Resolver) ResolveSignedURL(ctx context.Context, clientEmail string) (core.Subject, error) {
	sa, err := r.Store.FindServiceAccountByEmail(ctx, clientEmail)
	if err != nil {
		return core.Subject{}, err
	}
	return core.Subject{PrincipalName: sa.Principal}, nil
}

func randomHex(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func clampDuration(requestedDuration, defaultDuration, minDuration, maxDuration time.Duration) time.Duration {
	duration := requestedDuration
	if duration <= 0 {
		duration = defaultDuration
	}
	if duration < minDuration {
		return minDuration
	}
	if duration > maxDuration {
		return maxDuration
	}
	return duration
}
