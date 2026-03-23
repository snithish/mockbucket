package core

import "errors"

var (
	ErrNotFound          = errors.New("not found")
	ErrAccessDenied      = errors.New("access denied")
	ErrInvalidArgument   = errors.New("invalid argument")
	ErrSignatureMismatch = errors.New("signature mismatch")
	ErrExpiredToken      = errors.New("expired token")
	ErrConflict          = errors.New("conflict")
	ErrUnsupported       = errors.New("unsupported")
	ErrUnauthenticated   = errors.New("unauthenticated")
)
