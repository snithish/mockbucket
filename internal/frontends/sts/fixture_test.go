package sts

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/iam"
	"github.com/snithish/mockbucket/internal/storage"
)

type stsTestFixture struct {
	deps     common.Dependencies
	metadata *storage.SQLiteStore
}

func newSTSTestFixture(t *testing.T) stsTestFixture {
	t.Helper()
	metadata, err := storage.OpenSQLite(filepath.Join(t.TempDir(), "mockbucket.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = metadata.Close() })
	sessionManager := iam.SessionManager{
		Store:           metadata,
		DefaultDuration: time.Hour,
	}
	deps := common.Dependencies{
		SessionManager: sessionManager,
	}
	return stsTestFixture{
		deps:     deps,
		metadata: metadata,
	}
}
