package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	authaws "github.com/snithish/mockbucket/internal/auth/aws"
	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/frontends"
	"github.com/snithish/mockbucket/internal/frontends/common"
	"github.com/snithish/mockbucket/internal/httpx"
	"github.com/snithish/mockbucket/internal/iam"
	"github.com/snithish/mockbucket/internal/seed"
	"github.com/snithish/mockbucket/internal/storage"
)

type Runtime struct {
	Config        config.Config
	Logger        *slog.Logger
	HTTPServer    *http.Server
	Metadata      *storage.SQLiteStore
	Objects       *storage.FilesystemObjectStore
	Authenticator iam.Resolver
}

func New(ctx context.Context, cfg config.Config, logger *slog.Logger) (*Runtime, error) {
	if err := frontends.Validate(cfg); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Storage.SQLitePath), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite dir: %w", err)
	}
	objects, err := storage.NewFilesystemObjectStore(cfg.Storage.RootDir)
	if err != nil {
		return nil, err
	}
	metadata, err := storage.OpenSQLite(cfg.Storage.SQLitePath)
	if err != nil {
		return nil, err
	}
	doc, err := seed.Load(cfg.Seed.Path)
	if err != nil {
		_ = metadata.Close()
		return nil, err
	}
	if err := seed.Apply(ctx, doc, metadata, objects); err != nil {
		_ = metadata.Close()
		return nil, fmt.Errorf("apply seed: %w", err)
	}
	authResolver := iam.Resolver{Store: metadata, SessionManager: iam.SessionManager{Store: metadata, TrustEvaluator: iam.TrustEvaluator{}, DefaultDuration: cfg.Auth.SessionDuration}}
	deps := common.Dependencies{
		Metadata:       metadata,
		Objects:        objects,
		AuthResolver:   authResolver,
		Policy:         iam.Evaluator{},
		SessionManager: authResolver.SessionManager,
		AWSVerifier:    authaws.Verifier{},
	}
	mux := http.NewServeMux()
	registerHealth(mux, metadata)
	frontends.Register(mux, cfg, deps)
	handler := httpx.RequestID(httpx.RequestLog(logger, cfg.Server.RequestLog, mux))
	return &Runtime{
		Config:        cfg,
		Logger:        logger,
		Metadata:      metadata,
		Objects:       objects,
		Authenticator: authResolver,
		HTTPServer: &http.Server{
			Addr:              cfg.Server.Address,
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}, nil
}

func (r *Runtime) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		r.Logger.Info("mockbucket listening", slog.String("address", r.HTTPServer.Addr))
		if err := r.HTTPServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), r.Config.Server.ShutdownTimeout)
		defer cancel()
		_ = r.Metadata.DeleteExpiredSessions(context.Background(), time.Now().UTC())
		if err := r.HTTPServer.Shutdown(shutdownCtx); err != nil {
			return err
		}
		return nil
	}
}

func (r *Runtime) Close() error {
	return r.Metadata.Close()
}

func registerHealth(mux *http.ServeMux, metadata storage.MetadataStore) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := metadata.Ping(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
}
