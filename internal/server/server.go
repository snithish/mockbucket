package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/core"
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
	if err := cfg.Validate(); err != nil {
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
	if err := seed.Apply(ctx, cfg.Seed, metadata, objects); err != nil {
		_ = metadata.Close()
		return nil, fmt.Errorf("apply seed: %w", err)
	}
	authResolver := iam.Resolver{Store: metadata, SessionManager: iam.SessionManager{Store: metadata, DefaultDuration: cfg.Auth.SessionDuration}}

	var gcsServiceAccounts []seed.ServiceAccountJSON
	if cfg.Frontends.Type == config.FrontendGCS {
		host, port, err := parseServerAddress(cfg.Server.Address)
		if err != nil {
			return nil, fmt.Errorf("parse server address: %w", err)
		}
		for _, sc := range cfg.Seed.GCS.ServiceCredentials {
			sa, err := seed.GenerateServiceAccountJSON(host, port, sc.ClientEmail)
			if err != nil {
				return nil, fmt.Errorf("generate service account for %s: %w", sc.ClientEmail, err)
			}
			sa.Principal = sc.Principal
			gcsServiceAccounts = append(gcsServiceAccounts, sa)
			if err := metadata.UpsertServiceAccount(ctx, core.ServiceAccount{
				ClientEmail: sa.ClientEmail,
				Principal:   sc.Principal,
				Token:       fmt.Sprintf("jwt:%s", sa.ClientEmail),
			}); err != nil {
				return nil, fmt.Errorf("store service account: %w", err)
			}
		}
	}

	deps := common.Dependencies{
		Metadata:       metadata,
		Objects:        objects,
		AuthResolver:   authResolver,
		SessionManager: authResolver.SessionManager,
	}
	mux := http.NewServeMux()
	registerHealth(mux, cfg, metadata)
	frontends.Register(mux, cfg, deps, gcsServiceAccounts)
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
			r.Logger.Error("mockbucket listen failed", slog.String("address", r.HTTPServer.Addr), slog.Any("error", err))
			errCh <- fmt.Errorf("listen: %w", err)
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

func registerHealth(mux *http.ServeMux, cfg config.Config, metadata storage.HealthStore) {
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
	mux.HandleFunc("GET /readyz/details", func(w http.ResponseWriter, r *http.Request) {
		details := struct {
			OK       bool `json:"ok"`
			Metadata struct {
				Healthy bool   `json:"healthy"`
				Error   string `json:"error,omitempty"`
			} `json:"metadata"`
			Frontends struct {
				S3    bool `json:"s3"`
				STS   bool `json:"sts"`
				GCS   bool `json:"gcs"`
				Azure bool `json:"azure"`
			} `json:"frontends"`
			Seed struct {
				Buckets int `json:"buckets"`
			} `json:"seed"`
		}{
			OK: true,
		}
		if err := metadata.Ping(r.Context()); err != nil {
			details.OK = false
			details.Metadata.Healthy = false
			details.Metadata.Error = err.Error()
		} else {
			details.Metadata.Healthy = true
		}
		details.Frontends.S3 = cfg.Frontends.Type == config.FrontendS3
		details.Frontends.STS = cfg.Frontends.Type == config.FrontendS3
		details.Frontends.GCS = cfg.Frontends.Type == config.FrontendGCS
		details.Frontends.Azure = cfg.Frontends.Type == config.FrontendAzureBlob || cfg.Frontends.Type == config.FrontendAzureDataLake
		details.Seed.Buckets = len(cfg.Seed.Buckets)

		status := http.StatusOK
		if !details.OK {
			status = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(details)
	})
}

func parseServerAddress(addr string) (string, int, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("invalid address %q: %w", addr, err)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil {
		return "", 0, fmt.Errorf("invalid port: %w", err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host, portNum, nil
}
