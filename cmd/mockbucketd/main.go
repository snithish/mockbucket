package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/server"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	configPath := flag.String("config", "mockbucket.yaml", "path to config file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("mockbucketd %s (commit=%s, built=%s)\n", version, commit, date)
		os.Exit(0)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load config", slog.Any("error", err))
		os.Exit(1)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("mockbucketd starting", slog.String("version", version), slog.String("commit", commit))
	runtime, err := server.New(ctx, cfg, logger)
	if err != nil {
		logger.Error("bootstrap runtime", slog.Any("error", err))
		os.Exit(1)
	}
	defer runtime.Close()
	if err := runtime.Run(ctx); err != nil {
		logger.Error("run server", slog.Any("error", err))
		os.Exit(1)
	}
}
