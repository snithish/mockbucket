package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/snithish/mockbucket/internal/config"
	"github.com/snithish/mockbucket/internal/server"
)

func main() {
	configPath := flag.String("config", "mockbucket.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load config", slog.Any("error", err))
		os.Exit(1)
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runtime, err := server.New(ctx, cfg, logger)
	if err != nil {
		logger.Error("bootstrap runtime", slog.Any("error", err))
		os.Exit(1)
	}
	defer func() { _ = runtime.Close() }()
	if err := runtime.Run(ctx); err != nil {
		logger.Error("run server", slog.Any("error", err))
		os.Exit(1)
	}
}
