package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	configPath := flag.String("config", "", "path to hub JSON config")
	flag.Parse()
	if *configPath == "" {
		slog.Error("missing -config")
		os.Exit(2)
	}
	cfg, err := loadConfig(*configPath)
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	store, err := openStore(cfg.DBPath)
	if err != nil {
		slog.Error("database", "err", err)
		os.Exit(1)
	}
	defer store.Close()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	eng := NewEngine(store, cfg, nil)
	srv := NewServer(eng, log)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	log.Info("listening", "addr", cfg.Listen, "room", cfg.RoomID)
	if err := srv.Run(ctx, cfg.Listen); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}
