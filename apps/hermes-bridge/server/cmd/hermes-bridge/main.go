// Command hermes-bridge serves the Hermes WebSocket bridge on a loopback address.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"hermes-bridge/internal/bridge"
	"hermes-bridge/internal/config"
)

func main() {
	configPath := flag.String("config", "bridge.json", "path to the JSON configuration file")
	flag.Parse()
	if err := run(*configPath); err != nil {
		fmt.Fprintln(os.Stderr, "hermes-bridge:", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(cfg.LogLevel)); err != nil {
		return fmt.Errorf("logLevel: %w", err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	token, created, err := loadOrCreateToken(cfg.TokenFile)
	if err != nil {
		return err
	}
	if created {
		log.Info("token_created", "tokenFile", cfg.TokenFile)
	}
	if err := os.MkdirAll(cfg.Worker.Workdir, 0o700); err != nil {
		return fmt.Errorf("worker workdir: %w", err)
	}

	srv := bridge.New(cfg, token, log)
	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return err
	}
	httpSrv := &http.Server{Handler: srv.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go srv.RunReaper()
	if cfg.Preflight {
		go srv.Preflight()
	}

	served := make(chan error, 1)
	go func() { served <- httpSrv.Serve(ln) }()
	log.Info("listening", "addr", ln.Addr().String(), "pid", os.Getpid(),
		"maxSessions", cfg.MaxSessions, "maxConcurrentRequests", cfg.MaxConcurrentRequests,
		"requestTimeout", cfg.RequestTimeout.String(), "allowedOrigins", len(cfg.AllowedOrigins))

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sig:
		log.Info("signal_received", "signal", s.String())
	case err := <-served:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	signal.Stop(sig)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout.Duration)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	srv.Shutdown(ctx)
	return nil
}

// loadOrCreateToken reads the shared secret, creating a random one (mode 0600) on first start.
// A token file readable by group or others is refused.
func loadOrCreateToken(path string) ([]byte, bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, false, err
		}
		buf := make([]byte, 32)
		if _, err := rand.Read(buf); err != nil {
			return nil, false, err
		}
		tok := hex.EncodeToString(buf)
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, false, err
		}
		defer f.Close()
		if _, err := f.WriteString(tok + "\n"); err != nil {
			return nil, false, err
		}
		return []byte(tok), true, nil
	}
	if err != nil {
		return nil, false, err
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, false, fmt.Errorf("token file %s must not be accessible to group or others (chmod 600)", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	tok := strings.TrimSpace(string(raw))
	if len(tok) < 32 {
		return nil, false, fmt.Errorf("token file %s must hold at least 32 characters", path)
	}
	return []byte(tok), false, nil
}
