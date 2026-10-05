// Package config loads the bridge's JSON configuration and applies defaults.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Duration is a time.Duration that reads Go duration strings ("90s", "15m") from JSON.
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"30s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

type Worker struct {
	// Command is the executable and fixed arguments; the bridge appends its managed flags.
	Command []string `json:"command"`
	// Workdir is the agent's working directory (kept empty of project files on purpose).
	Workdir string `json:"workdir"`
	// HermesHome overrides HERMES_HOME for the worker only; empty inherits the server's.
	HermesHome    string `json:"hermesHome"`
	MaxIterations int    `json:"maxIterations"`
	Memory        bool   `json:"memory"`
}

type Config struct {
	ListenAddr     string   `json:"listenAddr"`
	TokenFile      string   `json:"tokenFile"`
	AllowedOrigins []string `json:"allowedOrigins"`
	LogLevel       string   `json:"logLevel"`

	MaxMessageBytes       int64 `json:"maxMessageBytes"`
	MaxQuestionBytes      int   `json:"maxQuestionBytes"`
	MaxConnections        int   `json:"maxConnections"`
	MaxSessions           int   `json:"maxSessions"`
	MaxConcurrentRequests int   `json:"maxConcurrentRequests"`
	MaxTurnsPerSession    int   `json:"maxTurnsPerSession"`
	MaxHistoryBytes       int64 `json:"maxHistoryBytes"`
	MaxWorkerRecordBytes  int   `json:"maxWorkerRecordBytes"`

	RequestTimeout     Duration `json:"requestTimeout"`
	CancelGrace        Duration `json:"cancelGrace"`
	IdleSessionTimeout Duration `json:"idleSessionTimeout"`
	WorkerStartTimeout Duration `json:"workerStartTimeout"`
	WorkerStopGrace    Duration `json:"workerStopGrace"`
	ShutdownTimeout    Duration `json:"shutdownTimeout"`

	Preflight           bool `json:"preflight"`
	ForwardWorkerStderr bool `json:"forwardWorkerStderr"`

	Worker Worker `json:"worker"`
}

// Default returns the conservative defaults documented in README.md.
func Default() Config {
	return Config{
		ListenAddr:            "127.0.0.1:8765",
		TokenFile:             "state/token",
		AllowedOrigins:        []string{},
		LogLevel:              "info",
		MaxMessageBytes:       64 << 10,
		MaxQuestionBytes:      32 << 10,
		MaxConnections:        8,
		MaxSessions:           4,
		MaxConcurrentRequests: 2,
		MaxTurnsPerSession:    50,
		MaxHistoryBytes:       512 << 10,
		MaxWorkerRecordBytes:  4 << 20,
		RequestTimeout:        Duration{180 * time.Second},
		CancelGrace:           Duration{10 * time.Second},
		IdleSessionTimeout:    Duration{15 * time.Minute},
		WorkerStartTimeout:    Duration{90 * time.Second},
		WorkerStopGrace:       Duration{5 * time.Second},
		ShutdownTimeout:       Duration{20 * time.Second},
		Preflight:             true,
		Worker:                Worker{Workdir: "state/workdir", MaxIterations: 4},
	}
}

// Load reads path over the defaults. Relative paths inside the file resolve against its directory.
func Load(path string) (Config, error) {
	cfg := Default()
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("%s: %w", path, err)
	}
	base := filepath.Dir(path)
	cfg.TokenFile = resolve(base, cfg.TokenFile)
	cfg.Worker.Workdir = resolve(base, cfg.Worker.Workdir)
	if cfg.Worker.HermesHome != "" {
		cfg.Worker.HermesHome = resolve(base, cfg.Worker.HermesHome)
	}
	return cfg, cfg.Validate()
}

func resolve(base, p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(base, p)
}

// Validate rejects configurations that would weaken the documented security posture.
func (c Config) Validate() error {
	var errs []error
	host, _, err := net.SplitHostPort(c.ListenAddr)
	if err != nil {
		errs = append(errs, fmt.Errorf("listenAddr: %w", err))
	} else if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		errs = append(errs, fmt.Errorf("listenAddr %q: this milestone only binds loopback; use an SSH tunnel for remote access", c.ListenAddr))
	}
	for _, o := range c.AllowedOrigins {
		if err := checkOrigin(o); err != nil {
			errs = append(errs, err)
		}
	}
	if len(c.Worker.Command) == 0 {
		errs = append(errs, errors.New("worker.command must name the worker executable and script"))
	}
	if c.TokenFile == "" {
		errs = append(errs, errors.New("tokenFile is required"))
	}
	positive := map[string]int64{
		"maxMessageBytes": c.MaxMessageBytes, "maxQuestionBytes": int64(c.MaxQuestionBytes),
		"maxConnections": int64(c.MaxConnections), "maxSessions": int64(c.MaxSessions),
		"maxConcurrentRequests": int64(c.MaxConcurrentRequests), "maxTurnsPerSession": int64(c.MaxTurnsPerSession),
		"maxHistoryBytes": c.MaxHistoryBytes, "maxWorkerRecordBytes": int64(c.MaxWorkerRecordBytes),
		"worker.maxIterations": int64(c.Worker.MaxIterations),
		"requestTimeout": int64(c.RequestTimeout.Duration), "cancelGrace": int64(c.CancelGrace.Duration),
		"idleSessionTimeout": int64(c.IdleSessionTimeout.Duration), "workerStartTimeout": int64(c.WorkerStartTimeout.Duration),
		"workerStopGrace": int64(c.WorkerStopGrace.Duration), "shutdownTimeout": int64(c.ShutdownTimeout.Duration),
	}
	for name, v := range positive {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", name))
		}
	}
	if int64(c.MaxQuestionBytes) >= c.MaxMessageBytes {
		errs = append(errs, errors.New("maxQuestionBytes must be smaller than maxMessageBytes"))
	}
	return errors.Join(errs...)
}

// checkOrigin accepts only exact scheme://host[:port] origins: no wildcards, paths or "null".
func checkOrigin(o string) error {
	if strings.ContainsAny(o, "*?[") || o == "null" {
		return fmt.Errorf("allowedOrigins %q: wildcard and null origins are not allowed", o)
	}
	u, err := url.Parse(o)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
		return fmt.Errorf("allowedOrigins %q: must be an exact origin like http://localhost:5173", o)
	}
	return nil
}
