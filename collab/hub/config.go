package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Limits are the V1 capacity and size caps. Zero values are filled by applyDefaults.
type Limits struct {
	RootDeadlineSec         int `json:"root_deadline_sec"`
	ChildDeadlineSec        int `json:"child_deadline_sec"`
	MaxChildTasks           int `json:"max_child_tasks"`
	MaxAttempts             int `json:"max_attempts"`
	LeaseSec                int `json:"lease_sec"`
	RetryBackoffSec         int `json:"retry_backoff_sec"`
	MaxExecutionSec         int `json:"max_execution_sec"`
	MaxObjectiveBytes       int `json:"max_objective_bytes"`
	MaxContextBytes         int `json:"max_context_bytes"`
	MaxSummaryBytes         int `json:"max_summary_bytes"`
	MaxEvidenceItems        int `json:"max_evidence_items"`
	MaxExcerptBytes         int `json:"max_excerpt_bytes"`
	MaxLimitationItems      int `json:"max_limitation_items"`
	MaxLimitationBytes      int `json:"max_limitation_bytes"`
	IdempotencyRetentionSec int `json:"idempotency_retention_sec"`
	MaxFrameBytes           int `json:"max_frame_bytes"`
	EventBuffer             int `json:"event_buffer"`
}

// Principal is a static identity. TokenSHA256 is hex-encoded SHA-256 of the bearer token.
type Principal struct {
	Kind         string   `json:"kind"`
	MachineID    string   `json:"machine_id"`
	Capabilities []string `json:"capabilities"`
	TokenSHA256  string   `json:"token_sha256"`
}

// Config is static server configuration. Tokens are hashes, never the secrets.
type Config struct {
	Listen     string               `json:"listen"`
	RoomID     string               `json:"room_id"`
	DBPath     string               `json:"db_path"`
	Principals map[string]Principal `json:"principals"`
	Limits     Limits               `json:"limits"`
}

func applyDefaults(c *Config) {
	if c.Listen == "" {
		c.Listen = "127.0.0.1:8765"
	}
	if c.RoomID == "" {
		c.RoomID = "lab"
	}
	d := &c.Limits
	if d.RootDeadlineSec == 0 {
		d.RootDeadlineSec = 180
	}
	if d.ChildDeadlineSec == 0 {
		d.ChildDeadlineSec = 120
	}
	if d.MaxChildTasks == 0 {
		d.MaxChildTasks = 3
	}
	if d.MaxAttempts == 0 {
		d.MaxAttempts = 2
	}
	if d.LeaseSec == 0 {
		d.LeaseSec = 30
	}
	if d.RetryBackoffSec == 0 {
		d.RetryBackoffSec = 2
	}
	if d.MaxExecutionSec == 0 {
		d.MaxExecutionSec = 150
	}
	if d.MaxObjectiveBytes == 0 {
		d.MaxObjectiveBytes = 2000
	}
	if d.MaxContextBytes == 0 {
		d.MaxContextBytes = 8000
	}
	if d.MaxSummaryBytes == 0 {
		d.MaxSummaryBytes = 8000
	}
	if d.MaxEvidenceItems == 0 {
		d.MaxEvidenceItems = 8
	}
	if d.MaxExcerptBytes == 0 {
		d.MaxExcerptBytes = 2000
	}
	if d.MaxLimitationItems == 0 {
		d.MaxLimitationItems = 8
	}
	if d.MaxLimitationBytes == 0 {
		d.MaxLimitationBytes = 500
	}
	if d.IdempotencyRetentionSec == 0 {
		d.IdempotencyRetentionSec = 7 * 24 * 3600
	}
	if d.MaxFrameBytes == 0 {
		d.MaxFrameBytes = 262144
	}
	if d.EventBuffer == 0 {
		d.EventBuffer = 32
	}
}

func loadConfig(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	applyDefaults(&cfg)
	if err := validateConfig(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func validateConfig(cfg Config) error {
	if cfg.DBPath == "" {
		return fmt.Errorf("config: db_path is required")
	}
	if len(cfg.Principals) == 0 {
		return fmt.Errorf("config: principals are required")
	}
	for id, p := range cfg.Principals {
		if p.Kind != "human" && p.Kind != "agent" {
			return fmt.Errorf("config: principal %s kind must be human or agent", id)
		}
		if p.Kind == "agent" && p.MachineID == "" {
			return fmt.Errorf("config: agent %s needs machine_id", id)
		}
		if _, err := hex.DecodeString(p.TokenSHA256); err != nil || len(p.TokenSHA256) != 64 {
			return fmt.Errorf("config: principal %s token_sha256 must be 64 hex chars", id)
		}
	}
	return nil
}

// Actor is the authenticated principal. Client-supplied names are never copied onto it.
type Actor struct {
	ID           string
	Kind         string
	MachineID    string
	Capabilities []string
}

func (c Config) authenticate(token string) (Actor, bool) {
	sum := sha256.Sum256([]byte(token))
	var found Actor
	ok := false
	for id, p := range c.Principals {
		want, err := hex.DecodeString(p.TokenSHA256)
		if err != nil || len(want) != len(sum) {
			continue
		}
		if subtle.ConstantTimeCompare(sum[:], want) == 1 {
			found = Actor{ID: id, Kind: p.Kind, MachineID: p.MachineID, Capabilities: append([]string(nil), p.Capabilities...)}
			ok = true
		}
	}
	return found, ok
}
