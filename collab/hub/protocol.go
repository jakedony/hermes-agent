package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"regexp"
)

const protocolVersion = 1

var requestIDRe = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,80}$`)

// Envelope is the JSON object on both WebSocket and HTTP.
type Envelope struct {
	V         int             `json:"v"`
	Type      string          `json:"type"`
	RequestID string          `json:"request_id,omitempty"`
	EventID   string          `json:"event_id,omitempty"`
	Seq       int64           `json:"seq,omitempty"`
	RoomID    string          `json:"room_id,omitempty"`
	TaskID    string          `json:"task_id,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

// Event is an accepted, immutable fact.
type Event struct {
	ID        string          `json:"event_id"`
	Seq       int64           `json:"seq"`
	Type      string          `json:"type"`
	RoomID    string          `json:"room_id"`
	TaskID    string          `json:"task_id,omitempty"`
	Actor     string          `json:"actor"`
	CreatedAt int64           `json:"created_at_ms"`
	Payload   json.RawMessage `json:"payload"`
}

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(b[:])
}

func normalizePayload(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}
	return raw
}
