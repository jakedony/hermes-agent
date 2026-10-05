// Package protocol defines the client-facing WebSocket protocol (PROTOCOL.md) and its validation.
package protocol

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

const Version = 1

// Subprotocol is the WebSocket subprotocol the server selects when a client offers subprotocols.
const Subprotocol = "hermes-bridge.v1"

// TokenSubprotocolPrefix carries the bearer token for browser clients, which cannot set headers.
const TokenSubprotocolPrefix = "hermes-bridge.token."

// Client message types.
const (
	TypeAsk        = "ask"
	TypeEndSession = "end_session"
)

// Server event types.
const (
	EventHello        = "hello"
	EventAccepted     = "accepted"
	EventAnswer       = "answer"
	EventError        = "error"
	EventDone         = "done"
	EventSessionEnded = "session_ended"
)

type Code string

const (
	CodeMalformedMessage    Code = "MALFORMED_MESSAGE"
	CodeUnsupportedVersion  Code = "UNSUPPORTED_PROTOCOL_VERSION"
	CodeUnsupportedType     Code = "UNSUPPORTED_MESSAGE_TYPE"
	CodeMissingField        Code = "MISSING_FIELD"
	CodeInvalidField        Code = "INVALID_FIELD"
	CodeEmptyMessage        Code = "EMPTY_MESSAGE"
	CodeMessageTooLarge     Code = "MESSAGE_TOO_LARGE"
	CodeUnsupportedContext  Code = "UNSUPPORTED_CONTEXT"
	CodeDuplicateRequestID  Code = "DUPLICATE_REQUEST_ID"
	CodeSessionBusy         Code = "SESSION_BUSY"
	CodeSessionLimit        Code = "SESSION_LIMIT"
	CodeServerBusy          Code = "SERVER_BUSY"
	CodeHistoryLimit        Code = "HISTORY_LIMIT"
	CodeShuttingDown        Code = "SHUTTING_DOWN"
	CodeHermesTimeout       Code = "HERMES_TIMEOUT"
	CodeHermesError         Code = "HERMES_ERROR"
	CodeHermesNotConfigured Code = "HERMES_NOT_CONFIGURED"
	CodeWorkerStartFailed   Code = "WORKER_START_FAILED"
	CodeWorkerCrashed       Code = "WORKER_CRASHED"
	CodeWorkerProtocol      Code = "WORKER_PROTOCOL_ERROR"
	CodeResponseTooLarge    Code = "RESPONSE_TOO_LARGE"
	CodeInternal            Code = "INTERNAL_ERROR"
)

// Limits are announced in the hello event so clients can pre-validate.
type Limits struct {
	MaxMessageBytes       int64 `json:"maxMessageBytes"`
	MaxQuestionBytes      int   `json:"maxQuestionBytes"`
	MaxSessions           int   `json:"maxSessions"`
	MaxConcurrentRequests int   `json:"maxConcurrentRequests"`
	MaxTurnsPerSession    int   `json:"maxTurnsPerSession"`
	RequestTimeoutMs      int64 `json:"requestTimeoutMs"`
	IdleSessionTimeoutMs  int64 `json:"idleSessionTimeoutMs"`
}

type Hello struct {
	ProtocolVersion int    `json:"protocolVersion"`
	Type            string `json:"type"`
	ConnectionID    string `json:"connectionId"`
	Limits          Limits `json:"limits"`
}

type Accepted struct {
	ProtocolVersion int    `json:"protocolVersion"`
	Type            string `json:"type"`
	RequestID       string `json:"requestId"`
	SessionID       string `json:"sessionId"`
	NewSession      bool   `json:"newSession"`
}

type Answer struct {
	ProtocolVersion int    `json:"protocolVersion"`
	Type            string `json:"type"`
	RequestID       string `json:"requestId"`
	SessionID       string `json:"sessionId"`
	Text            string `json:"text"`
}

// Error identifiers are pointers so an unparseable identifier is sent as JSON null.
type Error struct {
	ProtocolVersion int     `json:"protocolVersion"`
	Type            string  `json:"type"`
	RequestID       *string `json:"requestId"`
	SessionID       *string `json:"sessionId"`
	Code            Code    `json:"code"`
	Message         string  `json:"message"`
	Retryable       bool    `json:"retryable"`
	// Rejected is true when the request was never admitted: no accepted/done events follow or precede it.
	Rejected bool `json:"rejected"`
	// SessionReset is true when the session's conversation history was discarded.
	SessionReset bool   `json:"sessionReset,omitempty"`
	Field        string `json:"field,omitempty"`
}

type Done struct {
	ProtocolVersion int    `json:"protocolVersion"`
	Type            string `json:"type"`
	RequestID       string `json:"requestId"`
	SessionID       string `json:"sessionId"`
	Status          string `json:"status"` // "ok" or "error"
	DurationMs      int64  `json:"durationMs"`
}

type SessionEnded struct {
	ProtocolVersion int     `json:"protocolVersion"`
	Type            string  `json:"type"`
	RequestID       *string `json:"requestId"`
	SessionID       string  `json:"sessionId"`
	Reason          string  `json:"reason"` // "client_request" or "idle_timeout"
	Existed         bool    `json:"existed"`
}

// Context is the reserved document context. Fields are validated but any present field is
// rejected with UNSUPPORTED_CONTEXT until the server can actually use it.
type Context struct {
	DocumentID   *string `json:"documentId"`
	Page         *int    `json:"page"`
	SelectedText *string `json:"selectedText"`
}

type Ask struct {
	RequestID string
	SessionID string
	Message   string
}

type EndSession struct {
	RequestID string
	SessionID string
}

// Reject describes why a client message was not admitted.
type Reject struct {
	Code      Code
	Message   string
	Field     string
	Retryable bool
	RequestID *string
	SessionID *string
}

func (r *Reject) Event() Error {
	return Error{
		ProtocolVersion: Version, Type: EventError, RequestID: r.RequestID, SessionID: r.SessionID,
		Code: r.Code, Message: r.Message, Retryable: r.Retryable, Rejected: true, Field: r.Field,
	}
}

const (
	maxIDLen           = 128
	maxDocumentIDLen   = 128
	maxSelectedTextLen = 16 << 10
)

var idPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

// ValidID reports whether s is an acceptable requestId/sessionId.
func ValidID(s string) bool { return idPattern.MatchString(s) }

type askWire struct {
	ProtocolVersion int             `json:"protocolVersion"`
	Type            string          `json:"type"`
	RequestID       string          `json:"requestId"`
	SessionID       string          `json:"sessionId"`
	Message         string          `json:"message"`
	Context         json.RawMessage `json:"context"`
}

type endWire struct {
	ProtocolVersion int    `json:"protocolVersion"`
	Type            string `json:"type"`
	RequestID       string `json:"requestId"`
	SessionID       string `json:"sessionId"`
}

// Parse validates one client text frame and returns *Ask or *EndSession, or a Reject.
func Parse(data []byte, maxQuestionBytes int) (any, *Reject) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return nil, &Reject{Code: CodeMalformedMessage, Message: "The message must be a single JSON object."}
	}
	rej := func(code Code, field, msg string) *Reject {
		return &Reject{Code: code, Field: field, Message: msg,
			RequestID: safeID(fields["requestId"]), SessionID: safeID(fields["sessionId"])}
	}

	rawVersion, ok := fields["protocolVersion"]
	if !ok {
		return nil, rej(CodeMissingField, "protocolVersion", "protocolVersion is required.")
	}
	var version int
	if err := json.Unmarshal(rawVersion, &version); err != nil || version != Version {
		return nil, rej(CodeUnsupportedVersion, "protocolVersion",
			fmt.Sprintf("Unsupported protocolVersion; this server supports [%d].", Version))
	}
	var msgType string
	if raw, ok := fields["type"]; !ok {
		return nil, rej(CodeMissingField, "type", "type is required.")
	} else if err := json.Unmarshal(raw, &msgType); err != nil {
		return nil, rej(CodeInvalidField, "type", "type must be a string.")
	}

	switch msgType {
	case TypeAsk:
		return parseAsk(data, fields, maxQuestionBytes, rej)
	case TypeEndSession:
		var w endWire
		if r := strictDecode(data, &w, rej); r != nil {
			return nil, r
		}
		if r := checkIDs(fields, w.RequestID, w.SessionID, rej); r != nil {
			return nil, r
		}
		return &EndSession{RequestID: w.RequestID, SessionID: w.SessionID}, nil
	default:
		return nil, rej(CodeUnsupportedType, "type", fmt.Sprintf("Unsupported message type; expected %q or %q.", TypeAsk, TypeEndSession))
	}
}

func parseAsk(data []byte, fields map[string]json.RawMessage, maxQuestionBytes int, rej func(Code, string, string) *Reject) (any, *Reject) {
	var w askWire
	if r := strictDecode(data, &w, rej); r != nil {
		return nil, r
	}
	if r := checkIDs(fields, w.RequestID, w.SessionID, rej); r != nil {
		return nil, r
	}
	if _, ok := fields["message"]; !ok {
		return nil, rej(CodeMissingField, "message", "message is required.")
	}
	if strings.TrimSpace(w.Message) == "" {
		return nil, rej(CodeEmptyMessage, "message", "message must contain non-whitespace text.")
	}
	if len(w.Message) > maxQuestionBytes {
		return nil, rej(CodeMessageTooLarge, "message", fmt.Sprintf("message exceeds %d bytes.", maxQuestionBytes))
	}
	if r := checkContext(w.Context, rej); r != nil {
		return nil, r
	}
	return &Ask{RequestID: w.RequestID, SessionID: w.SessionID, Message: w.Message}, nil
}

func strictDecode(data []byte, v any, rej func(Code, string, string) *Reject) *Reject {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		var typeErr *json.UnmarshalTypeError
		if errors.As(err, &typeErr) {
			return rej(CodeInvalidField, typeErr.Field, fmt.Sprintf("%s has the wrong type.", typeErr.Field))
		}
		if field, ok := strings.CutPrefix(err.Error(), "json: unknown field "); ok {
			field = strings.Trim(field, `"`)
			return rej(CodeInvalidField, field, fmt.Sprintf("Unknown field %q (protocol v%d is strict).", field, Version))
		}
		return rej(CodeMalformedMessage, "", "The message could not be decoded.")
	}
	return nil
}

func checkIDs(fields map[string]json.RawMessage, requestID, sessionID string, rej func(Code, string, string) *Reject) *Reject {
	for _, f := range []struct{ name, value string }{{"requestId", requestID}, {"sessionId", sessionID}} {
		if _, ok := fields[f.name]; !ok {
			return rej(CodeMissingField, f.name, f.name+" is required.")
		}
		if !ValidID(f.value) {
			return rej(CodeInvalidField, f.name, f.name+" must be 1-128 characters of A-Z a-z 0-9 . _ : -")
		}
	}
	return nil
}

func checkContext(raw json.RawMessage, rej func(Code, string, string) *Reject) *Reject {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var ctx Context
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ctx); err != nil {
		return rej(CodeInvalidField, "context", "context must be an object with only documentId, page and selectedText.")
	}
	switch {
	case ctx.DocumentID != nil && (len(*ctx.DocumentID) == 0 || len(*ctx.DocumentID) > maxDocumentIDLen):
		return rej(CodeInvalidField, "context.documentId", "context.documentId must be 1-128 bytes.")
	case ctx.Page != nil && *ctx.Page < 1:
		return rej(CodeInvalidField, "context.page", "context.page must be an integer >= 1.")
	case ctx.SelectedText != nil && len(*ctx.SelectedText) > maxSelectedTextLen:
		return rej(CodeInvalidField, "context.selectedText", "context.selectedText exceeds 16384 bytes.")
	}
	if ctx.DocumentID != nil || ctx.Page != nil || ctx.SelectedText != nil {
		return rej(CodeUnsupportedContext, "context",
			"Document context is not supported in this version; the question was not sent, so it cannot be answered without the context.")
	}
	return nil
}

// safeID returns the identifier only when it is a string that passes ValidID.
func safeID(raw json.RawMessage) *string {
	var s string
	if raw == nil || json.Unmarshal(raw, &s) != nil || !ValidID(s) {
		return nil
	}
	return &s
}

func Ptr(s string) *string { return &s }
