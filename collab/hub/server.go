package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// Server exposes the engine over HTTP and WebSocket.
type Server struct {
	eng    *Engine
	log    *slog.Logger
	manual *offsetClock
}

func NewServer(eng *Engine, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	return &Server{eng: eng, log: log}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/command", s.command)
	mux.HandleFunc("GET /v1/tasks", s.listTasks)
	mux.HandleFunc("GET /v1/tasks/{id}", s.getTask)
	mux.HandleFunc("GET /v1/tasks/{id}/history", s.taskHistory)
	mux.HandleFunc("GET /ws", s.socket)
	mux.HandleFunc("POST /v1/test/advance", s.advanceClock)
	return mux
}

func (s *Server) advanceClock(w http.ResponseWriter, r *http.Request) {
	if s.manual == nil {
		http.NotFound(w, r)
		return
	}
	if _, ok := s.authorize(w, r); !ok {
		return
	}
	ms, ok := readAdvanceMS(w, r)
	if !ok {
		return
	}
	s.manual.Advance(time.Duration(ms) * time.Millisecond)
	events, err := s.eng.Sweep()
	if err != nil {
		s.log.Error("test clock sweep failed", "err", err)
		writeTransport(w, http.StatusInternalServerError, "", "internal", "sweep failed")
		return
	}
	s.eng.Publish(events)
	raw, err := json.Marshal(map[string]any{"ok": true, "now_ms": s.eng.now()})
	if err != nil {
		writeTransport(w, http.StatusInternalServerError, "", "internal", "encode failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func readAdvanceMS(w http.ResponseWriter, r *http.Request) (int64, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, 1024)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var body struct {
		AdvanceMS int64 `json:"advance_ms"`
	}
	if err := dec.Decode(&body); err != nil || body.AdvanceMS <= 0 || body.AdvanceMS > 3_600_000 {
		writeTransport(w, http.StatusBadRequest, "", "bad_request", "advance_ms must be 1..3600000")
		return 0, false
	}
	return body.AdvanceMS, true
}

func (s *Server) command(w http.ResponseWriter, r *http.Request) {
	// A malformed frame is a bad request even when the bearer token is missing.
	env, ok := s.readEnvelope(w, r)
	if !ok {
		return
	}
	actor, ok := s.authorize(w, r)
	if !ok {
		return
	}
	s.finish(w, r, actor, env)
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	s.readQuery(w, r, "task.list", "", map[string]any{
		"assigned_to": r.URL.Query().Get("assigned_to"),
		"state":       r.URL.Query().Get("state"),
	})
}

func (s *Server) getTask(w http.ResponseWriter, r *http.Request) {
	s.readQuery(w, r, "task.get", r.PathValue("id"), map[string]any{})
}

func (s *Server) taskHistory(w http.ResponseWriter, r *http.Request) {
	s.readQuery(w, r, "task.history", r.PathValue("id"), map[string]any{})
}

func (s *Server) readQuery(w http.ResponseWriter, r *http.Request, typ, taskID string, payload map[string]any) {
	actor, ok := s.authorize(w, r)
	if !ok {
		return
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		writeTransport(w, http.StatusInternalServerError, "", "internal", "encode failed")
		return
	}
	env := Envelope{V: protocolVersion, Type: typ, RequestID: newID("qry_"), RoomID: s.eng.cfg.RoomID, TaskID: taskID, Payload: raw}
	s.finish(w, r, actor, env)
}

func (s *Server) finish(w http.ResponseWriter, r *http.Request, actor Actor, env Envelope) {
	res, err := s.eng.Handle(actor, env)
	if err != nil {
		s.log.Error("command failed", "err", err, "type", env.Type, "actor", actor.ID, "request_id", env.RequestID)
		writeTransport(w, http.StatusInternalServerError, env.RequestID, "internal", "database error")
		return
	}
	if !res.Replay {
		s.eng.Publish(res.Events)
	}
	s.log.Info("command", "type", env.Type, "actor", actor.ID, "task_id", env.TaskID, "request_id", env.RequestID, "ok", res.OK, "code", res.Code)
	writeResult(w, env.RequestID, res)
}

func (s *Server) authorize(w http.ResponseWriter, r *http.Request) (Actor, bool) {
	if strings.Contains(strings.ToLower(r.URL.RawQuery), "token") {
		writeTransport(w, http.StatusBadRequest, "", "bad_request", "tokens do not belong in URLs")
		return Actor{}, false
	}
	actor, ok := s.eng.cfg.authenticate(bearerToken(r))
	if !ok {
		writeTransport(w, http.StatusUnauthorized, "", "unauthorized", "unauthorized")
		return Actor{}, false
	}
	return actor, true
}

func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}

func (s *Server) readEnvelope(w http.ResponseWriter, r *http.Request) (Envelope, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, int64(s.eng.cfg.Limits.MaxFrameBytes))
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var env Envelope
	if err := dec.Decode(&env); err != nil {
		writeTransport(w, http.StatusBadRequest, "", "bad_request", "malformed envelope")
		return Envelope{}, false
	}
	return env, true
}

func writeResult(w http.ResponseWriter, requestID string, res Result) {
	typ := "ack"
	if !res.OK {
		typ = "error"
	}
	payload := res.Wire
	if len(payload) == 0 {
		payload = []byte("null")
	}
	raw, err := json.Marshal(Envelope{V: protocolVersion, Type: typ, RequestID: requestID, Payload: payload})
	if err != nil {
		writeTransport(w, http.StatusInternalServerError, requestID, "internal", "encode failed")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}

func writeTransport(w http.ResponseWriter, status int, requestID, code, message string) {
	payload, _ := json.Marshal(map[string]string{"code": code, "message": message})
	raw, _ := json.Marshal(Envelope{V: protocolVersion, Type: "error", RequestID: requestID, Payload: payload})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func (s *Server) socket(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(strings.ToLower(r.URL.RawQuery), "token") {
		writeTransport(w, http.StatusBadRequest, "", "bad_request", "tokens do not belong in URLs")
		return
	}
	actor, ok := s.eng.cfg.authenticate(bearerToken(r))
	if !ok {
		writeTransport(w, http.StatusUnauthorized, "", "unauthorized", "unauthorized")
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		s.log.Info("websocket reject", "err", err)
		return
	}
	conn.SetReadLimit(int64(s.eng.cfg.Limits.MaxFrameBytes))
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	defer conn.Close(websocket.StatusGoingAway, "closed")

	env, err := readEnvelope(ctx, conn)
	if err != nil {
		_ = writeWS(ctx, conn, "", Result{Code: "bad_request", Message: "malformed frame", Body: map[string]string{"code": "bad_request", "message": "malformed frame"}})
		return
	}
	if env.Type != "agent.hello" || env.V != protocolVersion {
		_ = writeWS(ctx, conn, env.RequestID, Result{Code: "bad_request", Message: "first message must be agent.hello"})
		return
	}
	var hello struct {
		Takeover bool `json:"takeover"`
	}
	if err := decode(normalizePayload(env.Payload), &hello); err != nil {
		_ = writeWS(ctx, conn, env.RequestID, Result{Code: "bad_request", Message: "malformed payload"})
		return
	}
	session, err := s.eng.Bind(ctx, actor, hello.Takeover)
	if err != nil {
		var c *coded
		if errors.As(err, &c) {
			_ = writeWS(ctx, conn, env.RequestID, Result{Code: c.Code, Message: c.Message})
		}
		_ = conn.Close(websocket.StatusPolicyViolation, "session rejected")
		return
	}
	defer s.eng.Unbind(session)
	if err := writeWS(ctx, conn, env.RequestID, Result{OK: true, Body: map[string]any{
		"session_id": session.ID, "agent_id": actor.ID, "machine_id": actor.MachineID, "room_id": s.eng.cfg.RoomID,
	}}); err != nil {
		return
	}
	s.log.Info("session", "agent", actor.ID, "session", session.ID, "takeover", hello.Takeover)

	errc := make(chan struct{})
	go func() {
		s.writeLoop(session, conn)
		close(errc)
	}()
	s.readLoop(session, conn, actor)
	session.cancel()
	<-errc
}

func (s *Server) readLoop(session *Session, conn *websocket.Conn, actor Actor) {
	for {
		env, err := readEnvelope(session.Ctx, conn)
		if err != nil {
			return
		}
		if env.Type == "agent.hello" {
			s.enqueue(session, env.RequestID, Result{Code: "conflict", Message: "session is already helloed"})
			continue
		}
		res, err := s.eng.Handle(actor, env)
		if err != nil {
			s.log.Error("command failed", "err", err, "type", env.Type, "actor", actor.ID)
			s.enqueue(session, env.RequestID, Result{Code: "internal", Message: "database error"})
			continue
		}
		// Queue the acknowledgement before broadcasting so a waiting peer
		// usually observes the ack first. Events can still win the race.
		s.enqueue(session, env.RequestID, res)
		if !res.Replay {
			s.eng.Publish(res.Events)
		}
	}
}

func (s *Server) enqueue(session *Session, requestID string, res Result) {
	frame, err := frameFor(requestID, res)
	if err != nil {
		session.cancel()
		return
	}
	select {
	case session.Acks <- frame:
	default:
		session.cancel()
	}
}

func (s *Server) writeLoop(session *Session, conn *websocket.Conn) {
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-session.Ctx.Done():
			_ = conn.Close(websocket.StatusPolicyViolation, "session replaced or closed")
			return
		case ev := <-session.Events:
			raw, err := json.Marshal(Envelope{
				V: protocolVersion, Type: ev.Type, EventID: ev.ID, Seq: ev.Seq,
				RoomID: ev.RoomID, TaskID: ev.TaskID, Payload: ev.Payload,
			})
			if err != nil || writeFrame(session.Ctx, conn, raw) != nil {
				return
			}
		case raw := <-session.Acks:
			if writeFrame(session.Ctx, conn, raw) != nil {
				return
			}
		case <-ticker.C:
			pctx, cancel := context.WithTimeout(session.Ctx, 5*time.Second)
			err := conn.Ping(pctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func writeFrame(ctx context.Context, conn *websocket.Conn, raw []byte) error {
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, raw)
}

func readEnvelope(ctx context.Context, conn *websocket.Conn) (Envelope, error) {
	_, data, err := conn.Read(ctx)
	if err != nil {
		return Envelope{}, err
	}
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	var env Envelope
	if err := dec.Decode(&env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

func writeWS(ctx context.Context, conn *websocket.Conn, requestID string, res Result) error {
	frame, err := frameFor(requestID, res)
	if err != nil {
		return err
	}
	return writeFrame(ctx, conn, frame)
}

func frameFor(requestID string, res Result) ([]byte, error) {
	if len(res.Wire) == 0 {
		body := res.Body
		if !res.OK {
			body = map[string]any{"code": res.Code, "message": res.Message, "detail": res.Body}
		}
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		res.Wire = raw
	}
	return marshalAck(requestID, res)
}

func marshalAck(requestID string, res Result) ([]byte, error) {
	typ := "ack"
	if !res.OK {
		typ = "error"
	}
	return json.Marshal(Envelope{V: protocolVersion, Type: typ, RequestID: requestID, Payload: res.Wire})
}

// Run listens until ctx is cancelled and sweeps leases on a short interval.
func (s *Server) Run(ctx context.Context, addr string) error {
	go s.sweepLoop(ctx)
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
		return nil
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (s *Server) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			events, err := s.eng.Sweep()
			if err != nil {
				s.log.Error("sweep failed", "err", err)
				continue
			}
			s.eng.Publish(events)
		}
	}
}
