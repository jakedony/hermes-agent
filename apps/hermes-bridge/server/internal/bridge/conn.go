package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/coder/websocket"

	"hermes-bridge/internal/protocol"
	"hermes-bridge/internal/worker"
)

const (
	writeTimeout     = 10 * time.Second
	rememberedIDs    = 1024
	outboxCapacity   = 32
	closeFlushWindow = 5 * time.Second
)

// conn is one WebSocket connection. Sessions are owned by the connection that created them, keyed
// by the client's sessionId label; a different connection using the same label gets its own,
// unrelated session.
type conn struct {
	s  *Server
	id string
	ws *websocket.Conn

	ctx        context.Context
	cancel     context.CancelCauseFunc
	out        chan outMsg
	writerDone chan struct{}
	teardownMu sync.Once

	mu       sync.Mutex
	sessions map[string]*session
	seen     map[string]struct{}
	seenFIFO []string
}

type session struct {
	id           string
	proc         *worker.Process
	busy         bool
	closed       bool
	turns        int
	historyBytes int64
	lastUsed     time.Time
}

type outMsg struct {
	data        []byte
	close       bool
	closeCode   websocket.StatusCode
	closeReason string
}

func newConn(s *Server, id string, ws *websocket.Conn) *conn {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &conn{
		s: s, id: id, ws: ws, ctx: ctx, cancel: cancel,
		out: make(chan outMsg, outboxCapacity), writerDone: make(chan struct{}),
		sessions: map[string]*session{}, seen: map[string]struct{}{},
	}
}

func (c *conn) serve() {
	go c.writer()
	c.send(protocol.Hello{ProtocolVersion: protocol.Version, Type: protocol.EventHello, ConnectionID: c.id, Limits: c.s.limits()})
	c.readLoop()
	if c.s.isShuttingDown() {
		c.teardown(errShutdown)
	} else {
		c.teardown(errDisconnect)
	}
}

// readLoop only parses and admits; Hermes work always runs on its own goroutine.
func (c *conn) readLoop() {
	maxMsg := c.s.cfg.MaxMessageBytes
	// Frames between maxMessageBytes and this hard cap get a structured error; beyond it the
	// library closes the connection with 1009 (message too big).
	hard := 4 * maxMsg
	if hard < 1<<20 {
		hard = 1 << 20
	}
	c.ws.SetReadLimit(hard)
	for {
		typ, r, err := c.ws.Reader(c.ctx)
		if err != nil {
			return
		}
		data, err := io.ReadAll(io.LimitReader(r, maxMsg+1))
		if err != nil {
			return
		}
		if int64(len(data)) > maxMsg {
			if _, err := io.Copy(io.Discard, r); err != nil {
				return
			}
			c.reject(&protocol.Reject{Code: protocol.CodeMessageTooLarge, Message: "The WebSocket message exceeds the configured maxMessageBytes."})
			continue
		}
		if typ != websocket.MessageText {
			c.reject(&protocol.Reject{Code: protocol.CodeMalformedMessage, Message: "Only UTF-8 text frames carrying JSON are accepted."})
			continue
		}
		msg, rej := protocol.Parse(data, c.s.cfg.MaxQuestionBytes)
		if rej != nil {
			c.reject(rej)
			continue
		}
		switch m := msg.(type) {
		case *protocol.Ask:
			c.admit(m)
		case *protocol.EndSession:
			c.endSession(m)
		}
	}
}

func (c *conn) writer() {
	defer close(c.writerDone)
	for {
		select {
		case m := <-c.out:
			if m.close {
				_ = c.ws.Close(m.closeCode, m.closeReason)
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			err := c.ws.Write(ctx, websocket.MessageText, m.data)
			cancel()
			if err != nil {
				_ = c.ws.CloseNow()
				return
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// send queues an event for the single writer goroutine; it is dropped if the connection is gone.
func (c *conn) send(v any) {
	data, err := json.Marshal(v)
	if err != nil {
		c.s.log.Error("event_encode_failed", "connId", c.id, "error", err.Error())
		return
	}
	select {
	case c.out <- outMsg{data: data}:
	case <-c.writerDone:
	case <-c.ctx.Done():
	}
}

func (c *conn) reject(r *protocol.Reject) {
	c.s.log.Info("request_rejected", "connId", c.id, "requestId", deref(r.RequestID), "sessionId", deref(r.SessionID),
		"code", string(r.Code), "field", r.Field)
	c.send(r.Event())
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func (c *conn) rememberLocked(requestID string) {
	c.seen[requestID] = struct{}{}
	c.seenFIFO = append(c.seenFIFO, requestID)
	if len(c.seenFIFO) > rememberedIDs {
		delete(c.seen, c.seenFIFO[0])
		c.seenFIFO = c.seenFIFO[1:]
	}
}

func rejectFor(code protocol.Code, retryable bool, requestID, sessionID, msg string) *protocol.Reject {
	return &protocol.Reject{Code: code, Retryable: retryable, Message: msg,
		RequestID: protocol.Ptr(requestID), SessionID: protocol.Ptr(sessionID)}
}

// admit applies every admission check, then hands the request to its own goroutine.
func (c *conn) admit(a *protocol.Ask) {
	cfg := c.s.cfg
	if c.s.isShuttingDown() {
		c.reject(rejectFor(protocol.CodeShuttingDown, true, a.RequestID, a.SessionID, "The server is shutting down."))
		return
	}
	c.mu.Lock()
	if _, dup := c.seen[a.RequestID]; dup {
		c.mu.Unlock()
		c.reject(rejectFor(protocol.CodeDuplicateRequestID, false, a.RequestID, a.SessionID,
			"This requestId was already used on this connection."))
		return
	}
	sess := c.sessions[a.SessionID]
	newSession := sess == nil
	var rej *protocol.Reject
	switch {
	case sess != nil && sess.busy:
		rej = rejectFor(protocol.CodeSessionBusy, true, a.RequestID, a.SessionID,
			"This session is still processing a previous request; wait for its done event.")
	case sess != nil && (sess.turns >= cfg.MaxTurnsPerSession || sess.historyBytes >= cfg.MaxHistoryBytes):
		rej = rejectFor(protocol.CodeHistoryLimit, false, a.RequestID, a.SessionID,
			"This session reached its history limit; start a new sessionId.")
	case newSession && !c.s.reserveSession():
		rej = rejectFor(protocol.CodeSessionLimit, true, a.RequestID, a.SessionID,
			"The server has reached maxSessions; end a session or retry later.")
	}
	if rej == nil {
		select {
		case c.s.slots <- struct{}{}:
		default:
			if newSession {
				c.s.releaseSession()
			}
			rej = rejectFor(protocol.CodeServerBusy, true, a.RequestID, a.SessionID,
				"The server is running maxConcurrentRequests; retry later.")
		}
	}
	if rej == nil && !c.s.trackRequest() {
		<-c.s.slots
		if newSession {
			c.s.releaseSession()
		}
		rej = rejectFor(protocol.CodeShuttingDown, true, a.RequestID, a.SessionID, "The server is shutting down.")
	}
	if rej != nil {
		c.mu.Unlock()
		c.reject(rej)
		return
	}
	if newSession {
		sess = &session{id: a.SessionID}
		c.sessions[a.SessionID] = sess
	}
	sess.busy = true
	sess.lastUsed = time.Now()
	c.rememberLocked(a.RequestID)
	c.mu.Unlock()

	c.s.log.Info("request_accepted", "connId", c.id, "requestId", a.RequestID, "sessionId", a.SessionID,
		"newSession", newSession, "questionBytes", len(a.Message))
	c.send(protocol.Accepted{ProtocolVersion: protocol.Version, Type: protocol.EventAccepted,
		RequestID: a.RequestID, SessionID: a.SessionID, NewSession: newSession})
	go c.run(sess, a)
}

// trackRequest registers an in-flight request unless shutdown has begun (checked under the same
// lock Shutdown uses, so Shutdown's wait cannot miss a request).
func (s *Server) trackRequest() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shuttingDown {
		return false
	}
	s.requests.Add(1)
	return true
}

type failure struct {
	code      protocol.Code
	message   string
	retryable bool
	reset     bool
}

func (c *conn) run(sess *session, a *protocol.Ask) {
	defer c.s.requests.Done()
	defer func() { <-c.s.slots }()
	start := time.Now()

	base, cancelBase := context.WithCancelCause(c.ctx)
	stop := context.AfterFunc(c.s.root, func() { cancelBase(errShutdown) })
	defer stop()
	defer cancelBase(nil)

	res, fail := c.execute(base, sess, a)
	duration := time.Since(start).Milliseconds()

	c.mu.Lock()
	sess.busy = false
	sess.lastUsed = time.Now()
	if fail == nil {
		sess.turns++
		sess.historyBytes = res.HistoryBytes
	}
	c.mu.Unlock()
	if fail != nil && fail.reset {
		c.dropSession(sess)
	}

	if c.ctx.Err() != nil {
		c.s.log.Info("request_abandoned", "connId", c.id, "requestId", a.RequestID, "sessionId", a.SessionID,
			"reason", "client_disconnected", "durationMs", duration)
		return
	}
	if fail != nil {
		c.s.log.Warn("request_failed", "connId", c.id, "requestId", a.RequestID, "sessionId", a.SessionID,
			"code", string(fail.code), "sessionReset", fail.reset, "durationMs", duration)
		c.send(protocol.Error{ProtocolVersion: protocol.Version, Type: protocol.EventError,
			RequestID: protocol.Ptr(a.RequestID), SessionID: protocol.Ptr(a.SessionID),
			Code: fail.code, Message: fail.message, Retryable: fail.retryable, SessionReset: fail.reset})
	} else {
		c.s.log.Info("request_completed", "connId", c.id, "requestId", a.RequestID, "sessionId", a.SessionID,
			"durationMs", duration, "answerBytes", len(res.Text), "turns", sess.turns,
			"historyBytes", res.HistoryBytes, "apiCalls", res.APICalls)
		c.send(protocol.Answer{ProtocolVersion: protocol.Version, Type: protocol.EventAnswer,
			RequestID: a.RequestID, SessionID: a.SessionID, Text: res.Text})
	}
	status := "ok"
	if fail != nil {
		status = "error"
	}
	c.send(protocol.Done{ProtocolVersion: protocol.Version, Type: protocol.EventDone,
		RequestID: a.RequestID, SessionID: a.SessionID, Status: status, DurationMs: duration})
}

func (c *conn) execute(ctx context.Context, sess *session, a *protocol.Ask) (worker.Result, *failure) {
	if sess.proc == nil {
		p, _, err := c.s.startWorker(ctx)
		if err != nil {
			return worker.Result{}, startFailure(ctx, err)
		}
		c.mu.Lock()
		sess.proc = p
		c.mu.Unlock()
		go c.watchWorker(sess, p)
	}
	reqCtx, cancel := context.WithTimeoutCause(ctx, c.s.cfg.RequestTimeout.Duration, errTimeout)
	defer cancel()
	res, err := sess.proc.Ask(reqCtx, a.RequestID, a.Message, c.deltaForwarder(a))
	if err != nil {
		return res, askFailure(err)
	}
	if !res.OK {
		return res, resultFailure(res)
	}
	return res, nil
}

// deltaForwarder turns worker deltas into answer_delta events, or returns nil when the client did
// not ask to stream. Streamed text per request is capped at maxWorkerRecordBytes, the same bound
// as the final answer; past it the remaining deltas are dropped and the answer event still
// carries the full text (or RESPONSE_TOO_LARGE).
func (c *conn) deltaForwarder(a *protocol.Ask) func(string) {
	if !a.Stream {
		return nil
	}
	seq, streamed, limit := 0, 0, c.s.cfg.MaxWorkerRecordBytes
	return func(text string) {
		if streamed += len(text); streamed > limit {
			return
		}
		c.send(protocol.AnswerDelta{ProtocolVersion: protocol.Version, Type: protocol.EventAnswerDelta,
			RequestID: a.RequestID, SessionID: a.SessionID, Seq: seq, Text: text})
		seq++
	}
}

func startFailure(ctx context.Context, err error) *failure {
	if errors.Is(context.Cause(ctx), errShutdown) {
		return &failure{code: protocol.CodeShuttingDown, message: "The server is shutting down.", retryable: true, reset: true}
	}
	var se *worker.StartError
	if errors.As(err, &se) && se.Code == string(protocol.CodeHermesNotConfigured) {
		return &failure{code: protocol.CodeHermesNotConfigured, reset: true,
			message: "Hermes has no model provider configured on the server; run `hermes model` there. (" + se.Message + ")"}
	}
	return &failure{code: protocol.CodeWorkerStartFailed, message: "The Hermes worker could not start: " + err.Error(), retryable: true, reset: true}
}

func askFailure(err error) *failure {
	var ce *worker.CancelledError
	switch {
	case errors.As(err, &ce) && errors.Is(ce.Cause, errTimeout):
		msg := "The agent did not finish within the configured timeout."
		if ce.Recycled {
			msg += " The worker was stopped and this session's conversation history was discarded."
		} else {
			msg += " The turn was cancelled; earlier conversation history is kept."
		}
		return &failure{code: protocol.CodeHermesTimeout, message: msg, retryable: true, reset: ce.Recycled}
	case errors.As(err, &ce) && errors.Is(ce.Cause, errShutdown):
		return &failure{code: protocol.CodeShuttingDown, message: "The server is shutting down; the request was cancelled.", retryable: true, reset: true}
	case errors.As(err, &ce):
		return &failure{code: protocol.CodeInternal, message: "The request was cancelled.", reset: ce.Recycled}
	case errors.Is(err, worker.ErrProtocol):
		return &failure{code: protocol.CodeWorkerProtocol, message: "The Hermes worker produced invalid output and was stopped; session history was discarded.", retryable: true, reset: true}
	default:
		return &failure{code: protocol.CodeWorkerCrashed, message: "The Hermes worker exited unexpectedly; session history was discarded.", retryable: true, reset: true}
	}
}

func resultFailure(res worker.Result) *failure {
	switch res.Code {
	case string(protocol.CodeResponseTooLarge):
		return &failure{code: protocol.CodeResponseTooLarge, message: res.Message}
	case string(protocol.CodeHermesError):
		return &failure{code: protocol.CodeHermesError, message: res.Message, retryable: true}
	default:
		return &failure{code: protocol.CodeHermesError, message: "Hermes did not complete the turn (" + res.Code + ").", retryable: true}
	}
}

// watchWorker ends an idle session whose worker died on its own, so the client learns the history
// is gone instead of discovering it on the next question.
func (c *conn) watchWorker(sess *session, p *worker.Process) {
	<-p.Exited()
	c.mu.Lock()
	idle := !sess.busy && !sess.closed
	c.mu.Unlock()
	if idle && c.dropSession(sess) {
		c.s.log.Warn("session_worker_exited", "connId", c.id, "sessionId", sess.id)
		c.send(protocol.SessionEnded{ProtocolVersion: protocol.Version, Type: protocol.EventSessionEnded,
			SessionID: sess.id, Reason: "worker_exited", Existed: true})
	}
}

// dropSession unregisters a session exactly once, frees its slot and stops its worker.
func (c *conn) dropSession(sess *session) bool {
	c.mu.Lock()
	if sess.closed {
		c.mu.Unlock()
		return false
	}
	sess.closed = true
	if c.sessions[sess.id] == sess {
		delete(c.sessions, sess.id)
	}
	proc := sess.proc
	c.mu.Unlock()
	c.s.releaseSession()
	if proc != nil {
		proc.Stop()
	}
	return true
}

func (c *conn) endSession(m *protocol.EndSession) {
	c.mu.Lock()
	if _, dup := c.seen[m.RequestID]; dup {
		c.mu.Unlock()
		c.reject(rejectFor(protocol.CodeDuplicateRequestID, false, m.RequestID, m.SessionID,
			"This requestId was already used on this connection."))
		return
	}
	sess := c.sessions[m.SessionID]
	if sess != nil && sess.busy {
		c.mu.Unlock()
		c.reject(rejectFor(protocol.CodeSessionBusy, true, m.RequestID, m.SessionID,
			"This session is processing a request; wait for its done event before ending it."))
		return
	}
	c.rememberLocked(m.RequestID)
	c.mu.Unlock()
	existed := sess != nil && c.dropSession(sess)
	c.s.log.Info("session_ended", "connId", c.id, "requestId", m.RequestID, "sessionId", m.SessionID,
		"reason", "client_request", "existed", existed)
	c.send(protocol.SessionEnded{ProtocolVersion: protocol.Version, Type: protocol.EventSessionEnded,
		RequestID: protocol.Ptr(m.RequestID), SessionID: m.SessionID, Reason: "client_request", Existed: existed})
}

func (c *conn) reapIdle(cutoff time.Time) {
	c.mu.Lock()
	var idle []*session
	for _, sess := range c.sessions {
		if !sess.busy && sess.lastUsed.Before(cutoff) {
			idle = append(idle, sess)
		}
	}
	c.mu.Unlock()
	for _, sess := range idle {
		if c.dropSession(sess) {
			c.s.log.Info("session_ended", "connId", c.id, "sessionId", sess.id, "reason", "idle_timeout")
			c.send(protocol.SessionEnded{ProtocolVersion: protocol.Version, Type: protocol.EventSessionEnded,
				SessionID: sess.id, Reason: "idle_timeout", Existed: true})
		}
	}
}

// closeGracefully flushes queued events, sends a close frame, then tears the connection down.
func (c *conn) closeGracefully(code websocket.StatusCode, reason string) {
	select {
	case c.out <- outMsg{close: true, closeCode: code, closeReason: reason}:
	case <-c.writerDone:
	}
	t := time.NewTimer(closeFlushWindow)
	defer t.Stop()
	select {
	case <-c.writerDone:
	case <-t.C:
	}
	c.teardown(errShutdown)
}

// teardown is the single cleanup path: every session of this connection is destroyed and its
// worker stopped. Conversations do not survive a disconnect.
func (c *conn) teardown(cause error) {
	c.teardownMu.Do(func() {
		c.cancel(cause)
		_ = c.ws.CloseNow()
		c.mu.Lock()
		sessions := make([]*session, 0, len(c.sessions))
		for _, sess := range c.sessions {
			sessions = append(sessions, sess)
		}
		c.mu.Unlock()
		var wg sync.WaitGroup
		for _, sess := range sessions {
			wg.Add(1)
			go func(sess *session) {
				defer wg.Done()
				c.dropSession(sess)
			}(sess)
		}
		wg.Wait()
		c.s.removeConn(c)
		c.s.log.Info("connection_closed", "connId", c.id, "reason", cause.Error(), "sessionsClosed", len(sessions))
	})
}
