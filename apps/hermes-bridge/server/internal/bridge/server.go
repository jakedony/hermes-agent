// Package bridge implements the WebSocket front end: admission, sessions, and worker lifecycle.
package bridge

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"hermes-bridge/internal/config"
	"hermes-bridge/internal/protocol"
	"hermes-bridge/internal/worker"
)

var (
	errShutdown   = errors.New("server shutting down")
	errDisconnect = errors.New("client disconnected")
	errTimeout    = errors.New("request timeout")
)

// Server owns every connection, session and worker. One instance per process.
type Server struct {
	cfg     config.Config
	log     *slog.Logger
	token   []byte
	spec    worker.Spec
	workArg []string

	root       context.Context
	rootCancel context.CancelCauseFunc
	slots      chan struct{} // concurrent request admission

	mu           sync.Mutex
	conns        map[*conn]struct{}
	sessions     int
	shuttingDown bool
	nextConnID   uint64
	hermes       hermesStatus

	requests sync.WaitGroup // in-flight request goroutines
	workers  sync.WaitGroup // live worker processes
}

type hermesStatus struct {
	Status        string    `json:"status"` // unchecked | checking | ok | <error code>
	Message       string    `json:"message,omitempty"`
	HermesVersion string    `json:"hermesVersion,omitempty"`
	Tools         []string  `json:"tools"`
	CheckedAt     time.Time `json:"checkedAt,omitempty"`
}

func New(cfg config.Config, token []byte, log *slog.Logger) *Server {
	root, cancel := context.WithCancelCause(context.Background())
	args := []string{
		"--max-iterations", strconv.Itoa(cfg.Worker.MaxIterations),
		"--run-budget-seconds", strconv.FormatFloat((cfg.RequestTimeout.Duration - cfg.CancelGrace.Duration).Seconds(), 'f', 0, 64),
		"--max-record-bytes", strconv.Itoa(cfg.MaxWorkerRecordBytes),
		"--shutdown-grace-seconds", strconv.FormatFloat(cfg.WorkerStopGrace.Seconds(), 'f', 1, 64),
	}
	if cfg.Worker.Workdir != "" {
		args = append(args, "--workdir", cfg.Worker.Workdir)
	}
	if cfg.Worker.HermesHome != "" {
		args = append(args, "--hermes-home", cfg.Worker.HermesHome)
	}
	if cfg.Worker.Memory {
		args = append(args, "--memory")
	}
	return &Server{
		cfg: cfg, log: log, token: token, workArg: args,
		spec: worker.Spec{
			Command: cfg.Worker.Command, MaxRecordBytes: cfg.MaxWorkerRecordBytes,
			StartTimeout: cfg.WorkerStartTimeout.Duration, CancelGrace: cfg.CancelGrace.Duration,
			StopGrace: cfg.WorkerStopGrace.Duration, ForwardStderr: cfg.ForwardWorkerStderr, Logger: log,
		},
		root: root, rootCancel: cancel, slots: make(chan struct{}, cfg.MaxConcurrentRequests),
		conns: map[*conn]struct{}{}, hermes: hermesStatus{Status: "unchecked", Tools: []string{}},
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /ws", s.handleWS)
	return mux
}

// startWorker launches a worker and tracks it until it exits.
func (s *Server) startWorker(ctx context.Context) (*worker.Process, worker.Ready, error) {
	p, ready, err := worker.Start(ctx, s.spec, s.workArg...)
	if err != nil {
		return nil, ready, err
	}
	s.workers.Add(1)
	go func() {
		<-p.Exited()
		s.workers.Done()
		s.log.Info("worker_exited", "workerPid", ready.PID, "stderrLines", p.StderrLines())
	}()
	s.log.Info("worker_ready", "workerPid", ready.PID, "hermesVersion", ready.HermesVersion, "tools", len(ready.Tools))
	return p, ready, nil
}

// Preflight starts one worker (no model call) to report whether Hermes is importable and has a
// provider configured. The result is exposed on /healthz.
func (s *Server) Preflight() {
	s.setHermes(hermesStatus{Status: "checking", Tools: []string{}})
	p, ready, err := s.startWorker(s.root)
	st := hermesStatus{CheckedAt: time.Now().UTC(), Tools: []string{}}
	var se *worker.StartError
	switch {
	case err == nil:
		st.Status, st.HermesVersion, st.Tools = "ok", ready.HermesVersion, append(st.Tools, ready.Tools...)
		p.Stop()
	case errors.As(err, &se):
		st.Status, st.Message = se.Code, se.Message
	default:
		st.Status, st.Message = string(protocol.CodeWorkerStartFailed), err.Error()
	}
	s.setHermes(st)
	s.log.Info("hermes_preflight", "status", st.Status, "hermesVersion", st.HermesVersion)
}

func (s *Server) setHermes(st hermesStatus) {
	s.mu.Lock()
	s.hermes = st
	s.mu.Unlock()
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	body := map[string]any{
		"status": "ok", "protocolVersion": protocol.Version,
		"connections": len(s.conns), "sessions": s.sessions, "activeRequests": len(s.slots),
		"limits": s.limits(), "hermes": s.hermes,
	}
	code := http.StatusOK
	if s.shuttingDown {
		body["status"], code = "shutting_down", http.StatusServiceUnavailable
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Server) limits() protocol.Limits {
	return protocol.Limits{
		MaxMessageBytes: s.cfg.MaxMessageBytes, MaxQuestionBytes: s.cfg.MaxQuestionBytes,
		MaxSessions: s.cfg.MaxSessions, MaxConcurrentRequests: s.cfg.MaxConcurrentRequests,
		MaxTurnsPerSession: s.cfg.MaxTurnsPerSession, RequestTimeoutMs: s.cfg.RequestTimeout.Milliseconds(),
		IdleSessionTimeoutMs: s.cfg.IdleSessionTimeout.Milliseconds(),
	}
}

// handshakeError rejects before the upgrade; the reason is logged, the response stays generic.
func (s *Server) handshakeError(w http.ResponseWriter, status int, reason string) {
	s.log.Warn("handshake_rejected", "status", status, "reason", reason)
	http.Error(w, http.StatusText(status), status)
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	if !loopbackHost(r.Host) {
		s.handshakeError(w, http.StatusForbidden, "host_not_loopback")
		return
	}
	if origin := r.Header.Get("Origin"); origin != "" && !s.originAllowed(origin) {
		s.handshakeError(w, http.StatusForbidden, "origin_not_allowed")
		return
	}
	offered := offeredSubprotocols(r)
	if !s.authorized(r, offered) {
		s.handshakeError(w, http.StatusUnauthorized, "bad_or_missing_token")
		return
	}
	if len(offered) > 0 && !contains(offered, protocol.Subprotocol) {
		s.handshakeError(w, http.StatusBadRequest, "subprotocol_missing")
		return
	}

	s.mu.Lock()
	switch {
	case s.shuttingDown:
		s.mu.Unlock()
		s.handshakeError(w, http.StatusServiceUnavailable, "shutting_down")
		return
	case len(s.conns) >= s.cfg.MaxConnections:
		s.mu.Unlock()
		s.handshakeError(w, http.StatusServiceUnavailable, "connection_limit")
		return
	}
	s.nextConnID++
	id := "c" + strconv.FormatUint(s.nextConnID, 10)
	s.mu.Unlock()

	opts := &websocket.AcceptOptions{OriginPatterns: s.originHosts()}
	if len(offered) > 0 {
		// Only the protocol name is ever selected; the token subprotocol is never echoed.
		opts.Subprotocols = []string{protocol.Subprotocol}
	}
	ws, err := websocket.Accept(w, r, opts)
	if err != nil {
		s.log.Warn("upgrade_failed", "connId", id, "error", err.Error())
		return
	}
	c := newConn(s, id, ws)
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		ws.Close(websocket.StatusGoingAway, "server shutting down")
		return
	}
	s.conns[c] = struct{}{}
	s.mu.Unlock()
	s.log.Info("connection_opened", "connId", id, "authVia", authVia(r))
	c.serve()
}

// originAllowed is an exact, case-insensitive match against the configured allowlist. There is
// deliberately no same-origin shortcut: this server serves no pages, so a matching Host header
// would only indicate DNS rebinding.
func (s *Server) originAllowed(origin string) bool {
	for _, o := range s.cfg.AllowedOrigins {
		if strings.EqualFold(strings.TrimSuffix(o, "/"), origin) {
			return true
		}
	}
	return false
}

func (s *Server) originHosts() []string {
	hosts := make([]string, 0, len(s.cfg.AllowedOrigins))
	for _, o := range s.cfg.AllowedOrigins {
		if u, err := url.Parse(o); err == nil {
			hosts = append(hosts, u.Host)
		}
	}
	return hosts
}

func (s *Server) authorized(r *http.Request, offered []string) bool {
	if tok, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); ok {
		return subtle.ConstantTimeCompare([]byte(tok), s.token) == 1
	}
	for _, p := range offered {
		if tok, ok := strings.CutPrefix(p, protocol.TokenSubprotocolPrefix); ok {
			return subtle.ConstantTimeCompare([]byte(tok), s.token) == 1
		}
	}
	return false
}

func authVia(r *http.Request) string {
	if r.Header.Get("Authorization") != "" {
		return "header"
	}
	return "subprotocol"
}

func offeredSubprotocols(r *http.Request) []string {
	var out []string
	for _, h := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, p := range strings.Split(h, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

// loopbackHost guards against DNS rebinding: the tunnel always presents a loopback Host.
func loopbackHost(hostport string) bool {
	host := hostport
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// reserveSession counts a new session against maxSessions.
func (s *Server) reserveSession() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions >= s.cfg.MaxSessions {
		return false
	}
	s.sessions++
	return true
}

func (s *Server) releaseSession() {
	s.mu.Lock()
	s.sessions--
	s.mu.Unlock()
}

func (s *Server) isShuttingDown() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.shuttingDown
}

func (s *Server) removeConn(c *conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

// RunReaper closes sessions idle longer than idleSessionTimeout until the server shuts down.
func (s *Server) RunReaper() {
	every := s.cfg.IdleSessionTimeout.Duration / 4
	if every > 30*time.Second {
		every = 30 * time.Second
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.root.Done():
			return
		case now := <-t.C:
			s.mu.Lock()
			conns := make([]*conn, 0, len(s.conns))
			for c := range s.conns {
				conns = append(conns, c)
			}
			s.mu.Unlock()
			for _, c := range conns {
				c.reapIdle(now.Add(-s.cfg.IdleSessionTimeout.Duration))
			}
		}
	}
}

// Shutdown stops admission, cancels in-flight requests (clients get SHUTTING_DOWN), closes every
// connection and stops every worker, escalating to SIGKILL. Bounded by ctx.
func (s *Server) Shutdown(ctx context.Context) {
	s.mu.Lock()
	s.shuttingDown = true
	s.mu.Unlock()
	s.log.Info("shutdown_started")
	s.rootCancel(errShutdown)

	waitGroup(ctx, &s.requests)

	s.mu.Lock()
	conns := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, c := range conns {
		wg.Add(1)
		go func(c *conn) {
			defer wg.Done()
			c.closeGracefully(websocket.StatusGoingAway, "server shutting down")
		}(c)
	}
	wg.Wait()
	if !waitGroup(ctx, &s.workers) {
		s.log.Error("shutdown_workers_outstanding")
	}
	s.log.Info("shutdown_complete")
}

func waitGroup(ctx context.Context, wg *sync.WaitGroup) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	}
}
