package bridge

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"

	"hermes-bridge/internal/config"
)

// Every test in this file runs against the fake worker in fakeworker_test.go (a mock). Real Hermes
// is exercised only by testing/run_stub_e2e.sh and `bridge-client -selftest`.

func TestMain(m *testing.M) {
	if len(os.Args) > 2 && os.Args[1] == "fake-worker" {
		runFakeWorker(os.Args[2])
		os.Exit(0)
	}
	os.Exit(m.Run())
}

const testToken = "0123456789abcdef0123456789abcdef-test-token"

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type harness struct {
	t    *testing.T
	srv  *Server
	ts   *httptest.Server
	logs *syncBuffer
	url  string
}

func newHarness(t *testing.T, mode string, mutate func(*config.Config)) *harness {
	t.Helper()
	cfg := config.Default()
	cfg.Worker.Command = []string{os.Args[0], "fake-worker", mode}
	cfg.Worker.Workdir = t.TempDir()
	cfg.TokenFile = "unused"
	cfg.AllowedOrigins = []string{"http://localhost:5173"}
	cfg.RequestTimeout = config.Duration{Duration: 10 * time.Second}
	cfg.CancelGrace = config.Duration{Duration: 2 * time.Second}
	cfg.WorkerStartTimeout = config.Duration{Duration: 10 * time.Second}
	cfg.WorkerStopGrace = config.Duration{Duration: time.Second}
	cfg.MaxWorkerRecordBytes = 1 << 20
	if mutate != nil {
		mutate(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	srv := New(cfg, []byte(testToken), slog.New(slog.NewJSONHandler(logs, nil)))
	ts := httptest.NewServer(srv.Handler())
	h := &harness{t: t, srv: srv, ts: ts, logs: logs, url: "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
		ts.Close()
		for _, pid := range h.workerPIDs() {
			if alive(pid) {
				t.Errorf("worker %d still alive after shutdown", pid)
			}
		}
	})
	return h
}

var pidRE = regexp.MustCompile(`"msg":"worker_ready","workerPid":(\d+)`)

func (h *harness) workerPIDs() []int {
	var pids []int
	for _, m := range pidRE.FindAllStringSubmatch(h.logs.String(), -1) {
		pid, _ := strconv.Atoi(m[1])
		pids = append(pids, pid)
	}
	return pids
}

func (h *harness) lastWorkerPID() int {
	pids := h.workerPIDs()
	if len(pids) == 0 {
		h.t.Fatal("no worker started")
	}
	return pids[len(pids)-1]
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func waitDead(t *testing.T, pid int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for alive(pid) {
		if time.Now().After(deadline) {
			t.Fatalf("worker %d still alive", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

type tclient struct {
	t  *testing.T
	ws *websocket.Conn
	ch chan map[string]any
}

func (h *harness) dial(opts *websocket.DialOptions) *tclient {
	h.t.Helper()
	if opts == nil {
		opts = &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + testToken}}}
	}
	ws, _, err := websocket.Dial(context.Background(), h.url, opts)
	if err != nil {
		h.t.Fatalf("dial: %v", err)
	}
	ws.SetReadLimit(8 << 20)
	c := &tclient{t: h.t, ws: ws, ch: make(chan map[string]any, 64)}
	go func() {
		defer close(c.ch)
		for {
			_, data, err := ws.Read(context.Background())
			if err != nil {
				return
			}
			var m map[string]any
			_ = json.Unmarshal(data, &m)
			c.ch <- m
		}
	}()
	if ev := c.next(); ev["type"] != "hello" {
		h.t.Fatalf("expected hello, got %v", ev)
	}
	h.t.Cleanup(func() { ws.CloseNow() })
	return c
}

func (c *tclient) next() map[string]any {
	c.t.Helper()
	select {
	case ev, ok := <-c.ch:
		if !ok {
			c.t.Fatal("connection closed while waiting for an event")
		}
		return ev
	case <-time.After(15 * time.Second):
		c.t.Fatal("timed out waiting for an event")
	}
	return nil
}

func (c *tclient) send(v any) {
	c.t.Helper()
	data, ok := v.([]byte)
	if !ok {
		data, _ = json.Marshal(v)
	}
	if err := c.ws.Write(context.Background(), websocket.MessageText, data); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

func ask(req, sess, msg string) map[string]any {
	return map[string]any{"protocolVersion": 1, "type": "ask", "requestId": req, "sessionId": sess, "message": msg}
}

// roundTrip sends an ask and returns its events, asserting the accepted -> answer|error -> done shape.
func (c *tclient) roundTrip(req, sess, msg string) (accepted, outcome, done map[string]any) {
	c.t.Helper()
	c.send(ask(req, sess, msg))
	return c.collect(req)
}

func (c *tclient) collect(req string) (accepted, outcome, done map[string]any) {
	c.t.Helper()
	for done == nil {
		ev := c.next()
		if ev["requestId"] != req {
			c.t.Fatalf("unexpected event for another request: %v", ev)
		}
		if ev["sessionId"] == nil {
			c.t.Fatalf("event without sessionId: %v", ev)
		}
		switch ev["type"] {
		case "accepted":
			if accepted != nil || outcome != nil {
				c.t.Fatalf("accepted out of order: %v", ev)
			}
			accepted = ev
		case "answer", "error":
			if outcome != nil {
				c.t.Fatalf("second outcome for %s: %v", req, ev)
			}
			if accepted == nil {
				c.t.Fatalf("outcome before accepted: %v", ev)
			}
			outcome = ev
		case "done":
			if outcome == nil {
				c.t.Fatalf("done before outcome: %v", ev)
			}
			done = ev
		default:
			c.t.Fatalf("unexpected event %v", ev)
		}
	}
	return
}

func (c *tclient) rejection() map[string]any {
	c.t.Helper()
	ev := c.next()
	if ev["type"] != "error" || ev["rejected"] != true {
		c.t.Fatalf("expected rejected error, got %v", ev)
	}
	return ev
}

func TestRoundTripHistoryAndIsolation(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	acc, ans, done := c.roundTrip("r1", "s1", "remember mango")
	if acc["newSession"] != true || ans["type"] != "answer" || done["status"] != "ok" {
		t.Fatalf("bad first turn: %v %v %v", acc, ans, done)
	}
	if _, ans, _ = c.roundTrip("r2", "s1", "recall"); ans["text"] != "you said mango" {
		t.Fatalf("history not kept: %v", ans)
	}
	acc, ans, _ = c.roundTrip("r3", "s2", "recall")
	if acc["newSession"] != true || ans["text"] != "nothing remembered" {
		t.Fatalf("sessions share history: %v %v", acc, ans)
	}
	other := h.dial(nil)
	acc, ans, _ = other.roundTrip("r1", "s1", "recall")
	if acc["newSession"] != true || ans["text"] != "nothing remembered" {
		t.Fatalf("another connection reached s1's history by guessing its label: %v %v", acc, ans)
	}
	if n := len(h.workerPIDs()); n != 3 {
		t.Fatalf("want one worker per session (3), got %d", n)
	}
}

func TestRejectedMessagesKeepConnectionUsable(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	big := strings.Repeat("q", 33<<10)
	cases := []struct {
		name    string
		payload any
		code    string
		field   string
		ids     bool
	}{
		{"not json", []byte("{nope"), "MALFORMED_MESSAGE", "", false},
		{"json array", []byte(`[1,2]`), "MALFORMED_MESSAGE", "", false},
		{"missing version", map[string]any{"type": "ask", "requestId": "a1", "sessionId": "s", "message": "hi"}, "MISSING_FIELD", "protocolVersion", true},
		{"version 2", map[string]any{"protocolVersion": 2, "type": "ask", "requestId": "a1", "sessionId": "s", "message": "hi"}, "UNSUPPORTED_PROTOCOL_VERSION", "protocolVersion", true},
		{"version string", map[string]any{"protocolVersion": "1", "type": "ask", "requestId": "a1", "sessionId": "s", "message": "hi"}, "UNSUPPORTED_PROTOCOL_VERSION", "protocolVersion", true},
		{"missing type", map[string]any{"protocolVersion": 1, "requestId": "a1", "sessionId": "s"}, "MISSING_FIELD", "type", true},
		{"unknown type", map[string]any{"protocolVersion": 1, "type": "stream", "requestId": "a1", "sessionId": "s"}, "UNSUPPORTED_MESSAGE_TYPE", "type", true},
		{"missing requestId", map[string]any{"protocolVersion": 1, "type": "ask", "sessionId": "s", "message": "hi"}, "MISSING_FIELD", "requestId", false},
		{"unsafe requestId", map[string]any{"protocolVersion": 1, "type": "ask", "requestId": "a b<script>", "sessionId": "s", "message": "hi"}, "INVALID_FIELD", "requestId", false},
		{"missing message", map[string]any{"protocolVersion": 1, "type": "ask", "requestId": "a1", "sessionId": "s"}, "MISSING_FIELD", "message", true},
		{"message wrong type", map[string]any{"protocolVersion": 1, "type": "ask", "requestId": "a1", "sessionId": "s", "message": 7}, "INVALID_FIELD", "message", true},
		{"empty message", ask("a1", "s", " \n\t "), "EMPTY_MESSAGE", "message", true},
		{"unknown field", map[string]any{"protocolVersion": 1, "type": "ask", "requestId": "a1", "sessionId": "s", "message": "hi", "stream": true}, "INVALID_FIELD", "stream", true},
		{"question too large", ask("a1", "s", big), "MESSAGE_TOO_LARGE", "message", true},
		{"frame too large", ask("a1", "s", strings.Repeat("q", 100<<10)), "MESSAGE_TOO_LARGE", "", false},
		{"context set", map[string]any{"protocolVersion": 1, "type": "ask", "requestId": "a1", "sessionId": "s", "message": "hi",
			"context": map[string]any{"documentId": "document-001", "page": 4, "selectedText": "The passage"}}, "UNSUPPORTED_CONTEXT", "context", true},
		{"context only page", map[string]any{"protocolVersion": 1, "type": "ask", "requestId": "a1", "sessionId": "s", "message": "hi",
			"context": map[string]any{"page": 1}}, "UNSUPPORTED_CONTEXT", "context", true},
		{"context bad page", map[string]any{"protocolVersion": 1, "type": "ask", "requestId": "a1", "sessionId": "s", "message": "hi",
			"context": map[string]any{"page": 0}}, "INVALID_FIELD", "context.page", true},
		{"context unknown key", map[string]any{"protocolVersion": 1, "type": "ask", "requestId": "a1", "sessionId": "s", "message": "hi",
			"context": map[string]any{"chapter": 2}}, "INVALID_FIELD", "context", true},
	}
	for _, tc := range cases {
		c.send(tc.payload)
		ev := c.rejection()
		if ev["code"] != tc.code || (tc.field != "" && ev["field"] != tc.field) {
			t.Errorf("%s: got code=%v field=%v, want %s/%s", tc.name, ev["code"], ev["field"], tc.code, tc.field)
		}
		if tc.ids && (ev["requestId"] != "a1" || ev["sessionId"] != "s") {
			t.Errorf("%s: identifiers not preserved: %v", tc.name, ev)
		}
		if !tc.ids && ev["requestId"] != nil {
			t.Errorf("%s: unsafe identifier echoed: %v", tc.name, ev)
		}
	}
	if err := c.ws.Write(context.Background(), websocket.MessageBinary, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if ev := c.rejection(); ev["code"] != "MALFORMED_MESSAGE" {
		t.Errorf("binary frame: %v", ev)
	}
	// Empty and null context mean "no context" and are admitted.
	m := ask("ok-1", "s", "hi")
	m["context"] = map[string]any{}
	c.send(m)
	if _, ans, _ := c.collect("ok-1"); ans["type"] != "answer" {
		t.Fatalf("empty context was not admitted: %v", ans)
	}
	if len(h.workerPIDs()) != 1 {
		t.Fatalf("rejected requests must not start workers: %d", len(h.workerPIDs()))
	}
}

func TestFrameBeyondHardLimitClosesConnection(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	_ = c.ws.Write(context.Background(), websocket.MessageText, bytes.Repeat([]byte("x"), 2<<20))
	for ev := range c.ch {
		t.Fatalf("expected the connection to close, got %v", ev)
	}
	if h.srv.isShuttingDown() {
		t.Fatal("server must keep running")
	}
	h.dial(nil).roundTrip("r1", "s1", "still alive")
}

func TestDuplicateRequestID(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	c.roundTrip("dup", "s1", "one")
	c.send(ask("dup", "s2", "two"))
	if ev := c.rejection(); ev["code"] != "DUPLICATE_REQUEST_ID" || ev["requestId"] != "dup" {
		t.Fatalf("got %v", ev)
	}
	// Request IDs are scoped to the connection.
	h.dial(nil).roundTrip("dup", "s1", "fine elsewhere")
}

func TestBusySessionAndServerLimits(t *testing.T) {
	h := newHarness(t, "ok", func(c *config.Config) {
		c.MaxConcurrentRequests = 1
		c.MaxSessions = 2
	})
	c := h.dial(nil)
	c.send(ask("slow", "s1", "SLEEP 700ms"))
	if ev := c.next(); ev["type"] != "accepted" {
		t.Fatalf("got %v", ev)
	}
	c.send(ask("again", "s1", "too early"))
	if ev := c.rejection(); ev["code"] != "SESSION_BUSY" || ev["retryable"] != true {
		t.Fatalf("got %v", ev)
	}
	c.send(ask("other", "s2", "parallel"))
	if ev := c.rejection(); ev["code"] != "SERVER_BUSY" {
		t.Fatalf("got %v", ev)
	}
	for ev := c.next(); ev["type"] != "done"; ev = c.next() {
	}
	c.roundTrip("other-2", "s2", "now there is room")
	c.send(ask("third", "s3", "one session too many"))
	if ev := c.rejection(); ev["code"] != "SESSION_LIMIT" {
		t.Fatalf("got %v", ev)
	}
	c.send(map[string]any{"protocolVersion": 1, "type": "end_session", "requestId": "end-1", "sessionId": "s1"})
	if ev := c.next(); ev["type"] != "session_ended" || ev["existed"] != true || ev["reason"] != "client_request" {
		t.Fatalf("got %v", ev)
	}
	c.roundTrip("third-2", "s3", "a slot was freed")
}

func TestWorkerFailuresDoNotCrashServer(t *testing.T) {
	for _, tc := range []struct{ msg, code string }{
		{"CRASH", "WORKER_CRASHED"},
		{"GARBAGE", "WORKER_PROTOCOL_ERROR"},
		{"PARTIAL", "WORKER_PROTOCOL_ERROR"},
		{"BIG", "WORKER_PROTOCOL_ERROR"},
	} {
		t.Run(tc.msg, func(t *testing.T) {
			h := newHarness(t, "ok", nil)
			c := h.dial(nil)
			c.roundTrip("r1", "s1", "remember mango")
			pid := h.lastWorkerPID()
			_, outcome, done := c.roundTrip("r2", "s1", tc.msg)
			if outcome["code"] != tc.code || outcome["sessionReset"] != true || done["status"] != "error" {
				t.Fatalf("got %v %v", outcome, done)
			}
			waitDead(t, pid)
			acc, ans, _ := c.roundTrip("r3", "s1", "recall")
			if acc["newSession"] != true || ans["text"] != "nothing remembered" {
				t.Fatalf("session should restart empty after a reset: %v %v", acc, ans)
			}
		})
	}
}

func TestHermesErrorKeepsSession(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	c.roundTrip("r1", "s1", "remember mango")
	if _, outcome, _ := c.roundTrip("r2", "s1", "FAIL"); outcome["code"] != "HERMES_ERROR" || outcome["sessionReset"] != nil {
		t.Fatalf("got %v", outcome)
	}
	if _, ans, _ := c.roundTrip("r3", "s1", "recall"); ans["text"] != "you said mango" {
		t.Fatalf("history lost after a model error: %v", ans)
	}
}

func TestTimeoutCancelsCooperativeWorker(t *testing.T) {
	h := newHarness(t, "ok", func(c *config.Config) { c.RequestTimeout = config.Duration{Duration: 300 * time.Millisecond} })
	c := h.dial(nil)
	c.roundTrip("r1", "s1", "remember mango")
	pid := h.lastWorkerPID()
	_, outcome, _ := c.roundTrip("r2", "s1", "SLEEP 10s")
	if outcome["code"] != "HERMES_TIMEOUT" || outcome["sessionReset"] != nil || outcome["retryable"] != true {
		t.Fatalf("got %v", outcome)
	}
	if !alive(pid) {
		t.Fatal("a cooperative worker should survive a cancelled turn")
	}
	if _, ans, _ := c.roundTrip("r3", "s1", "recall"); ans["text"] != "you said mango" {
		t.Fatalf("history lost: %v", ans)
	}
}

func TestTimeoutKillsUnresponsiveWorker(t *testing.T) {
	h := newHarness(t, "ok", func(c *config.Config) {
		c.RequestTimeout = config.Duration{Duration: 300 * time.Millisecond}
		c.CancelGrace = config.Duration{Duration: 200 * time.Millisecond}
	})
	c := h.dial(nil)
	c.roundTrip("r1", "s1", "warm up")
	pid := h.lastWorkerPID()
	start := time.Now()
	_, outcome, _ := c.roundTrip("r2", "s1", "HANG 30s")
	if outcome["code"] != "HERMES_TIMEOUT" || outcome["sessionReset"] != true {
		t.Fatalf("got %v", outcome)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatalf("timeout took %v", time.Since(start))
	}
	waitDead(t, pid)
}

func TestNotConfiguredIsStructured(t *testing.T) {
	h := newHarness(t, "not-configured", nil)
	h.srv.Preflight()
	resp, err := http.Get(h.ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	var body struct {
		Status string
		Hermes struct{ Status string }
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	resp.Body.Close()
	if body.Status != "ok" || body.Hermes.Status != "HERMES_NOT_CONFIGURED" {
		t.Fatalf("healthz: %+v", body)
	}
	c := h.dial(nil)
	if _, outcome, _ := c.roundTrip("r1", "s1", "hello"); outcome["code"] != "HERMES_NOT_CONFIGURED" || outcome["retryable"] != false {
		t.Fatalf("got %v", outcome)
	}
}

func TestWorkerStartFailures(t *testing.T) {
	for _, mode := range []string{"die-on-start", "noise-on-start"} {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t, mode, nil)
			c := h.dial(nil)
			if _, outcome, _ := c.roundTrip("r1", "s1", "hello"); outcome["code"] != "WORKER_START_FAILED" {
				t.Fatalf("got %v", outcome)
			}
		})
	}
}

func TestDisconnectStopsWorkers(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	c.roundTrip("r1", "idle", "hello")
	idlePID := h.lastWorkerPID()
	c.send(ask("r2", "busy", "SLEEP 20s"))
	c.next()
	deadline := time.Now().Add(5 * time.Second)
	for len(h.workerPIDs()) < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	busyPID := h.lastWorkerPID()
	c.ws.CloseNow()
	waitDead(t, idlePID)
	waitDead(t, busyPID)
	deadline = time.Now().Add(5 * time.Second)
	for {
		h.srv.mu.Lock()
		sessions, conns := h.srv.sessions, len(h.srv.conns)
		h.srv.mu.Unlock()
		if sessions == 0 && conns == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leaked state: sessions=%d conns=%d", sessions, conns)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(h.srv.slots) != 0 {
		t.Fatalf("leaked concurrency slots: %d", len(h.srv.slots))
	}
}

func TestShutdownCancelsRequestsAndStopsWorkers(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	c.roundTrip("r1", "idle", "hello")
	c.send(ask("r2", "busy", "SLEEP 30s"))
	c.next()
	time.Sleep(300 * time.Millisecond)
	pids := h.workerPIDs()
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h.srv.Shutdown(ctx)
	if time.Since(start) > 8*time.Second {
		t.Fatalf("shutdown took %v", time.Since(start))
	}
	outcome, done := c.next(), c.next()
	if outcome["code"] != "SHUTTING_DOWN" || done["type"] != "done" {
		t.Fatalf("got %v %v", outcome, done)
	}
	for range c.ch {
	}
	for _, pid := range pids {
		if alive(pid) {
			t.Fatalf("worker %d alive after shutdown", pid)
		}
	}
	_, _, err := websocket.Dial(context.Background(), h.url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer " + testToken}}})
	if err == nil {
		t.Fatal("new connections must be refused during shutdown")
	}
}

func TestIdleSessionsExpire(t *testing.T) {
	h := newHarness(t, "ok", func(c *config.Config) { c.IdleSessionTimeout = config.Duration{Duration: 300 * time.Millisecond} })
	go h.srv.RunReaper()
	c := h.dial(nil)
	c.roundTrip("r1", "s1", "remember mango")
	pid := h.lastWorkerPID()
	ev := c.next()
	if ev["type"] != "session_ended" || ev["reason"] != "idle_timeout" || ev["sessionId"] != "s1" {
		t.Fatalf("got %v", ev)
	}
	waitDead(t, pid)
	if acc, _, _ := c.roundTrip("r2", "s1", "recall"); acc["newSession"] != true {
		t.Fatalf("expired session was reused: %v", acc)
	}
}

func TestWorkerDeathWhileIdleIsReported(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	c.roundTrip("r1", "s1", "hello")
	_ = syscall.Kill(h.lastWorkerPID(), syscall.SIGKILL)
	if ev := c.next(); ev["type"] != "session_ended" || ev["reason"] != "worker_exited" {
		t.Fatalf("got %v", ev)
	}
}

func TestHistoryLimit(t *testing.T) {
	h := newHarness(t, "ok", func(c *config.Config) { c.MaxTurnsPerSession = 2 })
	c := h.dial(nil)
	c.roundTrip("r1", "s1", "one")
	c.roundTrip("r2", "s1", "two")
	c.send(ask("r3", "s1", "three"))
	if ev := c.rejection(); ev["code"] != "HISTORY_LIMIT" || ev["retryable"] != false {
		t.Fatalf("got %v", ev)
	}
}

func TestHandshakePolicy(t *testing.T) {
	h := newHarness(t, "ok", nil)
	status := func(mod func(*http.Request)) int {
		req, _ := http.NewRequest("GET", h.ts.URL+"/ws", nil)
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Sec-WebSocket-Version", "13")
		req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
		req.Header.Set("Authorization", "Bearer "+testToken)
		mod(req)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	host := strings.TrimPrefix(h.ts.URL, "http://")
	cases := []struct {
		name string
		mod  func(*http.Request)
		want int
	}{
		{"header token, no origin (native client)", func(*http.Request) {}, http.StatusSwitchingProtocols},
		{"no token", func(r *http.Request) { r.Header.Del("Authorization") }, http.StatusUnauthorized},
		{"wrong token", func(r *http.Request) { r.Header.Set("Authorization", "Bearer nope") }, http.StatusUnauthorized},
		{"allowlisted origin", func(r *http.Request) { r.Header.Set("Origin", "http://localhost:5173") }, http.StatusSwitchingProtocols},
		{"foreign origin", func(r *http.Request) { r.Header.Set("Origin", "https://evil.example") }, http.StatusForbidden},
		{"origin equal to host is not enough", func(r *http.Request) { r.Header.Set("Origin", "http://"+host) }, http.StatusForbidden},
		{"null origin", func(r *http.Request) { r.Header.Set("Origin", "null") }, http.StatusForbidden},
		{"rebinding host", func(r *http.Request) { r.Host = "evil.example:80" }, http.StatusForbidden},
		{"browser subprotocol token", func(r *http.Request) {
			r.Header.Del("Authorization")
			r.Header.Set("Origin", "http://localhost:5173")
			r.Header.Set("Sec-WebSocket-Protocol", "hermes-bridge.v1, hermes-bridge.token."+testToken)
		}, http.StatusSwitchingProtocols},
		{"subprotocol token without protocol name", func(r *http.Request) {
			r.Header.Del("Authorization")
			r.Header.Set("Sec-WebSocket-Protocol", "hermes-bridge.token."+testToken)
		}, http.StatusBadRequest},
		{"subprotocol wrong token", func(r *http.Request) {
			r.Header.Del("Authorization")
			r.Header.Set("Sec-WebSocket-Protocol", "hermes-bridge.v1, hermes-bridge.token.nope")
		}, http.StatusUnauthorized},
	}
	for _, tc := range cases {
		if got := status(tc.mod); got != tc.want {
			t.Errorf("%s: got %d want %d", tc.name, got, tc.want)
		}
	}

	ws, resp, err := websocket.Dial(context.Background(), h.url, &websocket.DialOptions{
		Subprotocols: []string{"hermes-bridge.v1", "hermes-bridge.token." + testToken},
		HTTPHeader:   http.Header{"Origin": {"http://localhost:5173"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ws.CloseNow()
	if ws.Subprotocol() != "hermes-bridge.v1" || strings.Contains(resp.Header.Get("Sec-WebSocket-Protocol"), testToken) {
		t.Fatalf("server must select only the protocol name, got %q", resp.Header.Get("Sec-WebSocket-Protocol"))
	}
}

func TestLogsCarryNoContentOrSecrets(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	c.roundTrip("r1", "s1", "remember pineapple-secret-question")
	c.roundTrip("r2", "s1", "recall")
	c.send([]byte("{garbage pineapple"))
	c.rejection()
	logs := h.logs.String()
	for _, needle := range []string{"pineapple", "you said", "ok, remembered", testToken} {
		if strings.Contains(logs, needle) {
			t.Errorf("logs contain %q", needle)
		}
	}
	for _, want := range []string{`"requestId":"r1"`, `"msg":"request_completed"`, `"durationMs"`} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs lack %s", want)
		}
	}
}

func TestConcurrentSessionsShareOneConnection(t *testing.T) {
	h := newHarness(t, "ok", func(c *config.Config) { c.MaxConcurrentRequests = 4 })
	c := h.dial(nil)
	for i := 0; i < 4; i++ {
		c.send(ask("r"+strconv.Itoa(i), "s"+strconv.Itoa(i), "SLEEP 200ms"))
	}
	doneCount := 0
	for doneCount < 4 {
		if ev := c.next(); ev["type"] == "done" {
			doneCount++
		}
	}
}

func TestConfigRejectsUnsafeSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		mod  func(*config.Config)
	}{
		{"wildcard origin", func(c *config.Config) { c.AllowedOrigins = []string{"*"} }},
		{"pattern origin", func(c *config.Config) { c.AllowedOrigins = []string{"http://*.example.com"} }},
		{"public bind", func(c *config.Config) { c.ListenAddr = "0.0.0.0:8765" }},
		{"no worker", func(c *config.Config) { c.Worker.Command = nil }},
	} {
		cfg := config.Default()
		cfg.Worker.Command = []string{"python3", "worker.py"}
		tc.mod(&cfg)
		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: accepted", tc.name)
		}
	}
}
