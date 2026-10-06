package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func testConfig(dbPath string) Config {
	cfg := Config{
		Listen: "127.0.0.1:0",
		RoomID: "lab",
		DBPath: dbPath,
		Principals: map[string]Principal{
			"human-jacob": {Kind: "human", TokenSHA256: hashToken("human-secret")},
			"hermes-pc":   {Kind: "agent", MachineID: "pc", Capabilities: []string{"pc-network"}, TokenSHA256: hashToken("pc-secret")},
			"hermes-vm":   {Kind: "agent", MachineID: "vm", Capabilities: []string{"vm-local"}, TokenSHA256: hashToken("vm-secret")},
		},
		Limits: Limits{
			RootDeadlineSec: 180, ChildDeadlineSec: 120, MaxChildTasks: 3, MaxAttempts: 2,
			LeaseSec: 30, RetryBackoffSec: 2, MaxExecutionSec: 150,
			IdempotencyRetentionSec: 3600, EventBuffer: 2,
		},
	}
	applyDefaults(&cfg)
	return cfg
}

func newTest(t *testing.T) (*Engine, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: time.UnixMilli(1_700_000_000_000)}
	path := filepath.Join(t.TempDir(), "hub.db")
	store, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return NewEngine(store, testConfig(path), clock), clock
}

func (e *Engine) call(actorID, typ, taskID, requestID string, payload any) Result {
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	p := e.cfg.Principals[actorID]
	actor := Actor{ID: actorID, Kind: p.Kind, MachineID: p.MachineID, Capabilities: p.Capabilities}
	res, err := e.Handle(actor, Envelope{V: 1, Type: typ, RequestID: requestID, RoomID: "lab", TaskID: taskID, Payload: raw})
	if err != nil {
		panic(err)
	}
	return res
}

func bodyMap(t *testing.T, res Result) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(res.Wire, &m); err != nil {
		t.Fatalf("body: %s (%v)", res.Wire, err)
	}
	return m
}

func createRoot(t *testing.T, e *Engine, requestID string) string {
	t.Helper()
	res := e.call("human-jacob", "task.create", "", requestID, map[string]any{
		"assigned_to": "hermes-pc", "objective": "why is the vm service unreachable",
		"context": map[string]any{"target": "127.0.0.2:18080"}, "profile": "pc-net", "timeout_sec": 180,
	})
	if !res.OK {
		t.Fatalf("create: %+v %s", res.Code, res.Message)
	}
	return bodyMap(t, res)["task_id"].(string)
}

func claim(t *testing.T, e *Engine, actor, taskID, requestID string) string {
	t.Helper()
	res := e.call(actor, "task.claim", taskID, requestID, map[string]any{})
	if !res.OK {
		t.Fatalf("claim: %s %s", res.Code, res.Message)
	}
	return bodyMap(t, res)["attempt_id"].(string)
}

func TestLostCreateAcknowledgementIsIdempotent(t *testing.T) {
	e, _ := newTest(t)
	first := createRoot(t, e, "req-create-1")
	second := createRoot(t, e, "req-create-1")
	if first != second {
		t.Fatalf("retry created a second task %s %s", first, second)
	}
	res := e.call("human-jacob", "task.list", "", "req-list-1", map[string]any{})
	tasks := bodyMap(t, res)["tasks"].([]any)
	if len(tasks) != 1 {
		t.Fatalf("tasks: %d", len(tasks))
	}
}

func TestRequestIDReuseWithDifferentPayloadConflicts(t *testing.T) {
	e, _ := newTest(t)
	createRoot(t, e, "req-create-1")
	res := e.call("human-jacob", "task.create", "", "req-create-1", map[string]any{
		"assigned_to": "hermes-pc", "objective": "a different question",
	})
	if res.OK || res.Code != "conflict" {
		t.Fatalf("got %s", res.Code)
	}
}

func TestRepeatedClaimDoesNotCreateAnotherAttempt(t *testing.T) {
	e, _ := newTest(t)
	id := createRoot(t, e, "c1")
	a1 := claim(t, e, "hermes-pc", id, "claim-1")
	a2 := claim(t, e, "hermes-pc", id, "claim-2")
	if a1 != a2 {
		t.Fatalf("attempts %s %s", a1, a2)
	}
	got := e.call("hermes-pc", "task.get", id, "g1", map[string]any{})
	attempts := bodyMap(t, got)["attempts"].([]any)
	if len(attempts) != 1 {
		t.Fatalf("attempt rows %d", len(attempts))
	}
}

func TestCompleteRetryAndStaleAttempt(t *testing.T) {
	e, clock := newTest(t)
	root := createRoot(t, e, "c1")
	attempt := claim(t, e, "hermes-pc", root, "claim-1")
	e.call("hermes-pc", "attempt.renew", root, "ren-1", map[string]any{"attempt_id": attempt})
	child := e.call("hermes-pc", "task.create", "", "child-1", map[string]any{
		"parent_task_id": root, "assigned_to": "hermes-vm", "objective": "inspect sockets",
		"context": map[string]any{"hint": "pc timed out"}, "profile": "vm-local", "timeout_sec": 120,
	})
	if !child.OK {
		t.Fatal(child.Message)
	}
	childID := bodyMap(t, child)["task_id"].(string)
	vmAttempt := claim(t, e, "hermes-vm", childID, "vm-claim")
	payload := map[string]any{
		"attempt_id": vmAttempt, "summary": "nothing is listening on the reachable address",
		"machine_id": "vm", "limitations": []string{"firewall not ruled out"},
		"evidence": []map[string]string{{"operation": "listening_sockets", "source": "vm", "observed_at": "t", "excerpt": "127.0.0.1:18080"}},
	}
	done := e.call("hermes-vm", "task.complete", childID, "done-1", payload)
	if !done.OK {
		t.Fatal(done.Message)
	}
	again := e.call("hermes-vm", "task.complete", childID, "done-1", payload)
	if !again.OK || !again.Replay {
		t.Fatalf("retry ok=%v replay=%v %s", again.OK, again.Replay, again.Message)
	}
	clock.Advance(31 * time.Second)
	if _, err := e.Sweep(); err != nil {
		t.Fatal(err)
	}
	// The child is already completed, so expiry does not reopen it.
	// Lease loss requeues with the retry backoff; wait it out, then
	// reject a completion that names the abandoned attempt.
	clock.Advance(3 * time.Second)
	rootAgain := e.call("hermes-pc", "task.claim", root, "claim-root-2", map[string]any{})
	if !rootAgain.OK {
		t.Fatal(rootAgain.Message)
	}
	newAttempt := bodyMap(t, rootAgain)["attempt_id"].(string)
	if newAttempt == attempt {
		t.Fatal("expected a new attempt after lease loss")
	}
	stale := e.call("hermes-pc", "task.complete", root, "stale-1", map[string]any{
		"attempt_id": attempt, "summary": "late", "machine_id": "pc",
		"evidence": []map[string]string{{"operation": "tcp_probe", "source": "pc", "observed_at": "t", "excerpt": "timeout"}},
	})
	if stale.OK || stale.Code != "stale" {
		t.Fatalf("stale code %s", stale.Code)
	}
	got := e.call("hermes-pc", "task.get", root, "g-root", map[string]any{})
	if bodyMap(t, got)["task"].(map[string]any)["state"] != "running" {
		t.Fatalf("stale completion changed state: %v", bodyMap(t, got)["task"])
	}
	hist := e.call("human-jacob", "task.history", root, "hist-stale", map[string]any{})
	if !hist.OK || !strings.Contains(string(hist.Wire), "attempt.stale_result") {
		t.Fatalf("stale result was not audited: %s", hist.Wire)
	}
}

func TestLeaseExpiryRequeueAndExhaustion(t *testing.T) {
	e, clock := newTest(t)
	id := createRoot(t, e, "c1")
	claim(t, e, "hermes-pc", id, "claim-1")
	clock.Advance(31 * time.Second)
	events, err := e.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(events, "task.requeued") {
		t.Fatalf("events %#v", events)
	}
	got := e.call("human-jacob", "task.get", id, "g1", map[string]any{})
	task := bodyMap(t, got)["task"].(map[string]any)
	if task["state"] != "queued" {
		t.Fatalf("state %v", task["state"])
	}
	early := e.call("hermes-pc", "task.claim", id, "claim-early", map[string]any{})
	if early.Code != "not_ready" {
		t.Fatalf("early %s", early.Code)
	}
	clock.Advance(3 * time.Second)
	// not_ready is not a sticky idempotency record: the same request id can succeed later.
	retried := e.call("hermes-pc", "task.claim", id, "claim-early", map[string]any{})
	if !retried.OK || retried.Replay {
		t.Fatalf("not_ready was sticky: ok=%v replay=%v code=%s", retried.OK, retried.Replay, retried.Code)
	}
	claim(t, e, "hermes-pc", id, "claim-2")
	clock.Advance(31 * time.Second)
	events, err = e.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(events, "task.failed") {
		t.Fatalf("expected exhaustion, %#v", events)
	}
}

func TestFailureClassRetryPolicy(t *testing.T) {
	cases := []struct {
		class string
		state string
	}{
		{"transient", "queued"},
		{"crash", "queued"},
		{"lease_lost", "queued"},
		{"unauthorized", "failed"},
		{"cancelled", "failed"},
		{"deadline", "failed"},
	}
	for _, tc := range cases {
		t.Run(tc.class, func(t *testing.T) {
			e, clock := newTest(t)
			id := createRoot(t, e, "c1")
			attempt := claim(t, e, "hermes-pc", id, "claim-1")
			res := e.call("hermes-pc", "task.fail", id, "f1", map[string]any{
				"attempt_id": attempt, "class": tc.class, "message": "x",
			})
			if !res.OK || bodyMap(t, res)["state"] != tc.state {
				t.Fatalf("%s -> %s %s", tc.class, res.Code, res.Wire)
			}
			if tc.state != "queued" {
				return
			}
			early := e.call("hermes-pc", "task.claim", id, "again", map[string]any{})
			if early.Code != "not_ready" {
				t.Fatalf("backoff %s", early.Code)
			}
			clock.Advance(3 * time.Second)
			claim(t, e, "hermes-pc", id, "again-2")
		})
	}
}

func TestDeadlineSupersedesLiveLease(t *testing.T) {
	e, clock := newTest(t)
	id := createRoot(t, e, "c1")
	attempt := claim(t, e, "hermes-pc", id, "claim")
	for _, req := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		clock.Advance(25 * time.Second)
		renewed := e.call("hermes-pc", "attempt.renew", id, req, map[string]any{"attempt_id": attempt})
		if !renewed.OK {
			t.Fatalf("renew %s: %s %s", req, renewed.Code, renewed.Message)
		}
	}
	clock.Advance(10 * time.Second)
	events, err := e.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(events, "task.timed_out") || hasEvent(events, "task.requeued") {
		t.Fatalf("events %#v", events)
	}
	got := e.call("human-jacob", "task.get", id, "g", map[string]any{})
	if bodyMap(t, got)["task"].(map[string]any)["state"] != "timed_out" {
		t.Fatal(bodyMap(t, got)["task"])
	}
}

func TestUnclaimedRootTimesOut(t *testing.T) {
	e, clock := newTest(t)
	id := createRoot(t, e, "c1")
	clock.Advance(181 * time.Second)
	events, err := e.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(events, "task.timed_out") {
		t.Fatalf("events %#v", events)
	}
	got := e.call("human-jacob", "task.get", id, "g", map[string]any{})
	task := bodyMap(t, got)["task"].(map[string]any)
	if task["state"] != "timed_out" || task["error_class"] != "deadline" {
		t.Fatal(task)
	}
}

func TestSlowConsumerCancelsSession(t *testing.T) {
	e, _ := newTest(t)
	session, err := e.Bind(context.Background(), Actor{ID: "hermes-pc", Kind: "agent", MachineID: "pc"}, false)
	if err != nil {
		t.Fatal(err)
	}
	e.Publish([]Event{
		{ID: "e1", Type: "task.created"},
		{ID: "e2", Type: "task.started"},
		{ID: "e3", Type: "task.requeued"},
	})
	select {
	case <-session.Ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("full event buffer did not cancel the session")
	}
}

func TestDeadlineAndCancellationRace(t *testing.T) {
	e, clock := newTest(t)
	id := createRoot(t, e, "c1")
	attempt := claim(t, e, "hermes-pc", id, "claim-1")
	clock.Advance(181 * time.Second)
	events, err := e.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(events, "task.timed_out") {
		t.Fatalf("events %#v", events)
	}
	late := e.call("hermes-pc", "task.complete", id, "late", map[string]any{
		"attempt_id": attempt, "summary": "too late", "machine_id": "pc",
	})
	if late.OK {
		t.Fatal("late completion accepted")
	}

	e2, _ := newTest(t)
	id = createRoot(t, e2, "c1")
	attempt = claim(t, e2, "hermes-pc", id, "claim-1")
	started := make(chan struct{})
	release := make(chan struct{})
	e2.hold = func(kind string) {
		if kind == "task.complete" {
			close(started)
			<-release
		}
	}
	completeDone := make(chan Result, 1)
	go func() {
		completeDone <- e2.call("hermes-pc", "task.complete", id, "done", map[string]any{
			"attempt_id": attempt, "summary": "reachable locally only", "machine_id": "pc",
		})
	}()
	<-started
	cancelDone := make(chan Result, 1)
	go func() {
		cancelDone <- e2.call("human-jacob", "task.cancel", id, "cancel-1", map[string]any{"reason": "stop"})
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	cancelled := <-cancelDone
	completeRes := <-completeDone
	if !completeRes.OK {
		t.Fatalf("complete %s", completeRes.Message)
	}
	if cancelled.OK || cancelled.Code != "conflict" {
		t.Fatalf("cancel during complete: ok=%v code=%s", cancelled.OK, cancelled.Code)
	}
	final := e2.call("human-jacob", "task.get", id, "g", map[string]any{})
	if bodyMap(t, final)["task"].(map[string]any)["state"] != "completed" {
		t.Fatalf("state %v", bodyMap(t, final)["task"])
	}
}

func TestCancelThenCompleteIsStale(t *testing.T) {
	e, _ := newTest(t)
	id := createRoot(t, e, "c1")
	attempt := claim(t, e, "hermes-pc", id, "claim-1")
	cancelled := e.call("human-jacob", "task.cancel", id, "cancel-1", map[string]any{"reason": "stop"})
	if !cancelled.OK {
		t.Fatal(cancelled.Message)
	}
	late := e.call("hermes-pc", "task.complete", id, "done", map[string]any{
		"attempt_id": attempt, "summary": "after cancel", "machine_id": "pc",
	})
	if late.OK || late.Code != "stale" {
		t.Fatalf("code %s", late.Code)
	}
	got := e.call("human-jacob", "task.get", id, "g", map[string]any{})
	if bodyMap(t, got)["task"].(map[string]any)["state"] != "cancelled" {
		t.Fatal(bodyMap(t, got)["task"])
	}
}

func TestWorkerCannotDelegateAndSpoofedActorIsIgnored(t *testing.T) {
	e, _ := newTest(t)
	id := createRoot(t, e, "c1")
	claim(t, e, "hermes-pc", id, "claim-1")
	spoof := e.call("hermes-vm", "task.create", "", "spoof", map[string]any{
		"parent_task_id": id, "assigned_to": "hermes-pc", "objective": "pretend",
		"requested_by": "human-jacob", "actor": "human-jacob",
	})
	if spoof.OK || spoof.Code != "forbidden" {
		t.Fatalf("spoof %s %s", spoof.Code, spoof.Message)
	}
	hist := e.call("human-jacob", "task.history", id, "h1", map[string]any{})
	raw := string(hist.Wire)
	if strings.Contains(raw, "human-jacob") && strings.Contains(raw, "spoof") {
		t.Fatal("spoofed actor was recorded")
	}
}

func TestInvalidInputIsNotRetried(t *testing.T) {
	e, _ := newTest(t)
	id := createRoot(t, e, "c1")
	attempt := claim(t, e, "hermes-pc", id, "claim-1")
	res := e.call("hermes-pc", "task.fail", id, "f1", map[string]any{
		"attempt_id": attempt, "class": "invalid_input", "message": "bad profile",
	})
	if !res.OK || bodyMap(t, res)["state"] != "failed" {
		t.Fatalf("%s %s", res.Code, res.Wire)
	}
}

func TestDatabaseFailureDoesNotAccept(t *testing.T) {
	e, _ := newTest(t)
	e.store.failBegin = context.DeadlineExceeded
	res, err := e.Handle(Actor{ID: "human-jacob", Kind: "human"}, Envelope{
		V: 1, Type: "task.create", RequestID: "c1", RoomID: "lab",
		Payload: json.RawMessage(`{"assigned_to":"hermes-pc","objective":"x"}`),
	})
	if err == nil || res.OK {
		t.Fatalf("accepted despite database failure: %+v %v", res, err)
	}
	e2 := e
	ok := e2.call("human-jacob", "task.list", "", "list", map[string]any{})
	if n := len(bodyMap(t, ok)["tasks"].([]any)); n != 0 {
		t.Fatalf("tasks stored: %d", n)
	}
}

func TestRestartPreservesLeaseUntilExpiry(t *testing.T) {
	clock := &fakeClock{t: time.UnixMilli(1_700_000_000_000)}
	dir := tDir(t)
	path := filepath.Join(dir, "hub.db")
	store, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	e := NewEngine(store, testConfig(path), clock)
	id := createRoot(t, e, "c1")
	claim(t, e, "hermes-pc", id, "claim-1")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e = NewEngine(store, testConfig(path), clock)
	events, err := e.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("restart requeued early: %#v", events)
	}
	got := e.call("human-jacob", "task.get", id, "g", map[string]any{})
	if bodyMap(t, got)["task"].(map[string]any)["state"] != "running" {
		t.Fatal(bodyMap(t, got)["task"])
	}
	clock.Advance(31 * time.Second)
	events, err = e.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(events, "task.requeued") {
		t.Fatalf("events %#v", events)
	}
}

func TestClockStepBackExpiresLeases(t *testing.T) {
	e, clock := newTest(t)
	id := createRoot(t, e, "c1")
	claim(t, e, "hermes-pc", id, "claim-1")
	clock.Advance(-5 * time.Second)
	events, err := e.Sweep()
	if err != nil {
		t.Fatal(err)
	}
	if !hasEvent(events, "attempt.abandoned") {
		t.Fatalf("events %#v", events)
	}
}

func TestChildDeadlineIsCappedByRoot(t *testing.T) {
	e, clock := newTest(t)
	root := createRoot(t, e, "c1")
	attempt := claim(t, e, "hermes-pc", root, "claim-1")
	// The 30s lease would otherwise expire during this 100s jump and the
	// next command would sweep the root back to queued.
	for _, req := range []string{"ren-a", "ren-b", "ren-c", "ren-d"} {
		clock.Advance(25 * time.Second)
		renewed := e.call("hermes-pc", "attempt.renew", root, req, map[string]any{"attempt_id": attempt})
		if !renewed.OK {
			t.Fatal(renewed.Message)
		}
	}
	child := e.call("hermes-pc", "task.create", "", "child", map[string]any{
		"parent_task_id": root, "assigned_to": "hermes-vm", "objective": "look",
		"timeout_sec": 120,
	})
	if !child.OK {
		t.Fatal(child.Message)
	}
	deadline := int64(bodyMap(t, child)["deadline_at_ms"].(float64))
	rootDeadline := int64(bodyMap(t, e.call("human-jacob", "task.get", root, "g", map[string]any{}))["task"].(map[string]any)["deadline_at_ms"].(float64))
	if deadline > rootDeadline {
		t.Fatalf("child %d root %d", deadline, rootDeadline)
	}
	if deadline-clock.Now().UnixMilli() > 90_000 {
		t.Fatalf("child was not capped: %d", deadline-clock.Now().UnixMilli())
	}
}

func TestOneRootAndChildBudget(t *testing.T) {
	e, _ := newTest(t)
	root := createRoot(t, e, "c1")
	second := e.call("human-jacob", "task.create", "", "c2", map[string]any{
		"assigned_to": "hermes-pc", "objective": "another",
	})
	if second.Code != "capacity" {
		t.Fatalf("second root %s", second.Code)
	}
	claim(t, e, "hermes-pc", root, "claim")
	for i := 0; i < 3; i++ {
		res := e.call("hermes-pc", "task.create", "", "k"+string(rune('a'+i)), map[string]any{
			"parent_task_id": root, "assigned_to": "hermes-vm", "objective": "child",
		})
		if !res.OK {
			t.Fatalf("child %d %s", i, res.Message)
		}
	}
	over := e.call("hermes-pc", "task.create", "", "kover", map[string]any{
		"parent_task_id": root, "assigned_to": "hermes-vm", "objective": "too many",
	})
	if over.Code != "budget" {
		t.Fatalf("budget %s", over.Code)
	}
}

func TestDeskIsLocalShellWithoutSecrets(t *testing.T) {
	e, _ := newTest(t)
	srv := httptest.NewServer(NewServer(e, nil).Handler())
	t.Cleanup(srv.Close)
	res, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}
	if ct := res.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content-type %s", ct)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	page := string(body)
	for _, want := range []string{
		"hermes-pc", "hermes-vm", "human-jacob", "WORKSPACE", "Lab",
		"Give to agent", "Take control", "Diagnostic workspace, not a remote desktop.",
		"sessionStorage", `profile: "pc-net"`, "timeout_sec: 180",
		"desk://", "You are directing", "You are watching",
	} {
		if !strings.Contains(page, want) {
			t.Fatalf("desk missing %s", want)
		}
	}
	if strings.Contains(page, "human-secret") || strings.Contains(page, "pc-secret") || strings.Contains(page, "vm-secret") {
		t.Fatal("desk embedded a test token")
	}
	if strings.Contains(page, `class="agent`) || strings.Contains(page, "class='agent") {
		t.Fatal("desk reused class agent on a shared selector")
	}
}

func TestMalformedFrameAndHTTPAuth(t *testing.T) {
	e, _ := newTest(t)
	srv := httptest.NewServer(NewServer(e, nil).Handler())
	t.Cleanup(srv.Close)
	res, err := http.Post(srv.URL+"/v1/command", "application/json", strings.NewReader("{"))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d", res.StatusCode)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/command", strings.NewReader(`{"v":1,"type":"task.list","request_id":"r","room_id":"lab","payload":{}}`))
	req.Header.Set("Authorization", "Bearer human-secret")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("auth status %d", res.StatusCode)
	}
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/v1/tasks?token=human-secret", nil)
	req.Header.Set("Authorization", "Bearer human-secret")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("query token status %d", res.StatusCode)
	}
}

func TestSessionTakeoverDoesNotDuplicateAttempt(t *testing.T) {
	e, _ := newTest(t)
	srv := httptest.NewServer(NewServer(e, nil).Handler())
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	ctx := context.Background()
	first, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer pc-secret"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(websocket.StatusNormalClosure, "done")
	if err := writeJSON(ctx, first, Envelope{V: 1, Type: "agent.hello", RequestID: "h1", RoomID: "lab", Payload: json.RawMessage(`{"takeover":true}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := readJSON(ctx, first); err != nil {
		t.Fatal(err)
	}
	id := createRoot(t, e, "c1")
	if err := writeJSON(ctx, first, Envelope{V: 1, Type: "task.claim", RequestID: "claim", RoomID: "lab", TaskID: id, Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	ack, err := readAck(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Type != "ack" {
		t.Fatalf("claim ack %s %s", ack.Type, ack.Payload)
	}
	second, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer pc-secret"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(websocket.StatusNormalClosure, "done")
	if err := writeJSON(ctx, second, Envelope{V: 1, Type: "agent.hello", RequestID: "h2", RoomID: "lab", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	rejected, err := readJSON(ctx, second)
	if err != nil {
		t.Fatal(err)
	}
	if rejected.Type != "error" {
		t.Fatalf("expected conflict, got %s", rejected.Type)
	}
	third, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer pc-secret"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer third.Close(websocket.StatusNormalClosure, "done")
	if err := writeJSON(ctx, third, Envelope{V: 1, Type: "agent.hello", RequestID: "h3", RoomID: "lab", Payload: json.RawMessage(`{"takeover":true}`)}); err != nil {
		t.Fatal(err)
	}
	hello, err := readJSON(ctx, third)
	if err != nil {
		t.Fatal(err)
	}
	if hello.Type != "ack" {
		t.Fatalf("takeover %s %s", hello.Type, hello.Payload)
	}
	got := e.call("human-jacob", "task.get", id, "g", map[string]any{})
	attempts := bodyMap(t, got)["attempts"].([]any)
	if len(attempts) != 1 {
		t.Fatalf("attempts after takeover %d", len(attempts))
	}
}

func TestUnsupportedVersion(t *testing.T) {
	e, _ := newTest(t)
	res, err := e.Handle(Actor{ID: "human-jacob", Kind: "human"}, Envelope{V: 99, Type: "task.list", RequestID: "r", RoomID: "lab", Payload: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if res.Code != "bad_version" {
		t.Fatalf("%s", res.Code)
	}
}

func TestHTTPErrorStatuses(t *testing.T) {
	e, _ := newTest(t)
	srv := httptest.NewServer(NewServer(e, nil).Handler())
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/command", strings.NewReader(`{"v":1,"type":"task.create","request_id":"bad","room_id":"lab","payload":{}}`))
	req.Header.Set("Authorization", "Bearer human-secret")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("business status %d", res.StatusCode)
	}
	var env Envelope
	if err := json.NewDecoder(res.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if env.Type != "error" || env.V != 1 || !strings.Contains(string(env.Payload), "bad_request") {
		t.Fatalf("business envelope %+v", env)
	}

	e.store.failBegin = context.DeadlineExceeded
	req, _ = http.NewRequest(http.MethodPost, srv.URL+"/v1/command", strings.NewReader(`{"v":1,"type":"task.list","request_id":"db","room_id":"lab","payload":{}}`))
	req.Header.Set("Authorization", "Bearer human-secret")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("db status %d", res.StatusCode)
	}
	if err := json.NewDecoder(res.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if env.Type != "error" || !strings.Contains(string(env.Payload), "internal") {
		t.Fatalf("db envelope %+v", env)
	}
}

func TestStoreDurabilitySetup(t *testing.T) {
	e, _ := newTest(t)
	var mode string
	var syncN, fkN, busyN, ver int
	if err := e.store.db.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil || strings.ToLower(mode) != "wal" {
		t.Fatalf("journal %q %v", mode, err)
	}
	if err := e.store.db.QueryRow(`PRAGMA synchronous`).Scan(&syncN); err != nil || syncN != 2 {
		t.Fatalf("synchronous %d %v", syncN, err)
	}
	if err := e.store.db.QueryRow(`PRAGMA foreign_keys`).Scan(&fkN); err != nil || fkN != 1 {
		t.Fatalf("foreign_keys %d %v", fkN, err)
	}
	if err := e.store.db.QueryRow(`PRAGMA busy_timeout`).Scan(&busyN); err != nil || busyN < 5000 {
		t.Fatalf("busy_timeout %d %v", busyN, err)
	}
	if err := e.store.db.QueryRow(`SELECT version FROM schema_migrations`).Scan(&ver); err != nil || ver != 1 {
		t.Fatalf("schema_migrations %d %v", ver, err)
	}
}

func TestLoadConfigKeepsProductionDefaults(t *testing.T) {
	dir := t.TempDir()
	body, err := json.Marshal(map[string]any{
		"listen":  "127.0.0.1:9",
		"room_id": "lab",
		"db_path": filepath.Join(dir, "hub.db"),
		"principals": map[string]any{
			"human-jacob": map[string]any{"kind": "human", "token_sha256": hashToken("x")},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "hub.json")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ManualClock {
		t.Fatal("manual clock defaults on")
	}
	if cfg.Limits.RootDeadlineSec != 180 || cfg.Limits.ChildDeadlineSec != 120 || cfg.Limits.LeaseSec != 30 || cfg.Limits.MaxAttempts != 2 || cfg.Limits.MaxChildTasks != 3 {
		t.Fatalf("defaults changed: %+v", cfg.Limits)
	}
}

func TestAdvanceEndpointAbsentWithoutManualClock(t *testing.T) {
	e, _ := newTest(t)
	srv := httptest.NewServer(NewServer(e, nil).Handler())
	t.Cleanup(srv.Close)
	res, err := http.Post(srv.URL+"/v1/test/advance", "application/json", strings.NewReader(`{"advance_ms":31000}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d", res.StatusCode)
	}
}

func TestManualClockAdvanceExpiresLease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hub.db")
	store, err := openStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	clock := newOffsetClock()
	eng := NewEngine(store, testConfig(path), clock)
	srv := NewServer(eng, nil)
	srv.manual = clock
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	id := createRoot(t, eng, "c-clock")
	attempt := claim(t, eng, "hermes-pc", id, "claim-clock")
	before := eng.call("human-jacob", "task.get", id, "g-before", map[string]any{})
	if bodyMap(t, before)["task"].(map[string]any)["state"] != "running" {
		t.Fatal(bodyMap(t, before)["task"])
	}
	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/test/advance", strings.NewReader(`{"advance_ms":31000}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer human-secret")
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("advance status %d", res.StatusCode)
	}
	got := eng.call("human-jacob", "task.get", id, "g-after", map[string]any{})
	task := bodyMap(t, got)["task"].(map[string]any)
	if task["state"] != "queued" {
		t.Fatalf("lease did not expire on the test clock: %v", task)
	}
	attempts := bodyMap(t, got)["attempts"].([]any)
	if len(attempts) != 1 || attempts[0].(map[string]any)["attempt_id"] != attempt || attempts[0].(map[string]any)["state"] != "lost" {
		t.Fatalf("attempt %+v", attempts)
	}
}

func hasEvent(events []Event, typ string) bool {
	for _, ev := range events {
		if ev.Type == typ {
			return true
		}
	}
	return false
}

func tDir(t *testing.T) string {
	t.Helper()
	return t.TempDir()
}

func writeJSON(ctx context.Context, conn *websocket.Conn, env Envelope) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, raw)
}

func readJSON(ctx context.Context, conn *websocket.Conn) (Envelope, error) {
	readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(readCtx)
	if err != nil {
		return Envelope{}, err
	}
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// readAck skips broadcast events. A command ack and the events it caused
// are written on the same socket and either may arrive first.
func readAck(ctx context.Context, conn *websocket.Conn) (Envelope, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		env, err := readJSON(ctx, conn)
		if err != nil {
			return Envelope{}, err
		}
		if env.Type == "ack" || env.Type == "error" {
			return env, nil
		}
		if time.Now().After(deadline) {
			return env, context.DeadlineExceeded
		}
	}
}
