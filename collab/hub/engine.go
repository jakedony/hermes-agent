package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// Clock is injectable so lease and deadline tests do not sleep.
type Clock interface {
	Now() time.Time
}

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// offsetClock is wall time plus a test-only delta. Production uses wallClock.
// POST /v1/test/advance moves it, and only when config manual_clock is set.
type offsetClock struct {
	mu    sync.Mutex
	delta time.Duration
}

func newOffsetClock() *offsetClock {
	return &offsetClock{}
}

func (c *offsetClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return time.Now().Add(c.delta)
}

func (c *offsetClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.delta += d
	c.mu.Unlock()
}

// Result is the acknowledgement of one command.
type Result struct {
	OK      bool
	Code    string
	Message string
	Body    any
	Events  []Event
	Replay  bool
	Wire    json.RawMessage
}

type coded struct {
	Code    string
	Message string
}

func (c *coded) Error() string { return c.Message }

func refuse(code, message string) *coded { return &coded{Code: code, Message: message} }

// Session is one live WebSocket for an agent identity.
type Session struct {
	ID      string
	AgentID string
	Events  chan Event
	Acks    chan []byte
	Ctx     context.Context
	cancel  context.CancelFunc
}

// Engine applies task and attempt policy. The database is the authority.
type Engine struct {
	store *Store
	cfg   Config
	clock Clock

	mu       sync.Mutex
	sessions map[string]*Session
	hold     func(string)
}

func NewEngine(store *Store, cfg Config, clock Clock) *Engine {
	if clock == nil {
		clock = wallClock{}
	}
	applyDefaults(&cfg)
	return &Engine{store: store, cfg: cfg, clock: clock, sessions: map[string]*Session{}}
}

func (e *Engine) now() int64 { return e.clock.Now().UnixMilli() }

func (e *Engine) Handle(actor Actor, cmd Envelope) (Result, error) {
	cmd.Payload = normalizePayload(cmd.Payload)
	if cmd.V != protocolVersion {
		return Result{Code: "bad_version", Message: "unsupported protocol version"}, nil
	}
	if !requestIDRe.MatchString(cmd.RequestID) {
		return Result{Code: "bad_request", Message: "request_id is missing or malformed"}, nil
	}
	if cmd.RoomID != e.cfg.RoomID {
		return Result{Code: "forbidden", Message: "room is not served by this hub"}, nil
	}
	if len(cmd.Payload) > e.cfg.Limits.MaxFrameBytes {
		return Result{Code: "too_large", Message: "payload exceeds the frame limit"}, nil
	}
	hash := commandHash(cmd)
	var out Result
	err := e.store.withTx(func(tx *sql.Tx) error {
		prev, found, err := loadRequest(tx, actor.ID, cmd.RequestID)
		if err != nil {
			return err
		}
		if found {
			if prev.hash != hash {
				out = Result{Code: "conflict", Message: "request_id was reused with a different payload"}
				return nil
			}
			out = Result{OK: prev.ok, Code: prev.code, Message: prev.message, Wire: prev.body, Replay: true}
			return nil
		}
		if err := noteClock(tx, e.now()); err != nil {
			return err
		}
		events, err := e.sweep(tx)
		if err != nil {
			return err
		}
		res, err := e.dispatch(tx, actor, cmd)
		if err != nil {
			return err
		}
		res.Events = append(events, res.Events...)
		if !res.OK {
			res.Body = map[string]any{"code": res.Code, "message": res.Message, "detail": res.Body}
		}
		raw, err := json.Marshal(res.Body)
		if err != nil {
			return err
		}
		res.Wire = raw
		if persistable(res) {
			if err := saveRequest(tx, actor.ID, cmd.RequestID, hash, res, e.now()); err != nil {
				return err
			}
		}
		if e.hold != nil && res.OK {
			e.hold(cmd.Type)
		}
		out = res
		return nil
	})
	if err != nil {
		var c *coded
		if errors.As(err, &c) {
			return Result{Code: c.Code, Message: c.Message}, nil
		}
		return Result{}, err
	}
	return out, nil
}

// Sweep expires leases and returns the events it committed.
func (e *Engine) Sweep() ([]Event, error) {
	var events []Event
	err := e.store.withTx(func(tx *sql.Tx) error {
		if err := noteClock(tx, e.now()); err != nil {
			return err
		}
		var err error
		events, err = e.sweep(tx)
		return err
	})
	return events, err
}

func persistable(res Result) bool {
	if res.OK {
		return true
	}
	switch res.Code {
	case "not_ready", "lease_expired", "internal", "":
		return false
	default:
		return true
	}
}

func commandHash(cmd Envelope) string {
	h := sha256.New()
	h.Write([]byte(cmd.Type))
	h.Write([]byte{0})
	h.Write([]byte(cmd.RoomID))
	h.Write([]byte{0})
	h.Write([]byte(cmd.TaskID))
	h.Write([]byte{0})
	h.Write(cmd.Payload)
	return hex.EncodeToString(h.Sum(nil))
}

type savedRequest struct {
	hash    string
	ok      bool
	code    string
	message string
	body    json.RawMessage
}

func loadRequest(tx *sql.Tx, principal, requestID string) (savedRequest, bool, error) {
	var row savedRequest
	var okInt int
	var body string
	err := tx.QueryRow(`SELECT payload_hash, ok, code, message, body_json FROM requests WHERE principal=? AND request_id=?`,
		principal, requestID).Scan(&row.hash, &okInt, &row.code, &row.message, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return savedRequest{}, false, nil
	}
	if err != nil {
		return savedRequest{}, false, err
	}
	row.ok = okInt == 1
	row.body = json.RawMessage(body)
	return row, true, nil
}

func saveRequest(tx *sql.Tx, principal, requestID, hash string, res Result, now int64) error {
	okInt := 0
	if res.OK {
		okInt = 1
	}
	body := res.Wire
	if body == nil {
		body = []byte("null")
	}
	_, err := tx.Exec(`INSERT INTO requests(principal, request_id, payload_hash, ok, code, message, body_json, created_at)
		VALUES (?,?,?,?,?,?,?,?)`, principal, requestID, hash, okInt, res.Code, res.Message, string(body), now)
	return err
}

func noteClock(tx *sql.Tx, now int64) error {
	var last int64
	err := tx.QueryRow(`SELECT v FROM meta WHERE k='last_wall'`).Scan(&last)
	if err != nil {
		return err
	}
	if last > 0 && now < last-2000 {
		if _, err := tx.Exec(`UPDATE attempts SET lease_expires_at=? WHERE state IN ('claimed','running')`, now-1); err != nil {
			return err
		}
	}
	_, err = tx.Exec(`UPDATE meta SET v=? WHERE k='last_wall'`, now)
	return err
}

func (e *Engine) dispatch(tx *sql.Tx, actor Actor, cmd Envelope) (Result, error) {
	var res Result
	var err error
	switch cmd.Type {
	case "task.create":
		res, err = e.create(tx, actor, cmd)
	case "task.claim":
		res, err = e.claim(tx, actor, cmd)
	case "attempt.renew":
		res, err = e.renew(tx, actor, cmd)
	case "task.complete":
		res, err = e.complete(tx, actor, cmd)
	case "task.fail":
		res, err = e.fail(tx, actor, cmd)
	case "task.cancel":
		res, err = e.cancel(tx, actor, cmd)
	case "task.get":
		res, err = e.get(tx, actor, cmd)
	case "task.list":
		res, err = e.list(tx, actor, cmd)
	case "task.history":
		res, err = e.history(tx, actor, cmd)
	default:
		return Result{Code: "bad_request", Message: "unsupported command"}, nil
	}
	var c *coded
	if errors.As(err, &c) {
		return Result{Code: c.Code, Message: c.Message}, nil
	}
	return res, err
}

var profileRe = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

type createBody struct {
	ParentTaskID string          `json:"parent_task_id"`
	AssignedTo   string          `json:"assigned_to"`
	Objective    string          `json:"objective"`
	Context      json.RawMessage `json:"context"`
	Profile      string          `json:"profile"`
	TimeoutSec   int             `json:"timeout_sec"`
	// Clients may echo an actor. The hub never reads these; the bearer token is the actor.
	RequestedBy string `json:"requested_by"`
	Actor       string `json:"actor"`
}

func (e *Engine) create(tx *sql.Tx, actor Actor, cmd Envelope) (Result, error) {
	var body createBody
	if err := decode(cmd.Payload, &body); err != nil {
		return Result{}, err
	}
	objective := strings.TrimSpace(body.Objective)
	if objective == "" || len(objective) > e.cfg.Limits.MaxObjectiveBytes {
		return Result{}, refuse("bad_request", "objective is empty or too large")
	}
	if len(body.Context) == 0 {
		body.Context = json.RawMessage(`{}`)
	}
	if !json.Valid(body.Context) || body.Context[0] != '{' || len(body.Context) > e.cfg.Limits.MaxContextBytes {
		return Result{}, refuse("bad_request", "context must be a JSON object within the size limit")
	}
	profile := body.Profile
	if profile == "" {
		profile = "default"
	}
	if !profileRe.MatchString(profile) {
		return Result{}, refuse("bad_request", "profile is malformed")
	}
	if body.TimeoutSec < 0 {
		return Result{}, refuse("bad_request", "timeout_sec is negative")
	}
	assignee, ok := e.cfg.Principals[body.AssignedTo]
	if !ok || assignee.Kind != "agent" {
		return Result{}, refuse("bad_request", "assigned_to is not a known agent")
	}
	now := e.now()
	var parent *taskRow
	var rootID, coordinator string
	var deadline int64
	if actor.Kind == "human" {
		if body.ParentTaskID != "" {
			return Result{}, refuse("forbidden", "humans create root investigations only")
		}
		n, err := countRoots(tx, e.cfg.RoomID)
		if err != nil {
			return Result{}, err
		}
		if n >= 1 {
			return Result{}, refuse("capacity", "one root investigation at a time")
		}
		coordinator = body.AssignedTo
		rootID = ""
		deadline = now + int64(capTimeout(body.TimeoutSec, e.cfg.Limits.RootDeadlineSec))*1000
	} else {
		if body.ParentTaskID == "" {
			return Result{}, refuse("forbidden", "only a human creates a root investigation")
		}
		p, err := loadTask(tx, body.ParentTaskID)
		if err != nil {
			return Result{}, err
		}
		if p == nil || p.RoomID != e.cfg.RoomID {
			return Result{}, refuse("forbidden", "parent task is not in this room")
		}
		if p.ParentID != "" {
			return Result{}, refuse("forbidden", "only direct children of a root are allowed")
		}
		if p.CoordinatorID != actor.ID {
			return Result{}, refuse("forbidden", "only the root coordinator can delegate")
		}
		if p.State != "running" {
			return Result{}, refuse("conflict", "the root investigation is not running")
		}
		kids, err := countChildren(tx, p.ID)
		if err != nil {
			return Result{}, err
		}
		if kids >= e.cfg.Limits.MaxChildTasks {
			return Result{}, refuse("budget", "child task budget is exhausted")
		}
		if body.AssignedTo == actor.ID {
			return Result{}, refuse("forbidden", "the coordinator cannot assign a child to itself")
		}
		remain := p.DeadlineAt - now
		if remain <= 0 {
			return Result{}, refuse("deadline", "the root deadline has no time left")
		}
		sec := capTimeout(body.TimeoutSec, e.cfg.Limits.ChildDeadlineSec)
		if int64(sec)*1000 > remain {
			sec = int(remain / 1000)
			if sec < 1 {
				sec = 1
			}
		}
		deadline = now + int64(sec)*1000
		if deadline > p.DeadlineAt {
			deadline = p.DeadlineAt
		}
		parent = p
		rootID = p.RootID
		coordinator = p.CoordinatorID
	}
	id := newID("tsk_")
	if rootID == "" {
		rootID = id
	}
	parentID := ""
	if parent != nil {
		parentID = parent.ID
	}
	row := taskRow{
		ID: id, RoomID: e.cfg.RoomID, RootID: rootID, ParentID: parentID,
		AssignedTo: body.AssignedTo, RequestedBy: actor.ID, CoordinatorID: coordinator,
		Objective: objective, Context: string(body.Context), Profile: profile,
		State: "queued", DeadlineAt: deadline, CreatedAt: now, UpdatedAt: now,
	}
	if err := insertTask(tx, row); err != nil {
		return Result{}, err
	}
	ev, err := e.emit(tx, actor, "task.created", id, map[string]any{
		"assigned_to": row.AssignedTo, "requested_by": actor.ID, "parent_task_id": parentID,
		"root_id": rootID, "coordinator_id": coordinator, "deadline_at_ms": deadline, "profile": profile,
	})
	if err != nil {
		return Result{}, err
	}
	return Result{OK: true, Body: taskView(row), Events: []Event{ev}}, nil
}

func capTimeout(requested, capSec int) int {
	if requested == 0 || requested > capSec {
		return capSec
	}
	return requested
}

func (e *Engine) claim(tx *sql.Tx, actor Actor, cmd Envelope) (Result, error) {
	if actor.Kind != "agent" {
		return Result{}, refuse("forbidden", "only an agent claims work")
	}
	if cmd.TaskID == "" {
		return Result{}, refuse("bad_request", "task_id is required")
	}
	task, err := loadTask(tx, cmd.TaskID)
	if err != nil {
		return Result{}, err
	}
	if task == nil || task.RoomID != e.cfg.RoomID {
		return Result{}, refuse("forbidden", "task is not in this room")
	}
	if task.AssignedTo != actor.ID {
		return Result{}, refuse("forbidden", "task is assigned to another agent")
	}
	now := e.now()
	if task.State == "running" && task.CurrentAttempt != "" {
		att, err := loadAttempt(tx, task.CurrentAttempt)
		if err != nil {
			return Result{}, err
		}
		if att != nil && (att.State == "claimed" || att.State == "running") && att.LeaseExpires > now {
			return Result{OK: true, Body: grantView(*task, *att, e.cfg.Limits.LeaseSec)}, nil
		}
	}
	if task.State != "queued" {
		return Result{}, refuse("conflict", "task is not queued")
	}
	if now >= task.DeadlineAt {
		return Result{}, refuse("deadline", "the task deadline has passed")
	}
	if task.NotBefore > now {
		return Result{Code: "not_ready", Message: "retry backoff has not elapsed", Body: map[string]any{
			"retry_after_ms": task.NotBefore - now,
		}}, nil
	}
	busy, err := agentBusy(tx, actor.ID, task.ID)
	if err != nil {
		return Result{}, err
	}
	if busy {
		return Result{}, refuse("capacity", "this agent already has a live task")
	}
	if task.AttemptCount >= e.cfg.Limits.MaxAttempts {
		return Result{}, refuse("budget", "attempt budget is exhausted")
	}
	n := task.AttemptCount + 1
	att := attemptRow{
		ID: newID("att_"), TaskID: task.ID, N: n, State: "claimed",
		LeaseExpires: now + int64(e.cfg.Limits.LeaseSec)*1000, StartedAt: now,
	}
	if err := insertAttempt(tx, att); err != nil {
		return Result{}, err
	}
	if err := updateTaskAttempt(tx, task.ID, "running", att.ID, n, now); err != nil {
		return Result{}, err
	}
	task.State = "running"
	task.CurrentAttempt = att.ID
	task.AttemptCount = n
	ev, err := e.emit(tx, actor, "task.started", task.ID, map[string]any{
		"attempt_id": att.ID, "attempt_n": n, "lease_sec": e.cfg.Limits.LeaseSec,
	})
	if err != nil {
		return Result{}, err
	}
	return Result{OK: true, Body: grantView(*task, att, e.cfg.Limits.LeaseSec), Events: []Event{ev}}, nil
}

type attemptRef struct {
	AttemptID string `json:"attempt_id"`
}

func (e *Engine) renew(tx *sql.Tx, actor Actor, cmd Envelope) (Result, error) {
	var body attemptRef
	if err := decode(cmd.Payload, &body); err != nil {
		return Result{}, err
	}
	task, att, err := e.authorizedAttempt(tx, actor, cmd.TaskID, body.AttemptID)
	if err != nil {
		return Result{}, err
	}
	now := e.now()
	if att.LeaseExpires <= now {
		return Result{Code: "lease_expired", Message: "the execution lease has expired"}, nil
	}
	if now >= task.DeadlineAt {
		return Result{}, refuse("deadline", "the task deadline has passed")
	}
	exp := now + int64(e.cfg.Limits.LeaseSec)*1000
	if _, err := tx.Exec(`UPDATE attempts SET state='running', lease_expires_at=? WHERE id=?`, exp, att.ID); err != nil {
		return Result{}, err
	}
	ev, err := e.emit(tx, actor, "attempt.renewed", task.ID, map[string]any{
		"attempt_id": att.ID, "lease_expires_at_ms": exp,
	})
	if err != nil {
		return Result{}, err
	}
	return Result{OK: true, Body: map[string]any{"attempt_id": att.ID, "lease_sec": e.cfg.Limits.LeaseSec, "lease_expires_at_ms": exp}, Events: []Event{ev}}, nil
}

type evidence struct {
	Operation  string `json:"operation"`
	Source     string `json:"source"`
	ObservedAt string `json:"observed_at"`
	Excerpt    string `json:"excerpt"`
}

type completeBody struct {
	AttemptID   string     `json:"attempt_id"`
	Summary     string     `json:"summary"`
	MachineID   string     `json:"machine_id"`
	Limitations []string   `json:"limitations"`
	Evidence    []evidence `json:"evidence"`
}

func (e *Engine) complete(tx *sql.Tx, actor Actor, cmd Envelope) (Result, error) {
	var body completeBody
	if err := decode(cmd.Payload, &body); err != nil {
		return Result{}, err
	}
	if err := e.validateResult(actor, body); err != nil {
		return Result{}, err
	}
	task, att, err := e.fence(tx, actor, cmd.TaskID, body.AttemptID)
	if err != nil {
		var c *coded
		if errors.As(err, &c) && c.Code == "stale" {
			ev, emitErr := e.emit(tx, actor, "attempt.stale_result", cmd.TaskID, map[string]any{
				"attempt_id": body.AttemptID, "reason": c.Message,
			})
			if emitErr != nil {
				return Result{}, emitErr
			}
			return Result{Code: "stale", Message: c.Message, Events: []Event{ev}}, nil
		}
		return Result{}, err
	}
	now := e.now()
	raw, err := json.Marshal(body)
	if err != nil {
		return Result{}, err
	}
	elapsed := now - att.StartedAt
	if elapsed < 0 {
		elapsed = 0
	}
	if _, err := tx.Exec(`UPDATE attempts SET state='succeeded', finished_at=? WHERE id=?`, now, att.ID); err != nil {
		return Result{}, err
	}
	if _, err := tx.Exec(`UPDATE tasks SET state='completed', result_json=?, execution_ms=execution_ms+?, updated_at=?, error_class='', error_message='' WHERE id=?`,
		string(raw), elapsed, now, task.ID); err != nil {
		return Result{}, err
	}
	ev, err := e.emit(tx, actor, "task.completed", task.ID, map[string]any{
		"attempt_id": att.ID, "machine_id": body.MachineID,
	})
	if err != nil {
		return Result{}, err
	}
	task.State = "completed"
	task.Result = string(raw)
	return Result{OK: true, Body: map[string]any{"task_id": task.ID, "attempt_id": att.ID, "state": "completed"}, Events: []Event{ev}}, nil
}

func (e *Engine) validateResult(actor Actor, body completeBody) error {
	if body.MachineID == "" || body.MachineID != actor.MachineID {
		return refuse("bad_request", "machine_id does not match the authenticated agent")
	}
	if strings.TrimSpace(body.Summary) == "" || len(body.Summary) > e.cfg.Limits.MaxSummaryBytes {
		return refuse("bad_request", "summary is empty or too large")
	}
	if len(body.Evidence) > e.cfg.Limits.MaxEvidenceItems {
		return refuse("bad_request", "too many evidence excerpts")
	}
	for _, ev := range body.Evidence {
		if ev.Operation == "" || ev.Source == "" || len(ev.Excerpt) > e.cfg.Limits.MaxExcerptBytes {
			return refuse("bad_request", "evidence excerpt is missing attribution or is too large")
		}
	}
	if len(body.Limitations) > e.cfg.Limits.MaxLimitationItems {
		return refuse("bad_request", "too many limitations")
	}
	for _, line := range body.Limitations {
		if len(line) > e.cfg.Limits.MaxLimitationBytes {
			return refuse("bad_request", "a limitation is too large")
		}
	}
	return nil
}

type failBody struct {
	AttemptID string `json:"attempt_id"`
	Class     string `json:"class"`
	Message   string `json:"message"`
}

func (e *Engine) fail(tx *sql.Tx, actor Actor, cmd Envelope) (Result, error) {
	var body failBody
	if err := decode(cmd.Payload, &body); err != nil {
		return Result{}, err
	}
	if !validFailClass(body.Class) {
		return Result{}, refuse("bad_request", "failure class is not recognized")
	}
	if len(body.Message) > 2000 {
		return Result{}, refuse("bad_request", "failure message is too large")
	}
	task, att, err := e.fence(tx, actor, cmd.TaskID, body.AttemptID)
	if err != nil {
		var c *coded
		if errors.As(err, &c) && c.Code == "stale" {
			ev, emitErr := e.emit(tx, actor, "attempt.stale_result", cmd.TaskID, map[string]any{
				"attempt_id": body.AttemptID, "reason": c.Message,
			})
			if emitErr != nil {
				return Result{}, emitErr
			}
			return Result{Code: "stale", Message: c.Message, Events: []Event{ev}}, nil
		}
		return Result{}, err
	}
	now := e.now()
	elapsed := now - att.StartedAt
	if elapsed < 0 {
		elapsed = 0
	}
	return e.finishInterrupted(tx, actor, task, att, now, elapsed, body.Class, body.Message, retryableClass(body.Class))
}

func (e *Engine) cancel(tx *sql.Tx, actor Actor, cmd Envelope) (Result, error) {
	if cmd.TaskID == "" {
		return Result{}, refuse("bad_request", "task_id is required")
	}
	var body struct {
		Reason string `json:"reason"`
	}
	if err := decode(cmd.Payload, &body); err != nil {
		return Result{}, err
	}
	if len(body.Reason) > 500 {
		return Result{}, refuse("bad_request", "cancel reason is too large")
	}
	task, err := loadTask(tx, cmd.TaskID)
	if err != nil {
		return Result{}, err
	}
	if task == nil || task.RoomID != e.cfg.RoomID {
		return Result{}, refuse("forbidden", "task is not in this room")
	}
	root, err := loadTask(tx, task.RootID)
	if err != nil {
		return Result{}, err
	}
	if root == nil || !canCancel(actor, root) {
		return Result{}, refuse("forbidden", "not allowed to cancel this task")
	}
	if task.State == "cancelled" {
		return Result{OK: true, Body: map[string]any{"task_id": task.ID, "state": "cancelled"}}, nil
	}
	if terminal(task.State) {
		ev, err := e.emit(tx, actor, "task.cancel_rejected", task.ID, map[string]any{"state": task.State})
		if err != nil {
			return Result{}, err
		}
		return Result{Code: "conflict", Message: "task already finished", Events: []Event{ev}}, nil
	}
	now := e.now()
	if task.CurrentAttempt != "" {
		if _, err := tx.Exec(`UPDATE attempts SET state='abandoned', finished_at=? WHERE id=? AND state IN ('claimed','running')`, now, task.CurrentAttempt); err != nil {
			return Result{}, err
		}
	}
	if _, err := tx.Exec(`UPDATE tasks SET state='cancelled', updated_at=?, error_class='cancelled', error_message=? WHERE id=?`,
		now, body.Reason, task.ID); err != nil {
		return Result{}, err
	}
	ev, err := e.emit(tx, actor, "task.cancelled", task.ID, map[string]any{"reason": body.Reason})
	if err != nil {
		return Result{}, err
	}
	return Result{OK: true, Body: map[string]any{"task_id": task.ID, "state": "cancelled"}, Events: []Event{ev}}, nil
}

func canCancel(actor Actor, root *taskRow) bool {
	if actor.Kind == "human" {
		return true
	}
	return actor.ID == root.CoordinatorID
}

func (e *Engine) get(tx *sql.Tx, actor Actor, cmd Envelope) (Result, error) {
	task, err := loadTask(tx, cmd.TaskID)
	if err != nil {
		return Result{}, err
	}
	if task == nil || task.RoomID != e.cfg.RoomID || !canRead(actor, task) {
		return Result{}, refuse("forbidden", "task is not visible")
	}
	attempts, err := listAttempts(tx, task.ID)
	if err != nil {
		return Result{}, err
	}
	return Result{OK: true, Body: map[string]any{"task": taskView(*task), "attempts": attempts}}, nil
}

type listBody struct {
	AssignedTo string `json:"assigned_to"`
	State      string `json:"state"`
}

func (e *Engine) list(tx *sql.Tx, actor Actor, cmd Envelope) (Result, error) {
	var body listBody
	if err := decode(cmd.Payload, &body); err != nil {
		return Result{}, err
	}
	rows, err := listTasks(tx, e.cfg.RoomID, body.AssignedTo, body.State)
	if err != nil {
		return Result{}, err
	}
	views := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		if canRead(actor, &row) {
			views = append(views, taskView(row))
		}
	}
	return Result{OK: true, Body: map[string]any{"tasks": views}}, nil
}

func (e *Engine) history(tx *sql.Tx, actor Actor, cmd Envelope) (Result, error) {
	task, err := loadTask(tx, cmd.TaskID)
	if err != nil {
		return Result{}, err
	}
	if task == nil || task.RoomID != e.cfg.RoomID || !canRead(actor, task) {
		return Result{}, refuse("forbidden", "task is not visible")
	}
	events, err := listEvents(tx, task.RootID)
	if err != nil {
		return Result{}, err
	}
	return Result{OK: true, Body: map[string]any{"root_id": task.RootID, "events": events}}, nil
}

func canRead(actor Actor, task *taskRow) bool {
	if actor.Kind == "human" {
		return true
	}
	return actor.ID == task.AssignedTo || actor.ID == task.CoordinatorID || actor.ID == task.RequestedBy
}

func (e *Engine) authorizedAttempt(tx *sql.Tx, actor Actor, taskID, attemptID string) (*taskRow, *attemptRow, error) {
	if actor.Kind != "agent" {
		return nil, nil, refuse("forbidden", "only the assigned agent can update an attempt")
	}
	task, err := loadTask(tx, taskID)
	if err != nil {
		return nil, nil, err
	}
	if task == nil || task.AssignedTo != actor.ID || task.RoomID != e.cfg.RoomID {
		return nil, nil, refuse("forbidden", "task is not assigned to this agent")
	}
	if task.State != "running" || task.CurrentAttempt != attemptID {
		return nil, nil, refuse("stale", "attempt is not the current authorized attempt")
	}
	att, err := loadAttempt(tx, attemptID)
	if err != nil {
		return nil, nil, err
	}
	if att == nil || (att.State != "claimed" && att.State != "running") {
		return nil, nil, refuse("stale", "attempt is not active")
	}
	return task, att, nil
}

func (e *Engine) fence(tx *sql.Tx, actor Actor, taskID, attemptID string) (*taskRow, *attemptRow, error) {
	task, att, err := e.authorizedAttempt(tx, actor, taskID, attemptID)
	if err != nil {
		return nil, nil, err
	}
	if att.LeaseExpires <= e.now() {
		return nil, nil, refuse("stale", "the execution lease has expired")
	}
	if e.now() >= task.DeadlineAt {
		return nil, nil, refuse("deadline", "the task deadline has passed")
	}
	return task, att, nil
}

func (e *Engine) sweep(tx *sql.Tx) ([]Event, error) {
	now := e.now()
	rows, err := tx.Query(`SELECT a.id, a.task_id, a.started_at FROM attempts a
		JOIN tasks t ON t.id=a.task_id
		WHERE a.state IN ('claimed','running') AND t.state='running' AND t.current_attempt_id=a.id
		  AND (a.lease_expires_at<=? OR t.deadline_at<=?)`, now, now)
	if err != nil {
		return nil, err
	}
	type due struct {
		id, taskID string
		started    int64
	}
	var dues []due
	for rows.Next() {
		var d due
		if err := rows.Scan(&d.id, &d.taskID, &d.started); err != nil {
			_ = rows.Close()
			return nil, err
		}
		dues = append(dues, d)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var events []Event
	actor := Actor{ID: "hub", Kind: "system"}
	for _, d := range dues {
		task, err := loadTask(tx, d.taskID)
		if err != nil {
			return nil, err
		}
		if task == nil || task.CurrentAttempt != d.id || task.State != "running" {
			continue
		}
		elapsed := now - d.started
		if elapsed < 0 {
			elapsed = 0
		}
		res, err := tx.Exec(`UPDATE attempts SET state='lost', finished_at=? WHERE id=? AND state IN ('claimed','running')`, now, d.id)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			continue
		}
		class, message, reason := "lease_lost", "execution lease expired", "lease_expired"
		retryable := true
		if task.DeadlineAt <= now {
			class, message, reason = "deadline", "task deadline passed", "deadline"
			retryable = false
		}
		ev, err := e.emit(tx, actor, "attempt.abandoned", task.ID, map[string]any{
			"attempt_id": d.id, "reason": reason, "attempt_state": "lost",
		})
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
		more, err := e.finishInterrupted(tx, actor, task, &attemptRow{ID: d.id, StartedAt: d.started}, now, elapsed, class, message, retryable)
		if err != nil {
			return nil, err
		}
		events = append(events, more.Events...)
	}
	queued, err := e.timeoutQueued(tx, actor, now)
	if err != nil {
		return nil, err
	}
	events = append(events, queued...)
	if e.cfg.Limits.IdempotencyRetentionSec > 0 {
		cutoff := now - int64(e.cfg.Limits.IdempotencyRetentionSec)*1000
		if _, err := tx.Exec(`DELETE FROM requests WHERE created_at < ?`, cutoff); err != nil {
			return nil, err
		}
	}
	return events, nil
}

func (e *Engine) timeoutQueued(tx *sql.Tx, actor Actor, now int64) ([]Event, error) {
	rows, err := tx.Query(`SELECT id FROM tasks WHERE state='queued' AND deadline_at<=?`, now)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var events []Event
	for _, id := range ids {
		res, err := tx.Exec(`UPDATE tasks SET state='timed_out', error_class='deadline', error_message=?, updated_at=? WHERE id=? AND state='queued'`,
			"task deadline passed", now, id)
		if err != nil {
			return nil, err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			continue
		}
		ev, err := e.emit(tx, actor, "task.timed_out", id, map[string]any{"class": "deadline", "state": "timed_out"})
		if err != nil {
			return nil, err
		}
		events = append(events, ev)
	}
	return events, nil
}

func (e *Engine) finishInterrupted(tx *sql.Tx, actor Actor, task *taskRow, att *attemptRow, now, elapsed int64, class, message string, retryable bool) (Result, error) {
	task.ExecutionMS += elapsed
	requeue := retryable && now < task.DeadlineAt && task.AttemptCount < e.cfg.Limits.MaxAttempts && task.ExecutionMS < int64(e.cfg.Limits.MaxExecutionSec)*1000
	var state, eventType string
	var notBefore int64
	if requeue {
		state = "queued"
		notBefore = now + int64(e.cfg.Limits.RetryBackoffSec)*1000
		eventType = "task.requeued"
	} else if now >= task.DeadlineAt {
		state = "timed_out"
		class = "deadline"
		eventType = "task.timed_out"
	} else {
		state = "failed"
		eventType = "task.failed"
	}
	if att != nil && class != "lease_lost" {
		if _, err := tx.Exec(`UPDATE attempts SET state='failed', finished_at=? WHERE id=? AND state IN ('claimed','running')`, now, att.ID); err != nil {
			return Result{}, err
		}
	}
	changed, err := tx.Exec(`UPDATE tasks SET state=?, current_attempt_id='', not_before=?, execution_ms=?, error_class=?, error_message=?, updated_at=? WHERE id=? AND state='running' AND current_attempt_id=?`,
		state, notBefore, task.ExecutionMS, class, message, now, task.ID, att.ID)
	if err != nil {
		return Result{}, err
	}
	n, _ := changed.RowsAffected()
	if n == 0 {
		return Result{}, fmt.Errorf("task %s changed before the attempt could finish", task.ID)
	}
	ev, err := e.emit(tx, actor, eventType, task.ID, map[string]any{
		"attempt_id": att.ID, "class": class, "state": state, "not_before_ms": notBefore,
	})
	if err != nil {
		return Result{}, err
	}
	return Result{OK: true, Body: map[string]any{"task_id": task.ID, "attempt_id": att.ID, "state": state}, Events: []Event{ev}}, nil
}

func retryableClass(class string) bool {
	switch class {
	case "transient", "crash", "lease_lost":
		return true
	default:
		return false
	}
}

func validFailClass(class string) bool {
	switch class {
	case "transient", "crash", "lease_lost", "invalid_input", "unauthorized", "cancelled", "deadline":
		return true
	default:
		return false
	}
}

func terminal(state string) bool {
	switch state {
	case "completed", "failed", "cancelled", "timed_out":
		return true
	default:
		return false
	}
}

func (e *Engine) emit(tx *sql.Tx, actor Actor, typ, taskID string, payload map[string]any) (Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	var seq int64
	if err := tx.QueryRow(`UPDATE meta SET v=v+1 WHERE k='event_seq' RETURNING v`).Scan(&seq); err != nil {
		return Event{}, err
	}
	ev := Event{
		ID: newID("evt_"), Seq: seq, Type: typ, RoomID: e.cfg.RoomID, TaskID: taskID,
		Actor: actor.ID, CreatedAt: e.now(), Payload: raw,
	}
	_, err = tx.Exec(`INSERT INTO events(id, seq, room_id, task_id, type, actor, payload_json, created_at) VALUES (?,?,?,?,?,?,?,?)`,
		ev.ID, ev.Seq, ev.RoomID, ev.TaskID, ev.Type, ev.Actor, string(raw), ev.CreatedAt)
	return ev, err
}

func decode(raw json.RawMessage, dest any) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dest); err != nil {
		return refuse("bad_request", "malformed payload")
	}
	return nil
}

// Bind attaches an agent socket. Takeover closes the previous session without creating an attempt.
func (e *Engine) Bind(parent context.Context, actor Actor, takeover bool) (*Session, error) {
	if actor.Kind != "agent" {
		return nil, refuse("forbidden", "only agents open a bridge session")
	}
	ctx, cancel := context.WithCancel(parent)
	e.mu.Lock()
	defer e.mu.Unlock()
	if old := e.sessions[actor.ID]; old != nil {
		if !takeover {
			cancel()
			return nil, refuse("session_conflict", "this agent already has a live session")
		}
		old.cancel()
	}
	buf := e.cfg.Limits.EventBuffer
	if buf < 1 {
		buf = 1
	}
	s := &Session{ID: newID("ses_"), AgentID: actor.ID, Events: make(chan Event, buf), Acks: make(chan []byte, buf), Ctx: ctx, cancel: cancel}
	e.sessions[actor.ID] = s
	return s, nil
}

// Unbind drops the session only if it is still the current one.
func (e *Engine) Unbind(s *Session) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if cur, ok := e.sessions[s.AgentID]; ok && cur == s {
		delete(e.sessions, s.AgentID)
	}
}

// Publish fans an event out. A full buffer closes that session instead of growing memory.
func (e *Engine) Publish(events []Event) {
	if len(events) == 0 {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, s := range e.sessions {
		overflow := false
		for _, ev := range events {
			select {
			case s.Events <- ev:
			default:
				overflow = true
			}
			if overflow {
				break
			}
		}
		if overflow {
			s.cancel()
		}
	}
}
