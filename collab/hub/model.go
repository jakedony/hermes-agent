package main

import (
	"database/sql"
	"encoding/json"
)

type taskRow struct {
	ID             string
	RoomID         string
	RootID         string
	ParentID       string
	AssignedTo     string
	RequestedBy    string
	CoordinatorID  string
	Objective      string
	Context        string
	Profile        string
	State          string
	DeadlineAt     int64
	NotBefore      int64
	CurrentAttempt string
	AttemptCount   int
	ExecutionMS    int64
	Result         string
	ErrorClass     string
	ErrorMessage   string
	CreatedAt      int64
	UpdatedAt      int64
}

type attemptRow struct {
	ID           string
	TaskID       string
	N            int
	State        string
	LeaseExpires int64
	StartedAt    int64
	FinishedAt   int64
}

func insertTask(tx *sql.Tx, t taskRow) error {
	_, err := tx.Exec(`INSERT INTO tasks(id, room_id, root_id, parent_id, assigned_to, requested_by, coordinator_id,
		objective, context_json, profile, state, deadline_at, not_before, current_attempt_id, attempt_count,
		execution_ms, result_json, error_class, error_message, created_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		t.ID, t.RoomID, t.RootID, t.ParentID, t.AssignedTo, t.RequestedBy, t.CoordinatorID,
		t.Objective, t.Context, t.Profile, t.State, t.DeadlineAt, t.NotBefore, t.CurrentAttempt, t.AttemptCount,
		t.ExecutionMS, t.Result, t.ErrorClass, t.ErrorMessage, t.CreatedAt, t.UpdatedAt)
	return err
}

func loadTask(tx *sql.Tx, id string) (*taskRow, error) {
	var t taskRow
	err := tx.QueryRow(`SELECT id, room_id, root_id, parent_id, assigned_to, requested_by, coordinator_id,
		objective, context_json, profile, state, deadline_at, not_before, current_attempt_id, attempt_count,
		execution_ms, result_json, error_class, error_message, created_at, updated_at FROM tasks WHERE id=?`, id).Scan(
		&t.ID, &t.RoomID, &t.RootID, &t.ParentID, &t.AssignedTo, &t.RequestedBy, &t.CoordinatorID,
		&t.Objective, &t.Context, &t.Profile, &t.State, &t.DeadlineAt, &t.NotBefore, &t.CurrentAttempt, &t.AttemptCount,
		&t.ExecutionMS, &t.Result, &t.ErrorClass, &t.ErrorMessage, &t.CreatedAt, &t.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func updateTaskAttempt(tx *sql.Tx, id, state, attemptID string, n int, now int64) error {
	_, err := tx.Exec(`UPDATE tasks SET state=?, current_attempt_id=?, attempt_count=?, updated_at=? WHERE id=?`,
		state, attemptID, n, now, id)
	return err
}

func insertAttempt(tx *sql.Tx, a attemptRow) error {
	_, err := tx.Exec(`INSERT INTO attempts(id, task_id, n, state, lease_expires_at, started_at, finished_at) VALUES (?,?,?,?,?,?,?)`,
		a.ID, a.TaskID, a.N, a.State, a.LeaseExpires, a.StartedAt, a.FinishedAt)
	return err
}

func loadAttempt(tx *sql.Tx, id string) (*attemptRow, error) {
	var a attemptRow
	err := tx.QueryRow(`SELECT id, task_id, n, state, lease_expires_at, started_at, finished_at FROM attempts WHERE id=?`, id).Scan(
		&a.ID, &a.TaskID, &a.N, &a.State, &a.LeaseExpires, &a.StartedAt, &a.FinishedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func countRoots(tx *sql.Tx, room string) (int, error) {
	var n int
	err := tx.QueryRow(`SELECT COUNT(*) FROM tasks WHERE room_id=? AND parent_id='' AND state IN ('queued','running')`, room).Scan(&n)
	return n, err
}

func countChildren(tx *sql.Tx, parent string) (int, error) {
	var n int
	err := tx.QueryRow(`SELECT COUNT(*) FROM tasks WHERE parent_id=?`, parent).Scan(&n)
	return n, err
}

func agentBusy(tx *sql.Tx, agentID, exceptTask string) (bool, error) {
	var n int
	err := tx.QueryRow(`SELECT COUNT(*) FROM tasks WHERE assigned_to=? AND id!=? AND state='running'`, agentID, exceptTask).Scan(&n)
	return n > 0, err
}

func listTasks(tx *sql.Tx, room, assignee, state string) ([]taskRow, error) {
	q := `SELECT id, room_id, root_id, parent_id, assigned_to, requested_by, coordinator_id,
		objective, context_json, profile, state, deadline_at, not_before, current_attempt_id, attempt_count,
		execution_ms, result_json, error_class, error_message, created_at, updated_at FROM tasks WHERE room_id=?`
	args := []any{room}
	if assignee != "" {
		q += ` AND assigned_to=?`
		args = append(args, assignee)
	}
	if state != "" {
		q += ` AND state=?`
		args = append(args, state)
	}
	q += ` ORDER BY created_at`
	rows, err := tx.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []taskRow
	for rows.Next() {
		var t taskRow
		if err := rows.Scan(&t.ID, &t.RoomID, &t.RootID, &t.ParentID, &t.AssignedTo, &t.RequestedBy, &t.CoordinatorID,
			&t.Objective, &t.Context, &t.Profile, &t.State, &t.DeadlineAt, &t.NotBefore, &t.CurrentAttempt, &t.AttemptCount,
			&t.ExecutionMS, &t.Result, &t.ErrorClass, &t.ErrorMessage, &t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func listAttempts(tx *sql.Tx, taskID string) ([]map[string]any, error) {
	rows, err := tx.Query(`SELECT id, n, state, lease_expires_at, started_at, finished_at FROM attempts WHERE task_id=? ORDER BY n`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var a attemptRow
		if err := rows.Scan(&a.ID, &a.N, &a.State, &a.LeaseExpires, &a.StartedAt, &a.FinishedAt); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"attempt_id": a.ID, "n": a.N, "state": a.State,
			"lease_expires_at_ms": a.LeaseExpires, "started_at_ms": a.StartedAt, "finished_at_ms": a.FinishedAt,
		})
	}
	if out == nil {
		out = []map[string]any{}
	}
	return out, rows.Err()
}

func listEvents(tx *sql.Tx, rootID string) ([]Event, error) {
	rows, err := tx.Query(`SELECT e.id, e.seq, e.type, e.room_id, e.task_id, e.actor, e.payload_json, e.created_at
		FROM events e JOIN tasks t ON t.id=e.task_id WHERE t.root_id=? ORDER BY e.seq`, rootID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var ev Event
		var payload string
		if err := rows.Scan(&ev.ID, &ev.Seq, &ev.Type, &ev.RoomID, &ev.TaskID, &ev.Actor, &payload, &ev.CreatedAt); err != nil {
			return nil, err
		}
		ev.Payload = json.RawMessage(payload)
		out = append(out, ev)
	}
	if out == nil {
		out = []Event{}
	}
	return out, rows.Err()
}

func taskView(t taskRow) map[string]any {
	return map[string]any{
		"task_id": t.ID, "room_id": t.RoomID, "root_id": t.RootID, "parent_task_id": t.ParentID,
		"assigned_to": t.AssignedTo, "requested_by": t.RequestedBy, "coordinator_id": t.CoordinatorID,
		"objective": t.Objective, "context": rawOrNull(t.Context), "profile": t.Profile, "state": t.State,
		"deadline_at_ms": t.DeadlineAt, "not_before_ms": t.NotBefore, "current_attempt_id": t.CurrentAttempt,
		"attempt_count": t.AttemptCount, "execution_ms": t.ExecutionMS, "result": rawOrNull(t.Result),
		"error_class": t.ErrorClass, "error_message": t.ErrorMessage,
		"created_at_ms": t.CreatedAt, "updated_at_ms": t.UpdatedAt,
	}
}

func grantView(t taskRow, a attemptRow, leaseSec int) map[string]any {
	role := "worker"
	if t.ParentID == "" {
		role = "coordinator"
	}
	return map[string]any{
		"task_id": t.ID, "attempt_id": a.ID, "attempt_n": a.N, "lease_sec": leaseSec,
		"deadline_at_ms": t.DeadlineAt, "objective": t.Objective, "context": rawOrNull(t.Context),
		"profile": t.Profile, "parent_task_id": t.ParentID, "root_id": t.RootID, "role": role,
	}
}

func rawOrNull(s string) json.RawMessage {
	if s == "" {
		return json.RawMessage("null")
	}
	return json.RawMessage(s)
}
