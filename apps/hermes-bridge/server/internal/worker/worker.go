// Package worker runs one Hermes worker subprocess and speaks the NDJSON worker protocol with it.
package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"time"
)

// Spec describes how to launch workers. Command is passed to exec directly: no shell is involved
// and user text only ever travels inside JSON records on stdin.
type Spec struct {
	Command        []string
	Dir            string
	MaxRecordBytes int
	StartTimeout   time.Duration
	CancelGrace    time.Duration
	StopGrace      time.Duration
	ForwardStderr  bool
	Logger         *slog.Logger
}

type Ready struct {
	PID           int      `json:"pid"`
	HermesVersion string   `json:"hermesVersion"`
	Tools         []string `json:"tools"`
	Memory        bool     `json:"memory"`
}

type Result struct {
	RequestID    string `json:"requestId"`
	OK           bool   `json:"ok"`
	Text         string `json:"text"`
	Code         string `json:"code"`
	Message      string `json:"message"`
	MessageCount int    `json:"messageCount"`
	HistoryBytes int64  `json:"historyBytes"`
	Model        string `json:"model"`
	Provider     string `json:"provider"`
	APICalls     int    `json:"apiCalls"`
	ElapsedMs    int64  `json:"elapsedMs"`
}

// StartError is a worker that never became ready. Code is a client protocol code.
type StartError struct {
	Code    string
	Message string
}

func (e *StartError) Error() string { return e.Code + ": " + e.Message }

var (
	// ErrExited means the worker process ended while a request was outstanding.
	ErrExited = errors.New("worker exited")
	// ErrProtocol means the worker wrote something that is not a valid protocol record.
	ErrProtocol = errors.New("worker protocol violation")
)

// CancelledError is returned when ctx ended before the worker answered. Recycled reports whether
// the worker had to be killed (its conversation history is gone) or acknowledged the cancel.
type CancelledError struct {
	Cause    error
	Recycled bool
}

func (e *CancelledError) Error() string {
	return fmt.Sprintf("request cancelled (%v), worker recycled=%v", e.Cause, e.Recycled)
}

func (e *CancelledError) Unwrap() error { return e.Cause }

// record is any worker->bridge record; fatal records reuse Result's code and message fields.
type record struct {
	Type string `json:"type"`
	Result
	Ready
}

// Process is one running worker. Ask must not be called concurrently; the bridge serializes
// requests per session, which owns exactly one Process.
type Process struct {
	spec    Spec
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	writeMu sync.Mutex

	results    chan Result
	exited     chan struct{}
	readerDone chan struct{}

	mu       sync.Mutex
	protoErr error
	exitErr  error
	killed   bool

	stderrLines int

	// sinkMu is held for the whole of each delta delivery, so clearing the sink waits out an
	// in-flight one: once Ask returns, no delta for its request can still be delivered.
	sinkMu sync.Mutex
	sinkID string
	sink   func(text string)
}

// Start launches the worker and waits for its ready (or fatal) record.
func Start(ctx context.Context, spec Spec, extraArgs ...string) (*Process, Ready, error) {
	args := append(append([]string{}, spec.Command[1:]...), extraArgs...)
	cmd := exec.Command(spec.Command[0], args...)
	cmd.Dir = spec.Dir
	cmd.SysProcAttr = procAttr()

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, Ready{}, &StartError{Code: "WORKER_START_FAILED", Message: err.Error()}
	}
	// os.Pipe instead of StdoutPipe: cmd.Wait then never closes our read end, so exit detection
	// does not have to wait for EOF and EOF handling does not race Wait.
	outR, outW, err := os.Pipe()
	if err != nil {
		return nil, Ready{}, &StartError{Code: "WORKER_START_FAILED", Message: err.Error()}
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		outR.Close()
		outW.Close()
		return nil, Ready{}, &StartError{Code: "WORKER_START_FAILED", Message: err.Error()}
	}
	cmd.Stdout, cmd.Stderr = outW, errW
	if err := cmd.Start(); err != nil {
		for _, f := range []*os.File{outR, outW, errR, errW} {
			f.Close()
		}
		return nil, Ready{}, &StartError{Code: "WORKER_START_FAILED", Message: "could not launch worker: " + err.Error()}
	}
	outW.Close()
	errW.Close()

	p := &Process{spec: spec, cmd: cmd, stdin: stdin, results: make(chan Result, 1),
		exited: make(chan struct{}), readerDone: make(chan struct{})}
	readyCh := make(chan record, 1)
	go p.readStdout(outR, readyCh)
	go p.drainStderr(errR)
	go p.wait()

	startCtx, cancel := context.WithTimeout(ctx, spec.StartTimeout)
	defer cancel()
	select {
	case rec := <-readyCh:
		if rec.Type == "fatal" {
			p.Kill()
			code := rec.Code
			if code != "HERMES_NOT_CONFIGURED" {
				code = "WORKER_START_FAILED"
			}
			return nil, Ready{}, &StartError{Code: code, Message: rec.Message}
		}
		return p, rec.Ready, nil
	case <-p.exited:
		return nil, Ready{}, &StartError{Code: "WORKER_START_FAILED", Message: p.describeExit("worker exited before it was ready")}
	case <-startCtx.Done():
		p.Kill()
		return nil, Ready{}, &StartError{Code: "WORKER_START_FAILED", Message: "worker did not become ready within " + spec.StartTimeout.String()}
	}
}

func (p *Process) PID() int { return p.cmd.Process.Pid }

// Exited is closed once the worker process has been reaped.
func (p *Process) Exited() <-chan struct{} { return p.exited }

// Ask sends one question and waits for its result. When ctx ends first, the worker is asked to
// cancel; if it does not acknowledge within CancelGrace it is killed.
//
// A non-nil onDelta asks the worker to stream: it is called on the reader goroutine, in order,
// for every delta of this request, and never after Ask returns. A slow onDelta stalls the worker
// (backpressure) rather than buffering without bound.
func (p *Process) Ask(ctx context.Context, requestID, message string, onDelta func(text string)) (Result, error) {
	p.setSink(requestID, onDelta)
	defer p.setSink("", nil)
	if err := p.send(map[string]any{"type": "ask", "requestId": requestID, "message": message, "stream": onDelta != nil}); err != nil {
		p.Kill()
		<-p.exited
		return Result{}, p.exitCause()
	}
	select {
	case r := <-p.results:
		return p.checkCorrelation(requestID, r)
	case <-p.exited:
		return Result{}, p.exitCause()
	case <-ctx.Done():
	}

	cause := context.Cause(ctx)
	p.setSink("", nil)
	_ = p.send(map[string]string{"type": "cancel", "requestId": requestID})
	grace := time.NewTimer(p.spec.CancelGrace)
	defer grace.Stop()
	select {
	case r := <-p.results:
		if r, err := p.checkCorrelation(requestID, r); err != nil || r.OK {
			return r, err
		}
		return Result{}, &CancelledError{Cause: cause, Recycled: false}
	case <-p.exited:
		return Result{}, &CancelledError{Cause: cause, Recycled: true}
	case <-grace.C:
		p.Kill()
		<-p.exited
		return Result{}, &CancelledError{Cause: cause, Recycled: true}
	}
}

func (p *Process) setSink(requestID string, sink func(string)) {
	p.sinkMu.Lock()
	p.sinkID, p.sink = requestID, sink
	p.sinkMu.Unlock()
}

// deliverDelta drops deltas for any request but the current one: a cancelled turn may still emit
// a few before its result, and those are harmless.
func (p *Process) deliverDelta(requestID, text string) {
	p.sinkMu.Lock()
	defer p.sinkMu.Unlock()
	if p.sink != nil && requestID == p.sinkID && text != "" {
		p.sink(text)
	}
}

func (p *Process) checkCorrelation(requestID string, r Result) (Result, error) {
	if r.RequestID != requestID {
		p.setProtoErr(fmt.Errorf("%w: result for unexpected request", ErrProtocol))
		p.Kill()
		<-p.exited
		return Result{}, p.exitCause()
	}
	return r, nil
}

// Stop asks the worker to exit, then escalates to SIGTERM and SIGKILL for its whole process group.
func (p *Process) Stop() {
	select {
	case <-p.exited:
		return
	default:
	}
	_ = p.send(map[string]string{"type": "shutdown"})
	p.writeMu.Lock()
	p.stdin.Close()
	p.writeMu.Unlock()
	if p.waitExit(p.spec.StopGrace) {
		return
	}
	terminateGroup(p.cmd)
	if p.waitExit(2 * time.Second) {
		return
	}
	p.Kill()
	<-p.exited
}

// Kill sends SIGKILL to the worker's process group. Safe to call repeatedly.
func (p *Process) Kill() {
	p.mu.Lock()
	p.killed = true
	p.mu.Unlock()
	killGroup(p.cmd)
}

func (p *Process) waitExit(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-p.exited:
		return true
	case <-t.C:
		return false
	}
}

func (p *Process) send(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_, err = p.stdin.Write(append(data, '\n'))
	return err
}

func (p *Process) wait() {
	err := p.cmd.Wait()
	// Reap stragglers the worker may have spawned into its group; that also closes their copies of
	// stdout, so the reader reaches EOF and can classify a trailing partial record before exit is
	// reported.
	killGroup(p.cmd)
	select {
	case <-p.readerDone:
	case <-time.After(2 * time.Second):
	}
	p.mu.Lock()
	p.exitErr = err
	p.mu.Unlock()
	close(p.exited)
}

func (p *Process) exitCause() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.protoErr != nil {
		return p.protoErr
	}
	return fmt.Errorf("%w: %s", ErrExited, p.describeExitLocked(""))
}

func (p *Process) describeExit(prefix string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.describeExitLocked(prefix)
}

func (p *Process) describeExitLocked(prefix string) string {
	msg := "exit status unknown"
	if p.exitErr != nil {
		msg = p.exitErr.Error()
	} else if p.cmd.ProcessState != nil {
		msg = p.cmd.ProcessState.String()
	}
	if p.protoErr != nil {
		msg = p.protoErr.Error()
	}
	if prefix != "" {
		return prefix + " (" + msg + ")"
	}
	return msg
}

func (p *Process) setProtoErr(err error) {
	p.mu.Lock()
	if p.protoErr == nil {
		p.protoErr = err
	}
	p.mu.Unlock()
}

// readStdout parses protocol records. Any malformed, oversized or incomplete record is a protocol
// violation: the worker is killed rather than resynchronised.
func (p *Process) readStdout(r io.ReadCloser, readyCh chan<- record) {
	defer close(p.readerDone)
	defer r.Close()
	br := bufio.NewReaderSize(r, 64<<10)
	gotReady := false
	for {
		line, err := readRecord(br, p.spec.MaxRecordBytes)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				p.violation(err)
			}
			return
		}
		var rec record
		if err := json.Unmarshal(line, &rec); err != nil {
			p.violation(fmt.Errorf("%w: invalid JSON record", ErrProtocol))
			return
		}
		switch {
		case !gotReady && (rec.Type == "ready" || rec.Type == "fatal"):
			gotReady = true
			readyCh <- rec
		case gotReady && rec.Type == "delta":
			p.deliverDelta(rec.RequestID, rec.Text)
		case gotReady && rec.Type == "result":
			select {
			case p.results <- rec.Result:
			default:
				p.violation(fmt.Errorf("%w: unsolicited result", ErrProtocol))
				return
			}
		default:
			p.violation(fmt.Errorf("%w: unexpected %q record", ErrProtocol, rec.Type))
			return
		}
	}
}

func (p *Process) violation(err error) {
	p.setProtoErr(err)
	if p.spec.Logger != nil {
		p.spec.Logger.Warn("worker_protocol_violation", "workerPid", p.PID(), "error", err.Error())
	}
	p.Kill()
}

// readRecord returns one newline-terminated record without the newline. A trailing partial record
// at EOF is an error, not data.
func readRecord(br *bufio.Reader, max int) ([]byte, error) {
	var buf []byte
	for {
		chunk, err := br.ReadSlice('\n')
		if len(buf)+len(chunk) > max+1 {
			return nil, fmt.Errorf("%w: record exceeds %d bytes", ErrProtocol, max)
		}
		buf = append(buf, chunk...)
		switch {
		case err == nil:
			return buf[:len(buf)-1], nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF) && len(buf) > 0:
			return nil, fmt.Errorf("%w: incomplete record at EOF", ErrProtocol)
		default:
			return nil, err
		}
	}
}

// drainStderr keeps the pipe from filling. Worker stderr can include Hermes diagnostics that quote
// provider errors, so it is only logged when explicitly enabled.
func (p *Process) drainStderr(r io.ReadCloser) {
	defer r.Close()
	br := bufio.NewReaderSize(r, 16<<10)
	for {
		line, err := br.ReadSlice('\n')
		if len(line) > 0 {
			p.mu.Lock()
			p.stderrLines++
			p.mu.Unlock()
			if p.spec.ForwardStderr && p.spec.Logger != nil {
				if len(line) > 500 {
					line = line[:500]
				}
				p.spec.Logger.Info("worker_stderr", "workerPid", p.PID(), "line", string(line))
			}
		}
		if err != nil && !errors.Is(err, bufio.ErrBufferFull) {
			return
		}
	}
}

// StderrLines is the number of diagnostic lines the worker has written (content not retained).
func (p *Process) StderrLines() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stderrLines
}
