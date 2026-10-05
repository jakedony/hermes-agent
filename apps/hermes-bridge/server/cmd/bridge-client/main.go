// Command bridge-client is a small test client for the Hermes WebSocket bridge. It builds for
// Linux and Windows (GOOS=windows) and needs only the URL and the token file.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coder/websocket"
)

type options struct {
	url, tokenFile, origin, session string
	browserAuth, raw, keepGoing     bool
	stream                          bool
	timeout                         time.Duration
}

func main() {
	var o options
	var asks multi
	var interactive, selftest bool
	flag.StringVar(&o.url, "url", "ws://127.0.0.1:8765/ws", "bridge WebSocket URL (through the SSH tunnel)")
	flag.StringVar(&o.tokenFile, "token-file", "", "file holding the bridge token (required)")
	flag.StringVar(&o.origin, "origin", "", "send this Origin header (to exercise the origin policy)")
	flag.StringVar(&o.session, "session", "", "sessionId to use (default: random)")
	flag.BoolVar(&o.browserAuth, "browser-auth", false, "send the token as a WebSocket subprotocol, as a browser must")
	flag.BoolVar(&o.raw, "raw", false, "print every server event as JSON")
	flag.BoolVar(&o.keepGoing, "keep-going", false, "with -ask: continue with the next question after an error")
	flag.BoolVar(&o.stream, "stream", true, "with -ask/-interactive: request answer_delta events and print the answer as it arrives")
	flag.DurationVar(&o.timeout, "timeout", 5*time.Minute, "maximum wait for each request")
	flag.Var(&asks, "ask", "question to send (repeatable; sent in order on one session)")
	flag.BoolVar(&interactive, "interactive", false, "read questions from stdin, one per line")
	flag.BoolVar(&selftest, "selftest", false, "run the end-to-end verification suite")
	flag.Parse()

	if o.tokenFile == "" {
		fatal(errors.New("-token-file is required"))
	}
	if o.session == "" {
		o.session = "session-" + randID()
	}
	var err error
	switch {
	case selftest:
		err = runSelftest(o)
	case interactive:
		err = runInteractive(o)
	case len(asks) > 0:
		err = runAsks(o, asks)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fatal(err)
	}
}

type multi []string

func (m *multi) String() string     { return strings.Join(*m, " | ") }
func (m *multi) Set(v string) error { *m = append(*m, v); return nil }

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "bridge-client:", err)
	os.Exit(1)
}

func randID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Event is a decoded server event; fields not present are zero values.
type Event struct {
	Type         string          `json:"type"`
	RequestID    *string         `json:"requestId"`
	SessionID    *string         `json:"sessionId"`
	Text         string          `json:"text"`
	Code         string          `json:"code"`
	Message      string          `json:"message"`
	Retryable    bool            `json:"retryable"`
	Rejected     bool            `json:"rejected"`
	SessionReset bool            `json:"sessionReset"`
	NewSession   bool            `json:"newSession"`
	Status       string          `json:"status"`
	DurationMs   int64           `json:"durationMs"`
	Existed      bool            `json:"existed"`
	Seq          int             `json:"seq"`
	Raw          json.RawMessage `json:"-"`
}

type client struct {
	o      options
	ws     *websocket.Conn
	events chan Event
	hello  Event
	seq    int
	prefix string
	// onDelta, when set, sees each answer_delta's text as it arrives.
	onDelta func(text string)
}

func dial(o options) (*client, error) {
	token, err := os.ReadFile(o.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read token: %w", err)
	}
	tok := strings.TrimSpace(string(token))
	h := http.Header{}
	opts := &websocket.DialOptions{HTTPHeader: h}
	if o.browserAuth {
		opts.Subprotocols = []string{"hermes-bridge.v1", "hermes-bridge.token." + tok}
	} else {
		h.Set("Authorization", "Bearer "+tok)
	}
	if o.origin != "" {
		h.Set("Origin", o.origin)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ws, resp, err := websocket.Dial(ctx, o.url, opts)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("handshake rejected: HTTP %d", resp.StatusCode)
		}
		return nil, err
	}
	ws.SetReadLimit(8 << 20)
	c := &client{o: o, ws: ws, events: make(chan Event, 64), prefix: "req-" + randID()}
	go c.readLoop()
	select {
	case c.hello = <-c.events:
	case <-time.After(10 * time.Second):
		return nil, errors.New("no hello event from server")
	}
	if c.hello.Type != "hello" {
		return nil, fmt.Errorf("expected hello, got %q", c.hello.Type)
	}
	return c, nil
}

func (c *client) readLoop() {
	defer close(c.events)
	for {
		_, data, err := c.ws.Read(context.Background())
		if err != nil {
			return
		}
		var ev Event
		if json.Unmarshal(data, &ev) != nil {
			continue
		}
		ev.Raw = data
		if c.o.raw {
			fmt.Fprintln(os.Stderr, "<-", string(data))
		}
		c.events <- ev
	}
}

func (c *client) close() { _ = c.ws.Close(websocket.StatusNormalClosure, "bye") }

func (c *client) nextID() string {
	c.seq++
	return fmt.Sprintf("%s-%03d", c.prefix, c.seq)
}

func (c *client) sendRaw(data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return c.ws.Write(ctx, websocket.MessageText, data)
}

func (c *client) sendJSON(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.sendRaw(data)
}

func askMsg(requestID, sessionID, text string) map[string]any {
	return map[string]any{"protocolVersion": 1, "type": "ask", "requestId": requestID, "sessionId": sessionID, "message": text}
}

// Outcome collects every event of one request: it ends on done, or on a rejected error.
type Outcome struct {
	Accepted *Event
	Deltas   []string
	Answer   *Event
	Error    *Event
	Done     *Event
	// FirstDeltaAt and AnswerAt are arrival times, to show the stream arrived before the answer.
	FirstDeltaAt, AnswerAt time.Time
}

// Streamed is the concatenated answer_delta text: a provisional draft of Answer.Text.
func (o Outcome) Streamed() string { return strings.Join(o.Deltas, "") }

func (o Outcome) String() string {
	switch {
	case o.Answer != nil:
		return "answer"
	case o.Error != nil:
		return o.Error.Code
	}
	return "no outcome"
}

// await collects events for requestID. Events for other requests are returned in others.
func (c *client) await(requestID string) (Outcome, []Event, error) {
	var out Outcome
	var others []Event
	deadline := time.After(c.o.timeout)
	for {
		select {
		case ev, ok := <-c.events:
			if !ok {
				return out, others, errors.New("connection closed")
			}
			if ev.RequestID == nil || *ev.RequestID != requestID {
				others = append(others, ev)
				continue
			}
			e := ev
			switch ev.Type {
			case "accepted":
				out.Accepted = &e
			case "answer_delta":
				if out.Accepted == nil || out.Answer != nil || out.Error != nil || ev.Seq != len(out.Deltas) {
					return out, others, fmt.Errorf("answer_delta out of order (seq %d after %d deltas)", ev.Seq, len(out.Deltas))
				}
				if len(out.Deltas) == 0 {
					out.FirstDeltaAt = time.Now()
				}
				out.Deltas = append(out.Deltas, ev.Text)
				if c.onDelta != nil {
					c.onDelta(ev.Text)
				}
			case "answer":
				out.Answer = &e
				out.AnswerAt = time.Now()
			case "error":
				out.Error = &e
				if ev.Rejected {
					return out, others, nil
				}
			case "done":
				out.Done = &e
				return out, others, nil
			case "session_ended":
				return out, others, nil
			}
		case <-deadline:
			return out, others, fmt.Errorf("timed out waiting for %s", requestID)
		}
	}
}

func (c *client) ask(sessionID, text string) (Outcome, error) {
	return c.askWith(sessionID, text, false)
}

func (c *client) askWith(sessionID, text string, stream bool) (Outcome, error) {
	id := c.nextID()
	msg := askMsg(id, sessionID, text)
	if stream {
		msg["stream"] = true
	}
	if err := c.sendJSON(msg); err != nil {
		return Outcome{}, err
	}
	out, _, err := c.await(id)
	return out, err
}

// printOutcome finishes a request's output. When deltas were already printed live, the answer is
// reprinted only if it differs from them, because the answer event is authoritative.
func printOutcome(o Outcome, streamedLive bool) {
	shown := streamedLive && len(o.Deltas) > 0
	switch {
	case o.Answer != nil && shown && o.Streamed() == o.Answer.Text:
		fmt.Println()
	case o.Answer != nil && shown:
		fmt.Printf("\n[the final answer differs from the streamed draft; authoritative text:]\n%s\n", o.Answer.Text)
	case o.Answer != nil:
		fmt.Println(o.Answer.Text)
	case o.Error != nil:
		if shown {
			fmt.Println()
		}
		fmt.Printf("[error %s retryable=%v sessionReset=%v] %s\n", o.Error.Code, o.Error.Retryable, o.Error.SessionReset, o.Error.Message)
	}
}

func (c *client) streamToStdout() {
	if c.o.stream {
		c.onDelta = func(text string) { fmt.Print(text) }
	}
}

func runAsks(o options, asks []string) error {
	c, err := dial(o)
	if err != nil {
		return err
	}
	defer c.close()
	c.streamToStdout()
	var failed error
	for _, q := range asks {
		fmt.Printf(">>> %s\n", q)
		out, err := c.askWith(o.session, q, o.stream)
		if err != nil {
			return err
		}
		printOutcome(out, o.stream)
		if out.Error != nil {
			failed = fmt.Errorf("request failed: %s", out.Error.Code)
			if !o.keepGoing {
				return failed
			}
		}
	}
	return failed
}

func runInteractive(o options) error {
	c, err := dial(o)
	if err != nil {
		return err
	}
	defer c.close()
	c.streamToStdout()
	fmt.Printf("connected (%s), session %s. Empty line or Ctrl-D quits.\n", o.url, o.session)
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 64<<10), 64<<10)
	for fmt.Print("> "); in.Scan(); fmt.Print("> ") {
		q := strings.TrimSpace(in.Text())
		if q == "" {
			break
		}
		out, err := c.askWith(o.session, q, o.stream)
		if err != nil {
			return err
		}
		printOutcome(out, o.stream)
	}
	if err := in.Err(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
