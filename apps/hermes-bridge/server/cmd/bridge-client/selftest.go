package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type check struct {
	name   string
	ok     bool
	detail string
}

type suite struct{ checks []check }

func (s *suite) record(name string, ok bool, format string, args ...any) {
	s.checks = append(s.checks, check{name, ok, fmt.Sprintf(format, args...)})
	mark := "PASS"
	if !ok {
		mark = "FAIL"
	}
	fmt.Printf("[%s] %s: %s\n", mark, name, s.checks[len(s.checks)-1].detail)
}

func healthURL(wsURL string) string {
	u, err := url.Parse(wsURL)
	if err != nil {
		return ""
	}
	u.Scheme = strings.Replace(u.Scheme, "ws", "http", 1)
	u.Path = "/healthz"
	return u.String()
}

func snippet(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 160 {
		return s[:160] + "..."
	}
	return s
}

// runSelftest drives the round trip, history, isolation and protocol-rejection checks against a
// running bridge. Whether answers are live or stubbed depends only on the server's Hermes config.
func runSelftest(o options) error {
	var s suite

	resp, err := http.Get(healthURL(o.url))
	if err != nil {
		s.record("healthz", false, "%v", err)
	} else {
		var body struct {
			Status string `json:"status"`
			Hermes struct {
				Status, HermesVersion, Message string
				Tools                          []string
			} `json:"hermes"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		s.record("healthz", resp.StatusCode == 200 && body.Status == "ok",
			"HTTP %d status=%s hermes=%s version=%q tools=%v", resp.StatusCode, body.Status, body.Hermes.Status, body.Hermes.HermesVersion, body.Hermes.Tools)
	}

	c, err := dial(o)
	if err != nil {
		s.record("connect", false, "%v", err)
		return summarize(s)
	}
	defer c.close()
	s.record("connect", true, "connectionId=%s", strPtr(c.hello.Raw, "connectionId"))

	sessA, sessB := "selftest-a-"+randID(), "selftest-b-"+randID()

	out, err := c.ask(sessA, "Explain the difference between authentication and authorisation.")
	s.record("real question", err == nil && out.Answer != nil && strings.TrimSpace(out.Answer.Text) != "" && out.Done != nil && out.Done.Status == "ok",
		"outcome=%s err=%v answer=%q", out, err, answerText(out))

	out, err = c.ask(sessA, "Remember this test word: mango.")
	s.record("follow-up 1 (remember)", err == nil && out.Answer != nil, "outcome=%s answer=%q", out, answerText(out))

	out, err = c.ask(sessA, "What test word did I give you?")
	s.record("follow-up 2 (recall within session)", err == nil && strings.Contains(strings.ToLower(answerText(out)), "mango"),
		"outcome=%s answer=%q", out, answerText(out))

	out, err = c.ask(sessB, "What test word did I give you?")
	s.record("isolation: other session, same connection", err == nil && out.Answer != nil && out.Accepted != nil && out.Accepted.NewSession &&
		!strings.Contains(strings.ToLower(answerText(out)), "mango"), "outcome=%s newSession=%v answer=%q", out, out.Accepted != nil && out.Accepted.NewSession, answerText(out))

	if c2, err := dial(o); err != nil {
		s.record("isolation: same sessionId, other connection", false, "%v", err)
	} else {
		out, err = c2.ask(sessA, "What test word did I give you?")
		s.record("isolation: same sessionId, other connection", err == nil && out.Answer != nil && out.Accepted != nil && out.Accepted.NewSession &&
			!strings.Contains(strings.ToLower(answerText(out)), "mango"), "outcome=%s newSession=%v answer=%q", out, out.Accepted != nil && out.Accepted.NewSession, answerText(out))
		c2.close()
	}

	rejections := []struct {
		name, code string
		payload    func(id string) []byte
	}{
		{"empty question", "EMPTY_MESSAGE", func(id string) []byte { return mustJSON(askMsg(id, sessA, "   ")) }},
		{"unsupported protocol version", "UNSUPPORTED_PROTOCOL_VERSION", func(id string) []byte {
			m := askMsg(id, sessA, "hi")
			m["protocolVersion"] = 2
			return mustJSON(m)
		}},
		{"unsupported type", "UNSUPPORTED_MESSAGE_TYPE", func(id string) []byte {
			m := askMsg(id, sessA, "hi")
			m["type"] = "stream"
			return mustJSON(m)
		}},
		{"missing field", "MISSING_FIELD", func(id string) []byte {
			m := askMsg(id, sessA, "hi")
			delete(m, "message")
			return mustJSON(m)
		}},
		{"non-empty context", "UNSUPPORTED_CONTEXT", func(id string) []byte {
			m := askMsg(id, sessA, "Summarise this passage.")
			m["context"] = map[string]any{"documentId": "document-001", "page": 4, "selectedText": "The passage being discussed"}
			return mustJSON(m)
		}},
		{"oversized message", "MESSAGE_TOO_LARGE", func(id string) []byte {
			return mustJSON(askMsg(id, sessA, strings.Repeat("x", 200<<10)))
		}},
	}
	for _, r := range rejections {
		id := c.nextID()
		if err := c.sendRaw(r.payload(id)); err != nil {
			s.record("reject: "+r.name, false, "send: %v", err)
			continue
		}
		ev, err := c.nextRejection(id, r.code == "MESSAGE_TOO_LARGE")
		s.record("reject: "+r.name, err == nil && ev.Code == r.code && ev.Rejected, "got code=%s rejected=%v err=%v", ev.Code, ev.Rejected, err)
	}
	if err := c.sendRaw([]byte("{not json")); err == nil {
		ev, err := c.nextRejection("", true)
		s.record("reject: malformed JSON", err == nil && ev.Code == "MALFORMED_MESSAGE" && ev.RequestID == nil, "got code=%s err=%v", ev.Code, err)
	}

	// Busy policy: a second question on a session that is still working is rejected, not queued.
	first, second := c.nextID(), c.nextID()
	_ = c.sendJSON(askMsg(first, sessA, "In one sentence, what is a hash function?"))
	_ = c.sendJSON(askMsg(second, sessA, "And a second question sent too early."))
	out2, others, err2 := c.await(second)
	busyOK := err2 == nil && out2.Error != nil && out2.Error.Code == "SESSION_BUSY" && out2.Error.Rejected
	out1, _, err1 := c.awaitWithBacklog(first, others)
	s.record("busy session", busyOK && err1 == nil && out1.Answer != nil, "second=%s first=%s", out2, out1)

	dup := c.nextID()
	_ = c.sendJSON(askMsg(dup, sessB, "Say OK."))
	_, _, _ = c.await(dup)
	_ = c.sendJSON(askMsg(dup, sessB, "Say OK again."))
	out, _, err = c.await(dup)
	s.record("duplicate requestId", err == nil && out.Error != nil && out.Error.Code == "DUPLICATE_REQUEST_ID", "outcome=%s", out)

	end := c.nextID()
	_ = c.sendJSON(map[string]any{"protocolVersion": 1, "type": "end_session", "requestId": end, "sessionId": sessA})
	ev, err := c.nextOfType(end, "session_ended")
	s.record("end_session", err == nil && ev.Existed, "existed=%v err=%v", ev.Existed, err)

	return summarize(s)
}

func (c *client) awaitWithBacklog(requestID string, backlog []Event) (Outcome, []Event, error) {
	var out Outcome
	for _, ev := range backlog {
		e := ev
		if ev.RequestID == nil || *ev.RequestID != requestID {
			continue
		}
		switch ev.Type {
		case "accepted":
			out.Accepted = &e
		case "answer":
			out.Answer = &e
		case "error":
			out.Error = &e
		case "done":
			out.Done = &e
			return out, nil, nil
		}
	}
	more, others, err := c.await(requestID)
	if out.Accepted != nil && more.Accepted == nil {
		more.Accepted = out.Accepted
	}
	if more.Answer == nil {
		more.Answer = out.Answer
	}
	return more, others, err
}

// nextRejection waits for a rejected error. When the identifiers cannot be echoed (oversized or
// malformed frames) it matches the next rejected error with a null requestId.
func (c *client) nextRejection(requestID string, anonymous bool) (Event, error) {
	deadline := time.After(30 * time.Second)
	for {
		select {
		case ev, ok := <-c.events:
			if !ok {
				return Event{}, fmt.Errorf("connection closed")
			}
			if ev.Type != "error" || !ev.Rejected {
				continue
			}
			if (anonymous && ev.RequestID == nil) || (ev.RequestID != nil && *ev.RequestID == requestID) {
				return ev, nil
			}
		case <-deadline:
			return Event{}, fmt.Errorf("no rejection received")
		}
	}
}

func (c *client) nextOfType(requestID, typ string) (Event, error) {
	deadline := time.After(30 * time.Second)
	for {
		select {
		case ev, ok := <-c.events:
			if !ok {
				return Event{}, fmt.Errorf("connection closed")
			}
			if ev.Type == typ && ev.RequestID != nil && *ev.RequestID == requestID {
				return ev, nil
			}
		case <-deadline:
			return Event{}, fmt.Errorf("no %s event", typ)
		}
	}
}

func answerText(o Outcome) string {
	if o.Answer != nil {
		return snippet(o.Answer.Text)
	}
	if o.Error != nil {
		return o.Error.Code + ": " + snippet(o.Error.Message)
	}
	return ""
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func strPtr(raw json.RawMessage, key string) string {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	v, _ := m[key].(string)
	return v
}

func summarize(s suite) error {
	failed := 0
	for _, c := range s.checks {
		if !c.ok {
			failed++
		}
	}
	fmt.Printf("\n%d checks, %d passed, %d failed\n", len(s.checks), len(s.checks)-failed, failed)
	if failed > 0 {
		return fmt.Errorf("%d selftest checks failed", failed)
	}
	return nil
}
