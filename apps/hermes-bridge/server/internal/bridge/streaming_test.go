package bridge

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"hermes-bridge/internal/config"
)

// Streaming contracts, against the fake worker (a mock). collect() already enforces that every
// answer_delta lies between accepted and the outcome and that seq has no gaps.

func streamAsk(req, sess, msg string) map[string]any {
	m := ask(req, sess, msg)
	m["stream"] = true
	return m
}

func joinDeltas(deltas []map[string]any) string {
	var b strings.Builder
	for _, d := range deltas {
		b.WriteString(d["text"].(string))
	}
	return b.String()
}

func TestStreamedDeltasPrecedeTheAuthoritativeAnswer(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	c.send(streamAsk("r1", "s1", "STREAM 5"))
	_, ans, done := c.collect("r1")
	if ans["type"] != "answer" || done["status"] != "ok" {
		t.Fatalf("bad outcome: %v %v", ans, done)
	}
	if len(c.deltas) != 5 || joinDeltas(c.deltas) != ans["text"] {
		t.Fatalf("deltas %q (%d) do not build the answer %q; a delta for another request must be dropped",
			joinDeltas(c.deltas), len(c.deltas), ans["text"])
	}
	for _, d := range c.deltas {
		if d["requestId"] != "r1" || d["sessionId"] != "s1" || d["protocolVersion"] != float64(1) {
			t.Fatalf("delta not correlated: %v", d)
		}
	}

	// The answer, not the stream, is authoritative when they differ.
	c.send(streamAsk("r2", "s1", "STREAM-REVISED"))
	_, ans, _ = c.collect("r2")
	if len(c.deltas) != 2 || ans["text"] != "the authoritative final answer" {
		t.Fatalf("revised stream: deltas=%v answer=%v", c.deltas, ans)
	}
}

func TestNonStreamingAskGetsNoDeltas(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	if _, ans, _ := c.roundTrip("r1", "s1", "STREAM 3"); len(c.deltas) != 0 || ans["text"] != "w0 w1 w2 " {
		t.Fatalf("v1 client without stream:true got deltas %v (answer %v)", c.deltas, ans)
	}
}

func TestNoDeltaAfterATimedOutRequest(t *testing.T) {
	h := newHarness(t, "ok", func(c *config.Config) {
		c.RequestTimeout = config.Duration{Duration: 500 * time.Millisecond}
	})
	c := h.dial(nil)
	c.send(streamAsk("r1", "s1", "STREAM-THEN-HANG"))
	_, outcome, done := c.collect("r1")
	if outcome["code"] != "HERMES_TIMEOUT" || done["status"] != "error" {
		t.Fatalf("want HERMES_TIMEOUT, got %v", outcome)
	}
	if joinDeltas(c.deltas) != "partial " {
		t.Fatalf("the worker's late delta leaked into the stream: %q", joinDeltas(c.deltas))
	}
	// The session survives the cooperative cancel and its next request streams cleanly.
	c.send(streamAsk("r2", "s1", "STREAM 2"))
	if _, ans, _ := c.collect("r2"); ans["text"] != "w0 w1 " || joinDeltas(c.deltas) != "w0 w1 " {
		t.Fatalf("next request after timeout: %v deltas=%q", ans, joinDeltas(c.deltas))
	}
}

func TestStreamedBytesAreCappedPerRequest(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	c.send(streamAsk("r1", "s1", "STREAM-FLOOD"))
	_, ans, _ := c.collect("r1")
	streamed := len(joinDeltas(c.deltas))
	if ans["text"] != "flood done" || streamed == 0 || streamed > 1<<20 {
		t.Fatalf("streamed %d bytes (cap %d), answer %v", streamed, 1<<20, ans)
	}
}

func TestDeltaEventsCarryNoContentIntoLogs(t *testing.T) {
	h := newHarness(t, "ok", nil)
	c := h.dial(nil)
	c.send(streamAsk("r1", "s1", "STREAM 4"))
	c.collect("r1")
	for i := 0; i < 4; i++ {
		if piece := fmt.Sprintf("w%d ", i); strings.Contains(h.logs.String(), piece) {
			t.Fatalf("streamed text %q reached the server log", piece)
		}
	}
}
