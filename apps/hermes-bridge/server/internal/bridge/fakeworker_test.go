package bridge

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The test binary doubles as a scriptable worker: `<test binary> fake-worker <mode> [flags...]`.
// It is a MOCK of hermes_worker.py's protocol, used only for transport and failure tests.
func runFakeWorker(mode string) {
	out := os.Stdout
	var mu sync.Mutex
	emit := func(v any) {
		b, _ := json.Marshal(v)
		mu.Lock()
		out.Write(append(b, '\n'))
		mu.Unlock()
	}
	switch mode {
	case "not-configured":
		emit(map[string]any{"type": "fatal", "code": "HERMES_NOT_CONFIGURED", "message": "ProviderNotConfiguredError: No LLM provider configured."})
		os.Exit(3)
	case "die-on-start":
		os.Exit(1)
	case "noise-on-start":
		fmt.Fprintln(out, "Loading tools...")
	}
	emit(map[string]any{"type": "ready", "pid": os.Getpid(), "hermesVersion": "fake", "tools": []string{}})

	var remembered string
	turns := 0
	cancels := make(chan string, 4)
	asks := make(chan map[string]any)
	go func() {
		sc := bufio.NewScanner(os.Stdin)
		sc.Buffer(make([]byte, 1<<20), 1<<20)
		for sc.Scan() {
			var rec map[string]any
			if json.Unmarshal(sc.Bytes(), &rec) != nil {
				os.Exit(5)
			}
			switch rec["type"] {
			case "ask":
				asks <- rec
			case "cancel":
				cancels <- rec["requestId"].(string)
			case "shutdown":
				os.Exit(0)
			}
		}
		os.Exit(0)
	}()

	for rec := range asks {
		id, msg := rec["requestId"].(string), rec["message"].(string)
		result := func(text string) {
			turns++
			emit(map[string]any{"type": "result", "requestId": id, "ok": true, "text": text, "messageCount": turns * 2, "historyBytes": turns * 100})
		}
		// Deltas are emitted whether or not the ask set stream, so tests prove the server gates them.
		delta := func(reqID, text string) { emit(map[string]any{"type": "delta", "requestId": reqID, "text": text}) }
		switch {
		case strings.HasPrefix(msg, "STREAM "):
			n, _ := strconv.Atoi(strings.Fields(msg)[1])
			var full strings.Builder
			for i := 0; i < n; i++ {
				piece := fmt.Sprintf("w%d ", i)
				full.WriteString(piece)
				delta(id, piece)
			}
			delta("some-other-request", "must never reach the client")
			result(full.String())
		case msg == "STREAM-REVISED":
			delta(id, "draft that a provider retry ")
			delta(id, "later replaced")
			result("the authoritative final answer")
		case msg == "STREAM-THEN-HANG":
			delta(id, "partial ")
			<-cancels
			delta(id, "late delta after cancel")
			emit(map[string]any{"type": "result", "requestId": id, "ok": false, "code": "CANCELLED", "message": "interrupted"})
		case msg == "STREAM-FLOOD":
			for i := 0; i < 64; i++ {
				delta(id, strings.Repeat("y", 64<<10))
			}
			result("flood done")
		case msg == "CRASH":
			os.Exit(2)
		case msg == "GARBAGE":
			fmt.Fprintln(out, "this is not a protocol record")
		case msg == "PARTIAL":
			out.Write([]byte(`{"type":"result","requestId":"` + id + `"`))
			os.Exit(0)
		case msg == "BIG":
			result(strings.Repeat("x", 2<<20))
		case msg == "FAIL":
			emit(map[string]any{"type": "result", "requestId": id, "ok": false, "code": "HERMES_ERROR", "message": "provider said no"})
		case strings.HasPrefix(msg, "SLEEP "), strings.HasPrefix(msg, "HANG "):
			d, _ := time.ParseDuration(strings.Fields(msg)[1])
			if strings.HasPrefix(msg, "HANG ") {
				time.Sleep(d)
				result("woke up")
				continue
			}
			select {
			case <-time.After(d):
				result("slept " + d.String())
			case <-cancels:
				emit(map[string]any{"type": "result", "requestId": id, "ok": false, "code": "CANCELLED", "message": "interrupted"})
			}
		case strings.HasPrefix(msg, "remember "):
			remembered = strings.TrimPrefix(msg, "remember ")
			result("ok, remembered")
		case msg == "recall":
			if remembered == "" {
				result("nothing remembered")
			} else {
				result("you said " + remembered)
			}
		default:
			result("echo: " + msg)
		}
	}
}
