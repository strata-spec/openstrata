package runlog

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestRunlogWrite(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/strata.log"
	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}

	l.Write(Entry{Stage: 1, Event: "stage_start", Message: "start"})
	l.Write(Entry{Stage: 1, Event: "stage_complete", DurationMS: 20})
	l.Write(Entry{Stage: 3, Event: "column_profiled", Table: "users", Column: "email", DistinctCount: 10})

	if err := l.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d", len(lines))
	}

	for _, line := range lines {
		var e Entry
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("line is not valid json: %q (%v)", line, err)
		}
		if e.Time == "" {
			t.Fatalf("expected time to be populated: %q", line)
		}
	}
}

func TestRunlogNilSafe(t *testing.T) {
	t.Parallel()

	var l *Logger
	l.Write(Entry{Stage: 1, Event: "noop"})
	if err := l.Close(); err != nil {
		t.Fatalf("expected nil-safe Close, got %v", err)
	}
}

func TestRunlogConcurrentWrites(t *testing.T) {
	t.Parallel()

	path := t.TempDir() + "/strata.log"
	l, err := Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}

	var wg sync.WaitGroup
	workers := 10
	perWorker := 10
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				l.Write(Entry{Stage: 3, Event: "column_profiled", Table: "t", Column: "c"})
			}
		}(i)
	}
	wg.Wait()

	if err := l.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != workers*perWorker {
		t.Fatalf("expected %d lines, got %d", workers*perWorker, len(lines))
	}

	for _, line := range lines {
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			t.Fatalf("invalid json line %q: %v", line, err)
		}
	}
}
