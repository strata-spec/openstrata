package runlog

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Entry is a single structured run-log event.
type Entry struct {
	Time       string `json:"time"`
	Stage      int    `json:"stage"`
	Event      string `json:"event"`
	Table      string `json:"table,omitempty"`
	Column     string `json:"column,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`

	DistinctCount int64 `json:"distinct_count,omitempty"`
	NullCount     int64 `json:"null_count,omitempty"`

	TokensIn  int `json:"tokens_in,omitempty"`
	TokensOut int `json:"tokens_out,omitempty"`

	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// Logger writes newline-delimited JSON entries to a file.
// Safe for concurrent use.
type Logger struct {
	mu sync.Mutex
	f  *os.File
}

// Open creates or truncates the log file at path.
func Open(path string) (*Logger, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("runlog: %w", err)
	}
	return &Logger{f: f}, nil
}

// NoOp returns a nil logger; Write and Close are nil-safe no-ops.
func NoOp() *Logger {
	return nil
}

// Write appends a single log entry. Nil-safe.
func (l *Logger) Write(e Entry) {
	if l == nil {
		return
	}
	e.Time = time.Now().UTC().Format(time.RFC3339)
	b, err := json.Marshal(e)
	if err != nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.f.Write(b)
	_, _ = l.f.Write([]byte("\n"))
}

// Close flushes and closes the underlying file. Nil-safe.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	return l.f.Close()
}
