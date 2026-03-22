package inference

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Progress receives pipeline stage events. Implementations must be
// safe to call from multiple goroutines.
type Progress interface {
	// Stage marks the start of a named pipeline stage.
	// The returned func must be called when the stage completes.
	// Calling the returned func with a non-nil error marks the stage failed.
	Stage(name string) func(err error)

	// Item reports progress on a unit of work within a stage.
	// label is e.g. "table orders" or "column users.email".
	Item(label string)

	// Info prints a one-line informational message.
	Info(msg string)
}

// NoOpProgress discards all progress events. Used in tests.
type NoOpProgress struct{}

func (NoOpProgress) Stage(name string) func(error) { return func(error) {} }
func (NoOpProgress) Item(label string)             {}
func (NoOpProgress) Info(msg string)               {}

// StderrProgress writes human-readable progress to w (typically os.Stderr).
type StderrProgress struct {
	w     io.Writer
	start time.Time
	mu    sync.Mutex
}

func NewStderrProgress(w io.Writer) *StderrProgress {
	return &StderrProgress{w: w, start: time.Now()}
}

// Stage prints "► {name}" on start and
// "  ✓ {name} ({elapsed})" or "  ✗ {name}: {err} ({elapsed})" on completion.
func (p *StderrProgress) Stage(name string) func(error) {
	p.mu.Lock()
	fmt.Fprintf(p.w, "\n► %s\n", name)
	p.mu.Unlock()

	stageStart := time.Now()
	return func(err error) {
		elapsed := time.Since(stageStart).Round(time.Millisecond)

		p.mu.Lock()
		defer p.mu.Unlock()

		if err != nil {
			fmt.Fprintf(p.w, "  ✗ %s: %v (%s)\n", name, err, elapsed)
			return
		}

		fmt.Fprintf(p.w, "  ✓ %s (%s)\n", name, elapsed)
	}
}

// Item prints "  → {label}".
func (p *StderrProgress) Item(label string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, "\r  %-70s", label)
}

// Info prints "  {msg}".
func (p *StderrProgress) Info(msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Fprintf(p.w, "  %s\n", msg)
}
