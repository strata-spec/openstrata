package inference

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Spinner displays an animated spinner on a single line while
// a long-running operation is in progress.
// It is stopped by calling the returned cancel func.
// Safe to use only from one goroutine at a time.
type Spinner struct {
	w       io.Writer
	mu      sync.Mutex
	running bool
}

func NewSpinner(w io.Writer) *Spinner {
	return &Spinner{w: w}
}

// Start begins spinning with the given label.
// Returns a stop func that must be called to clear the spinner line.
func (s *Spinner) Start(label string) func() {
	frames := []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

	s.mu.Lock()
	s.running = true
	s.mu.Unlock()

	done := make(chan struct{})
	stopped := make(chan struct{})
	ticker := time.NewTicker(80 * time.Millisecond)

	go func() {
		defer close(stopped)
		defer ticker.Stop()

		i := 0
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				s.mu.Lock()
				if s.running {
					fmt.Fprintf(s.w, "\r  %s %s", frames[i%len(frames)], label)
				}
				s.mu.Unlock()
				i++
			}
		}
	}()

	return func() {
		close(done)
		<-stopped

		s.mu.Lock()
		fmt.Fprintf(s.w, "\r%s\r", spaces(len(label)+6))
		s.running = false
		s.mu.Unlock()
	}
}

func spaces(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = ' '
	}
	return string(b)
}
