// (c) go2rtc

package utils

import (
	"context"
	"sync"
)

// Waiter support:
// - autotart on first Wait
// - block new waiters after last Done
// - safe Done after finish
type Waiter struct {
	mu    sync.Mutex
	state int // state < 0 means finish
	err   error
	done  chan struct{}
}

// Add registers work before terminal completion; completed waiters cannot be reused.
func (w *Waiter) Add(delta int) {
	w.mu.Lock()
	if w.state >= 0 {
		w.state += delta
	}
	w.mu.Unlock()
}

// Wait returns the terminal result, registering one implicit task for a zero-value waiter.
func (w *Waiter) Wait() error {
	return w.WaitContext(context.Background())
}

// WaitContext bounds a handshake without leaving a waiting goroutine behind.
func (w *Waiter) WaitContext(ctx context.Context) error {
	w.mu.Lock()
	if w.state < 0 {
		err := w.err
		w.mu.Unlock()
		return err
	}
	if w.done == nil {
		w.done = make(chan struct{})
	}
	// first wait auto start waiter
	if w.state == 0 {
		w.state++
	}
	done := w.done
	w.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.err
	}
}

// Done completes one task and publishes the final result exactly once before waking waiters.
func (w *Waiter) Done(err error) {
	w.mu.Lock()

	// safe run Done only when have tasks
	if w.state > 0 {
		w.state--
	}

	// block waiter for any operations after last done
	if w.state == 0 {
		w.state = -1
		w.err = err
		if w.done != nil {
			close(w.done)
		}
	}

	w.mu.Unlock()
}

// WaitChan delivers the terminal result through a buffered channel, including for late subscribers.
func (w *Waiter) WaitChan() <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- w.Wait() }()
	return ch
}
