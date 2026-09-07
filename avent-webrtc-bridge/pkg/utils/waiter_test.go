package utils

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWaiterCompletionAndCancellation(t *testing.T) {
	want := errors.New("handshake failed")
	for range 100 {
		var w Waiter
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				if err := w.Wait(); !errors.Is(err, want) {
					t.Errorf("got %v, want %v", err, want)
				}
			})
		}
		w.Done(want)
		w.Done(nil) // First completion wins.
		wg.Wait()
		if err := <-w.WaitChan(); !errors.Is(err, want) {
			t.Fatal("late WaitChan lost completed error")
		}
	}
	var w Waiter
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := w.WaitContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	w.Done(nil)
	if err := w.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestWaiterMultipleTasks(t *testing.T) {
	var w Waiter
	w.Add(2)
	w.Done(nil)
	ctx, cancel := context.WithTimeout(t.Context(), time.Millisecond)
	defer cancel()
	if err := w.WaitContext(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiter completed before last task")
	}
	w.Done(nil)
	if err := w.Wait(); err != nil {
		t.Fatal(err)
	}
}
