package middleware

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/pivovarit/outboxd"
)

func TestRecover_CatchesStringPanic(t *testing.T) {
	wrapped := Recover()(func(_ context.Context, _ outboxd.Message) error {
		panic("boom")
	})

	err := wrapped(context.Background(), outboxd.Message{ID: 1})

	if err == nil {
		t.Fatal("expected non-nil error from recovered panic")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("expected error to contain 'boom', got %q", err.Error())
	}
}

func TestRecover_CatchesNonStringPanic(t *testing.T) {
	cases := []struct {
		name     string
		value    any
		wantSubs string
	}{
		{"int", 42, "42"},
		{"error", errors.New("panicked-error"), "panicked-error"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wrapped := Recover()(func(_ context.Context, _ outboxd.Message) error {
				panic(tc.value)
			})

			err := wrapped(context.Background(), outboxd.Message{ID: 1})

			if err == nil {
				t.Fatal("expected non-nil error")
			}
			if !strings.Contains(err.Error(), tc.wantSubs) {
				t.Errorf("expected error to contain %q, got %q", tc.wantSubs, err.Error())
			}
		})
	}
}

func TestRecover_PassesThroughNormalErrors(t *testing.T) {
	sentinel := errors.New("normal handler error")
	wrapped := Recover()(func(_ context.Context, _ outboxd.Message) error {
		return sentinel
	})

	err := wrapped(context.Background(), outboxd.Message{ID: 1})

	if !errors.Is(err, sentinel) {
		t.Errorf("expected sentinel error to pass through unchanged, got %v", err)
	}
}

func TestRecover_PassesThroughSuccess(t *testing.T) {
	wrapped := Recover()(func(_ context.Context, _ outboxd.Message) error {
		return nil
	})

	if err := wrapped(context.Background(), outboxd.Message{ID: 1}); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

// TestRecover_IsReentrantAcrossCalls proves that a single Recover-wrapped
// handler keeps recovering across repeated invocations, which is what the
// outboxd retry loop will do when a handler panics.
func TestRecover_IsReentrantAcrossCalls(t *testing.T) {
	wrapped := Recover()(func(_ context.Context, _ outboxd.Message) error {
		panic("boom")
	})

	const attempts = 5
	for i := 0; i < attempts; i++ {
		err := wrapped(context.Background(), outboxd.Message{ID: int64(i)})
		if err == nil {
			t.Fatalf("attempt %d: expected error, got nil", i)
		}
	}
}
