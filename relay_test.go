package outboxd

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeSource struct {
	messages  []Message
	pos       int
	confirmed []int64
}

func (f *fakeSource) Next(ctx context.Context) (Message, error) {
	if f.pos < len(f.messages) {
		msg := f.messages[f.pos]
		f.pos++
		return msg, nil
	}
	<-ctx.Done()
	return Message{}, ctx.Err()
}

func (f *fakeSource) Remaining() int {
	remaining := len(f.messages) - f.pos
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (f *fakeSource) Confirm(_ context.Context, ids ...int64) error {
	f.confirmed = append(f.confirmed, ids...)
	return nil
}

func (f *fakeSource) Close(_ context.Context) {}

func startRelayWithFakeSource(ctx context.Context, src *fakeSource, handler Handler, cfg Config) error {
	cfg.setDefaults()
	r := &Relay{handler: handler, cfg: cfg}
	return r.run(ctx, src)
}

func TestRelay_DeliversMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var received []Message
	handler := func(_ context.Context, msg Message) error {
		received = append(received, msg)
		cancel()
		return nil
	}

	src := &fakeSource{
		messages: []Message{{ID: 1, Topic: "orders", Payload: []byte("hello")}},
	}

	err := startRelayWithFakeSource(ctx, src, handler, Config{RetryDelay: time.Millisecond})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if len(received) != 1 {
		t.Fatalf("expected handler called once, got %d", len(received))
	}
	if received[0].ID != 1 {
		t.Errorf("expected message id 1, got %d", received[0].ID)
	}
	if len(src.confirmed) != 1 || src.confirmed[0] != 1 {
		t.Errorf("expected id 1 confirmed, got %v", src.confirmed)
	}
}

func TestRelay_RetriesOnHandlerError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	handler := func(_ context.Context, msg Message) error {
		attempts++
		if attempts < 3 {
			return errors.New("transient error")
		}
		cancel()
		return nil
	}

	src := &fakeSource{
		messages: []Message{{ID: 42, Topic: "test", Payload: []byte("payload")}},
	}

	startRelayWithFakeSource(ctx, src, handler, Config{RetryDelay: time.Millisecond}) //nolint:errcheck — we only care about side effects

	if attempts != 3 {
		t.Errorf("expected 3 handler attempts, got %d", attempts)
	}
	if len(src.confirmed) != 1 || src.confirmed[0] != 42 {
		t.Errorf("expected id 42 confirmed after retry, got %v", src.confirmed)
	}
}

func TestRelay_DropsMessageAfterMaxRetries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	handler := func(_ context.Context, _ Message) error {
		attempts++
		return errors.New("permanent error")
	}

	var droppedMsg Message
	var droppedErr error

	src := &fakeSource{
		messages: []Message{
			{ID: 1, Topic: "poison", Payload: []byte("bad")},
			{ID: 2, Topic: "ok", Payload: []byte("good")},
		},
	}

	cfg := Config{
		RetryDelay: time.Millisecond,
		MaxRetries: 3,
		OnDropped: func(msg Message, err error) {
			droppedMsg = msg
			droppedErr = err
		},
	}

	realHandler := func(_ context.Context, msg Message) error {
		if msg.ID == 2 {
			cancel()
			return nil
		}
		return handler(ctx, msg)
	}

	err := startRelayWithFakeSource(ctx, src, realHandler, cfg)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts before drop, got %d", attempts)
	}
	if droppedMsg.ID != 1 {
		t.Errorf("expected dropped message id 1, got %d", droppedMsg.ID)
	}
	if droppedErr == nil || droppedErr.Error() != "permanent error" {
		t.Errorf("expected dropped error 'permanent error', got %v", droppedErr)
	}
	if len(src.confirmed) != 2 {
		t.Errorf("expected both messages confirmed, got %v", src.confirmed)
	}
}

func TestRelay_RetriesForeverWhenMaxRetriesZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	handler := func(_ context.Context, _ Message) error {
		attempts++
		if attempts >= 10 {
			cancel()
			return nil
		}
		return errors.New("transient error")
	}

	src := &fakeSource{
		messages: []Message{{ID: 1, Topic: "test", Payload: []byte("data")}},
	}

	err := startRelayWithFakeSource(ctx, src, handler, Config{RetryDelay: time.Millisecond, MaxRetries: 0})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if attempts != 10 {
		t.Errorf("expected 10 attempts (unlimited retries), got %d", attempts)
	}
}

func TestRelay_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	src := &fakeSource{}

	err := startRelayWithFakeSource(ctx, src, func(_ context.Context, _ Message) error { return nil }, Config{RetryDelay: time.Millisecond})

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}
