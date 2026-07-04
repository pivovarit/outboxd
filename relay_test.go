package outboxd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeSource struct {
	mu                sync.Mutex
	messages          []Message
	pos               int
	remainingOverride *int
	confirmed         []int64
	confirmCalls      int
	gate              chan struct{}
}

func (f *fakeSource) Next(ctx context.Context) (Message, int, error) {
	f.mu.Lock()
	if f.gate != nil && f.pos > 0 {
		f.mu.Unlock()
		select {
		case <-f.gate:
		case <-ctx.Done():
			return Message{}, 0, ctx.Err()
		}
		f.mu.Lock()
	}
	if f.pos < len(f.messages) {
		msg := f.messages[f.pos]
		f.pos++
		var remaining int
		if f.remainingOverride != nil {
			remaining = *f.remainingOverride
		} else {
			remaining = len(f.messages) - f.pos
		}
		f.mu.Unlock()
		return msg, remaining, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return Message{}, 0, ctx.Err()
}

func (f *fakeSource) Confirm(_ context.Context, ids ...int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirmed = append(f.confirmed, ids...)
	f.confirmCalls++
	return nil
}

func (f *fakeSource) Close(_ context.Context) {}

func (f *fakeSource) confirmedSnapshot() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int64, len(f.confirmed))
	copy(out, f.confirmed)
	return out
}

func (f *fakeSource) confirmCallsSnapshot() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.confirmCalls
}

func startRelayWithFakeSource(ctx context.Context, src *fakeSource, handler Handler, cfg Config) error {
	cfg.setDefaults()
	r := &Relay{handler: wrap(handler, cfg.Middlewares), cfg: cfg}
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
	confirmed := src.confirmedSnapshot()
	if len(confirmed) != 1 || confirmed[0] != 1 {
		t.Errorf("expected id 1 confirmed, got %v", confirmed)
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

	startRelayWithFakeSource(ctx, src, handler, Config{RetryDelay: time.Millisecond})

	if attempts != 3 {
		t.Errorf("expected 3 handler attempts, got %d", attempts)
	}
	confirmed := src.confirmedSnapshot()
	if len(confirmed) != 1 || confirmed[0] != 42 {
		t.Errorf("expected id 42 confirmed after retry, got %v", confirmed)
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
	if attempts != 4 {
		t.Errorf("expected 4 attempts (1 initial + 3 retries) before drop, got %d", attempts)
	}
	if droppedMsg.ID != 1 {
		t.Errorf("expected dropped message id 1, got %d", droppedMsg.ID)
	}
	if droppedErr == nil || droppedErr.Error() != "permanent error" {
		t.Errorf("expected dropped error 'permanent error', got %v", droppedErr)
	}
	confirmed := src.confirmedSnapshot()
	if len(confirmed) != 2 {
		t.Errorf("expected both messages confirmed, got %v", confirmed)
	}
}

func TestRelay_SurvivesOnDroppedPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	handler := func(_ context.Context, msg Message) error {
		if msg.ID == 2 {
			cancel()
			return nil
		}
		return errors.New("permanent error")
	}

	src := &fakeSource{
		messages: []Message{
			{ID: 1, Topic: "poison", Payload: []byte("bad")},
			{ID: 2, Topic: "ok", Payload: []byte("good")},
		},
	}

	cfg := Config{
		RetryDelay: time.Millisecond,
		MaxRetries: 1,
		OnDropped:  func(Message, error) { panic("dead-letter sink exploded") },
	}

	err := startRelayWithFakeSource(ctx, src, handler, cfg)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	confirmed := src.confirmedSnapshot()
	if len(confirmed) != 2 {
		t.Errorf("expected both messages confirmed despite OnDropped panic, got %v", confirmed)
	}
}

func TestRelay_DoesNotDropWhenFinalAttemptCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	attempts := 0
	handler := func(hCtx context.Context, _ Message) error {
		attempts++
		if attempts < 3 {
			return errors.New("transient error")
		}
		cancel()
		return hCtx.Err()
	}

	dropped := false
	src := &fakeSource{
		messages: []Message{{ID: 7, Topic: "test", Payload: []byte("data")}},
	}

	cfg := Config{
		RetryDelay: time.Millisecond,
		MaxRetries: 2,
		OnDropped:  func(Message, error) { dropped = true },
	}

	err := startRelayWithFakeSource(ctx, src, handler, cfg)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if dropped {
		t.Error("message must not be dropped on an attempt aborted by shutdown")
	}
	if confirmed := src.confirmedSnapshot(); len(confirmed) != 0 {
		t.Errorf("expected no confirmations (message must be redelivered after restart), got %v", confirmed)
	}
}

func TestRelay_FailStopHaltsInsteadOfDropping(t *testing.T) {
	// The timeout is a hang guard: a correct fail-stop returns well before it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	attempts := 0
	dropped := false
	handler := func(_ context.Context, msg Message) error {
		if msg.ID == 1 {
			return nil
		}
		attempts++
		return errors.New("permanent error")
	}

	src := &fakeSource{
		messages: []Message{
			{ID: 1, Topic: "ok"},
			{ID: 2, Topic: "poison"},
		},
	}

	cfg := Config{
		RetryDelay: time.Millisecond,
		MaxRetries: 2,
		FailStop:   true,
		OnDropped:  func(Message, error) { dropped = true },
	}

	err := startRelayWithFakeSource(ctx, src, handler, cfg)

	if !errors.Is(err, ErrRetriesExhausted) {
		t.Fatalf("expected ErrRetriesExhausted, got %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts (1 initial + 2 retries) before halt, got %d", attempts)
	}
	if dropped {
		t.Error("OnDropped must not fire when FailStop halts the relay")
	}
	confirmed := src.confirmedSnapshot()
	if len(confirmed) != 1 || confirmed[0] != 1 {
		t.Errorf("expected only id 1 flushed before halt, got %v", confirmed)
	}
}

func TestStart_ReturnsTerminalErrorOnFailStop(t *testing.T) {
	// The timeout is a hang guard: a correct fail-stop returns well before it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handler := func(_ context.Context, _ Message) error {
		return errors.New("permanent error")
	}
	r := New("dsn-unused", handler, Config{
		RetryDelay: time.Millisecond,
		MaxRetries: 1,
		FailStop:   true,
	})

	sourcesCreated := 0
	r.newSource = func(_ context.Context, _ string, _ Config) (source, error) {
		sourcesCreated++
		return &fakeSource{messages: []Message{{ID: 1, Topic: "poison"}}}, nil
	}

	err := r.Start(ctx)

	if !errors.Is(err, ErrRetriesExhausted) {
		t.Fatalf("expected ErrRetriesExhausted from Start, got %v", err)
	}
	if sourcesCreated != 1 {
		t.Errorf("expected no reconnect after terminal error, got %d source creations", sourcesCreated)
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

func TestRelay_StatusTracksDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := Config{RetryDelay: time.Millisecond}
	cfg.setDefaults()
	r := &Relay{cfg: cfg}
	r.handler = func(_ context.Context, msg Message) error {
		if msg.ID == 2 {
			cancel()
		}
		return nil
	}

	src := &fakeSource{messages: []Message{{ID: 1}, {ID: 2}}}
	_ = r.run(ctx, src)

	st := r.Status()
	if st.Delivered != 2 {
		t.Errorf("expected 2 delivered, got %d", st.Delivered)
	}
	if st.LastDeliveredAt.IsZero() {
		t.Error("expected LastDeliveredAt to be set")
	}
	if st.Retrying {
		t.Error("expected no active retry after clean deliveries")
	}
}

func TestRelay_StatusExposesActiveRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := Config{RetryDelay: time.Millisecond}
	cfg.setDefaults()
	r := &Relay{cfg: cfg}

	secondAttempt := make(chan struct{})
	attempts := 0
	r.handler = func(_ context.Context, _ Message) error {
		attempts++
		if attempts == 2 {
			close(secondAttempt)
		}
		return errors.New("permanent error")
	}

	src := &fakeSource{messages: []Message{{ID: 7, Topic: "poison"}}}
	done := make(chan struct{})
	go func() {
		_ = r.run(ctx, src)
		close(done)
	}()

	// Once attempt 2 has started, attempt 1's failure is already recorded.
	<-secondAttempt
	st := r.Status()
	cancel()
	<-done

	if !st.Retrying {
		t.Fatal("expected Retrying=true while a message is stuck in retry")
	}
	if st.RetryingID != 7 {
		t.Errorf("expected RetryingID 7, got %d", st.RetryingID)
	}
	if st.RetryAttempts < 1 {
		t.Errorf("expected at least 1 recorded attempt, got %d", st.RetryAttempts)
	}
	if st.RetryingSince.IsZero() {
		t.Error("expected RetryingSince to be set")
	}
}

func TestRelay_StatusClearsRetryOnRecoveryAndDrop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := Config{
		RetryDelay: time.Millisecond,
		MaxRetries: 2,
		OnDropped:  func(Message, error) {},
	}
	cfg.setDefaults()
	r := &Relay{cfg: cfg}

	attempts := map[int64]int{}
	r.handler = func(_ context.Context, msg Message) error {
		attempts[msg.ID]++
		switch {
		case msg.ID == 1 && attempts[1] == 1:
			return errors.New("transient error") // recovers on attempt 2
		case msg.ID == 2:
			return errors.New("permanent error") // dropped after retries
		case msg.ID == 3:
			cancel()
		}
		return nil
	}

	src := &fakeSource{messages: []Message{{ID: 1}, {ID: 2}, {ID: 3}}}
	_ = r.run(ctx, src)

	st := r.Status()
	if st.Retrying {
		t.Errorf("expected retry state cleared after recovery and drop, got %+v", st)
	}
	if st.Delivered != 2 {
		t.Errorf("expected 2 delivered (dropped message not counted), got %d", st.Delivered)
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

func TestRelay_MiddlewareIsInvoked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var mwCalls int
	mw := func(next Handler) Handler {
		return func(ctx context.Context, msg Message) error {
			mwCalls++
			return next(ctx, msg)
		}
	}

	handler := func(_ context.Context, _ Message) error {
		cancel()
		return nil
	}

	src := &fakeSource{
		messages: []Message{{ID: 1, Topic: "t", Payload: []byte("p")}},
	}

	cfg := Config{
		RetryDelay:  time.Millisecond,
		Middlewares: []Middleware{mw},
	}

	err := startRelayWithFakeSource(ctx, src, handler, cfg)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if mwCalls != 1 {
		t.Errorf("expected middleware invoked once, got %d", mwCalls)
	}
}

func TestRelay_MiddlewareAppliedInOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var trace []string
	makeMW := func(name string) Middleware {
		return func(next Handler) Handler {
			return func(ctx context.Context, msg Message) error {
				trace = append(trace, name+"-in")
				err := next(ctx, msg)
				trace = append(trace, name+"-out")
				return err
			}
		}
	}

	handler := func(_ context.Context, _ Message) error {
		trace = append(trace, "handler")
		cancel()
		return nil
	}

	src := &fakeSource{messages: []Message{{ID: 1}}}

	cfg := Config{
		RetryDelay:  time.Millisecond,
		Middlewares: []Middleware{makeMW("A"), makeMW("B"), makeMW("C")},
	}

	_ = startRelayWithFakeSource(ctx, src, handler, cfg)

	want := []string{"A-in", "B-in", "C-in", "handler", "C-out", "B-out", "A-out"}
	if len(trace) != len(want) {
		t.Fatalf("trace length mismatch: got %v, want %v", trace, want)
	}
	for i := range want {
		if trace[i] != want[i] {
			t.Errorf("trace[%d]: got %q, want %q", i, trace[i], want[i])
		}
	}
}

func TestRelay_MiddlewareSeesEachRetryAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var mwCalls int
	mw := func(next Handler) Handler {
		return func(ctx context.Context, msg Message) error {
			mwCalls++
			return next(ctx, msg)
		}
	}

	attempts := 0
	handler := func(_ context.Context, _ Message) error {
		attempts++
		if attempts < 3 {
			return errors.New("transient")
		}
		cancel()
		return nil
	}

	src := &fakeSource{messages: []Message{{ID: 1}}}

	cfg := Config{
		RetryDelay:  time.Millisecond,
		Middlewares: []Middleware{mw},
	}

	_ = startRelayWithFakeSource(ctx, src, handler, cfg)

	if mwCalls != 3 {
		t.Errorf("expected middleware invoked 3 times (once per attempt), got %d", mwCalls)
	}
}

func TestRelay_NilMiddlewaresBehavesIdentically(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var received []Message
	handler := func(_ context.Context, msg Message) error {
		received = append(received, msg)
		cancel()
		return nil
	}

	src := &fakeSource{messages: []Message{{ID: 1, Topic: "t", Payload: []byte("p")}}}

	// Middlewares explicitly nil.
	err := startRelayWithFakeSource(ctx, src, handler, Config{
		RetryDelay:  time.Millisecond,
		Middlewares: nil,
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if len(received) != 1 || received[0].ID != 1 {
		t.Errorf("expected single message id 1, got %v", received)
	}
	confirmed := src.confirmedSnapshot()
	if len(confirmed) != 1 || confirmed[0] != 1 {
		t.Errorf("expected id 1 confirmed, got %v", confirmed)
	}
}

func TestSchemaConfig_TopicEnabled(t *testing.T) {
	tests := []struct {
		column string
		want   bool
	}{
		{"topic", true},
		{"event_type", true},
		{"", false},
		{"-", false},
	}
	for _, tt := range tests {
		cfg := SchemaConfig{TopicColumn: tt.column}
		if got := cfg.topicEnabled(); got != tt.want {
			t.Errorf("topicEnabled(%q) = %v, want %v", tt.column, got, tt.want)
		}
	}
}

func TestSchemaConfig_CreatedAtEnabled(t *testing.T) {
	tests := []struct {
		column string
		want   bool
	}{
		{"created_at", true},
		{"inserted_at", true},
		{"", false},
		{"-", false},
	}
	for _, tt := range tests {
		cfg := SchemaConfig{CreatedAtColumn: tt.column}
		if got := cfg.createdAtEnabled(); got != tt.want {
			t.Errorf("createdAtEnabled(%q) = %v, want %v", tt.column, got, tt.want)
		}
	}
}

func TestSetDefaults_FiltersExtraColumnCollisions(t *testing.T) {
	cfg := Config{
		Schema: SchemaConfig{
			Table:           "outbox",
			IDColumn:        "id",
			TopicColumn:     "topic",
			PayloadColumn:   "payload",
			CreatedAtColumn: "created_at",
			ExtraColumns:    []string{"aggregate_id", "id", "topic", "payload", "created_at", "partition_key"},
		},
	}
	cfg.setDefaults()

	want := []string{"aggregate_id", "partition_key"}
	if len(cfg.Schema.ExtraColumns) != len(want) {
		t.Fatalf("ExtraColumns = %v, want %v", cfg.Schema.ExtraColumns, want)
	}
	for i, got := range cfg.Schema.ExtraColumns {
		if got != want[i] {
			t.Errorf("ExtraColumns[%d] = %q, want %q", i, got, want[i])
		}
	}
}

func TestSetDefaults_FiltersExtraColumnCollisionsWithCustomNames(t *testing.T) {
	cfg := Config{
		Schema: SchemaConfig{
			IDColumn:        "event_id",
			TopicColumn:     "event_type",
			PayloadColumn:   "data",
			CreatedAtColumn: "inserted_at",
			ExtraColumns:    []string{"event_id", "event_type", "data", "inserted_at", "partition_key"},
		},
	}
	cfg.setDefaults()

	want := []string{"partition_key"}
	if len(cfg.Schema.ExtraColumns) != len(want) {
		t.Fatalf("ExtraColumns = %v, want %v", cfg.Schema.ExtraColumns, want)
	}
	if cfg.Schema.ExtraColumns[0] != "partition_key" {
		t.Errorf("ExtraColumns[0] = %q, want %q", cfg.Schema.ExtraColumns[0], "partition_key")
	}
}

func TestSetDefaults_NoFilteringWhenNoExtras(t *testing.T) {
	cfg := Config{}
	cfg.setDefaults()

	if cfg.Schema.ExtraColumns != nil {
		t.Errorf("ExtraColumns should be nil, got %v", cfg.Schema.ExtraColumns)
	}
}

func TestSetDefaults_DisabledColumnsNotFiltered(t *testing.T) {
	cfg := Config{
		Schema: SchemaConfig{
			TopicColumn:     "-",
			CreatedAtColumn: "-",
			ExtraColumns:    []string{"event_type", "inserted_at"},
		},
	}
	cfg.setDefaults()

	want := []string{"event_type", "inserted_at"}
	if len(cfg.Schema.ExtraColumns) != len(want) {
		t.Fatalf("ExtraColumns = %v, want %v", cfg.Schema.ExtraColumns, want)
	}
}

func benchmarkWrap(b *testing.B, n int) {
	mws := make([]Middleware, n)
	for i := range mws {
		mws[i] = func(next Handler) Handler {
			return func(ctx context.Context, msg Message) error {
				return next(ctx, msg)
			}
		}
	}
	handler := func(_ context.Context, _ Message) error { return nil }
	ctx := context.Background()
	msg := Message{ID: 1}

	wrapped := wrap(handler, mws)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = wrapped(ctx, msg)
	}
}

func BenchmarkWrap_None(b *testing.B) { benchmarkWrap(b, 0) }
func BenchmarkWrap_One(b *testing.B)  { benchmarkWrap(b, 1) }
func BenchmarkWrap_Five(b *testing.B) { benchmarkWrap(b, 5) }

func TestNextDelay_DoublesAfterUnstableRun(t *testing.T) {
	got := nextDelay(time.Second, time.Second, 0)
	if got != 2*time.Second {
		t.Errorf("expected 2s after fast-failing run, got %v", got)
	}
}

func TestNextDelay_GrowsMonotonicallyUntilCap(t *testing.T) {
	delay := time.Second
	for i := 0; i < 20; i++ {
		delay = nextDelay(delay, time.Second, 0)
	}
	if delay != maxRetryDelay {
		t.Errorf("expected delay to reach cap after sustained failures, got %v", delay)
	}
}

func TestNextDelay_ResetsAfterStableRun(t *testing.T) {
	got := nextDelay(maxRetryDelay, time.Second, maxRetryDelay)
	if got != time.Second {
		t.Errorf("expected reset to base after stable run, got %v", got)
	}
}

func TestNextDelay_ShortRunDoesNotReset(t *testing.T) {
	got := nextDelay(4*time.Second, time.Second, maxRetryDelay-time.Second)
	if got != 8*time.Second {
		t.Errorf("expected delay to keep doubling after borderline-short run, got %v", got)
	}
}

func TestConfig_KeepaliveIntervalDefault(t *testing.T) {
	cfg := Config{}
	cfg.setDefaults()
	if cfg.KeepaliveInterval != 5*time.Second {
		t.Errorf("expected KeepaliveInterval default 5s, got %v", cfg.KeepaliveInterval)
	}
}

func TestConfig_KeepaliveIntervalPreservesExplicitValue(t *testing.T) {
	cfg := Config{KeepaliveInterval: 250 * time.Millisecond}
	cfg.setDefaults()
	if cfg.KeepaliveInterval != 250*time.Millisecond {
		t.Errorf("expected explicit KeepaliveInterval preserved, got %v", cfg.KeepaliveInterval)
	}
}

func TestRelay_TickerConfirmsPendingMidBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	override := 1
	src := &fakeSource{
		messages:          []Message{{ID: 1}, {ID: 2}},
		remainingOverride: &override,
	}

	handler := func(_ context.Context, msg Message) error {
		if msg.ID == 2 {
			// Let the ticker fire once before we cancel.
			time.Sleep(60 * time.Millisecond)
			cancel()
		}
		return nil
	}

	_ = startRelayWithFakeSource(ctx, src, handler, Config{
		RetryDelay:        time.Millisecond,
		KeepaliveInterval: 20 * time.Millisecond,
	})

	if src.confirmCallsSnapshot() < 1 {
		t.Fatalf("expected at least 1 ticker-driven Confirm call, got %d", src.confirmCallsSnapshot())
	}
	confirmed := src.confirmedSnapshot()
	if len(confirmed) < 1 {
		t.Errorf("expected at least one id confirmed by ticker, got %v", confirmed)
	}
}

func TestRelay_FlushesPendingOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	override := 1
	src := &fakeSource{
		messages:          []Message{{ID: 7}},
		remainingOverride: &override,
	}

	handler := func(_ context.Context, _ Message) error {
		cancel()
		return nil
	}

	_ = startRelayWithFakeSource(ctx, src, handler, Config{
		RetryDelay:        time.Millisecond,
		KeepaliveInterval: time.Hour,
	})

	confirmed := src.confirmedSnapshot()
	if len(confirmed) != 1 || confirmed[0] != 7 {
		t.Errorf("expected id 7 flushed on context cancel, got %v", confirmed)
	}
}
