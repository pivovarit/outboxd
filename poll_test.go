package outboxd

import "testing"

func TestPollSource_ArmBatchDrainsStaleConfirmSignal(t *testing.T) {
	p := &pollSource{confirmCh: make(chan struct{}, 1)}
	p.confirmCh <- struct{}{} // stale signal from a prior batch

	first := p.armBatch([]Message{{ID: 1}, {ID: 2}, {ID: 3}})

	if first.ID != 1 {
		t.Fatalf("first message: got %d, want 1", first.ID)
	}
	if got := p.batchInFlight.Load(); got != 3 {
		t.Fatalf("batchInFlight: got %d, want 3", got)
	}
	if len(p.buffered) != 2 {
		t.Fatalf("buffered: got %d, want 2", len(p.buffered))
	}
	if len(p.confirmCh) != 0 {
		t.Fatalf("confirmCh should be drained before arming a new batch; still has %d", len(p.confirmCh))
	}
}

func TestPollSource_ArmBatchLeavesConfirmChEmptyWhenNoStaleSignal(t *testing.T) {
	p := &pollSource{confirmCh: make(chan struct{}, 1)}

	p.armBatch([]Message{{ID: 1}})

	if len(p.confirmCh) != 0 {
		t.Fatalf("confirmCh should remain empty; has %d", len(p.confirmCh))
	}
}
