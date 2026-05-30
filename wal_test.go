package outboxd

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
)

func TestWAL_RegisterPrecedesDeliveryBlocksIdleAdvance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := &walListener{
		batchCh:         make(chan walBatch),
		tracker:         newInFlightTracker(),
		readCtx:         ctx,
		standbyInterval: time.Hour,
	}
	nextStandby := time.Now().Add(time.Hour)

	batch := walBatch{messages: []Message{{ID: 1}}, lsn: pglogrepl.LSN(200)}

	done := make(chan struct{})
	go func() {
		if ok, err := w.registerAndDeliver(batch, &nextStandby); err != nil || !ok {
			close(done)
			return
		}
		w.mu.Lock()
		w.tracker.AdvanceIdle(pglogrepl.LSN(210))
		w.mu.Unlock()
		close(done)
	}()

	got := <-w.batchCh
	if got.lsn != pglogrepl.LSN(200) {
		t.Fatalf("received batch lsn=%v, want 200", got.lsn)
	}

	<-done

	w.mu.Lock()
	lsn := w.tracker.ConfirmedLSN()
	w.mu.Unlock()
	if lsn != 0 {
		t.Fatalf("confirmedLSN advanced to %v past in-flight undelivered batch at LSN 200 (silent message loss)", lsn)
	}
}
