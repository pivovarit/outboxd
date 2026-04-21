package outboxd

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

type inFlightBatch struct {
	lsn     pglogrepl.LSN
	pending map[int64]struct{}
}

type inFlightTracker struct {
	batches      []*inFlightBatch
	index        map[int64]*inFlightBatch
	confirmedLSN pglogrepl.LSN
}

func newInFlightTracker() *inFlightTracker {
	return &inFlightTracker{index: make(map[int64]*inFlightBatch)}
}

func (t *inFlightTracker) Register(lsn pglogrepl.LSN, ids []int64) {
	b := &inFlightBatch{
		lsn:     lsn,
		pending: make(map[int64]struct{}, len(ids)),
	}
	for _, id := range ids {
		b.pending[id] = struct{}{}
		t.index[id] = b
	}
	t.batches = append(t.batches, b)
}

func (t *inFlightTracker) ConfirmedLSN() pglogrepl.LSN { return t.confirmedLSN }

func (t *inFlightTracker) tracks(id int64) bool {
	_, ok := t.index[id]
	return ok
}

func (t *inFlightTracker) Validate(ids []int64) error {
	seen := make(map[int64]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := t.index[id]; !ok {
			return fmt.Errorf("outboxd: confirm unknown id %d", id)
		}
		if _, dup := seen[id]; dup {
			return fmt.Errorf("outboxd: confirm duplicate id %d", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func (t *inFlightTracker) Apply(ids []int64) (pglogrepl.LSN, bool) {
	for _, id := range ids {
		b, ok := t.index[id]
		if !ok {
			panic(fmt.Sprintf("outboxd: apply unknown id %d", id))
		}
		delete(b.pending, id)
		delete(t.index, id)
	}
	advanced := false
	for len(t.batches) > 0 && len(t.batches[0].pending) == 0 {
		t.confirmedLSN = t.batches[0].lsn
		t.batches[0] = nil
		t.batches = t.batches[1:]
		advanced = true
	}
	return t.confirmedLSN, advanced
}
