package outboxd

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestTracker_RegisterIndexesIDs(t *testing.T) {
	tr := newInFlightTracker()
	tr.Register(pglogrepl.LSN(100), []int64{1, 2, 3})

	for _, id := range []int64{1, 2, 3} {
		if !tr.tracks(id) {
			t.Errorf("expected id %d to be tracked", id)
		}
	}
	if tr.tracks(99) {
		t.Error("did not expect id 99 to be tracked")
	}
	if got := tr.ConfirmedLSN(); got != 0 {
		t.Errorf("expected ConfirmedLSN=0 with nothing confirmed, got %v", got)
	}
}

func TestTracker_ValidateAllTracked(t *testing.T) {
	tr := newInFlightTracker()
	tr.Register(pglogrepl.LSN(100), []int64{1, 2, 3})
	if err := tr.Validate([]int64{1, 3}); err != nil {
		t.Errorf("expected nil for tracked ids, got %v", err)
	}
}

func TestTracker_ValidateUnknownID(t *testing.T) {
	tr := newInFlightTracker()
	tr.Register(pglogrepl.LSN(100), []int64{1, 2, 3})
	err := tr.Validate([]int64{1, 999})
	if err == nil {
		t.Fatal("expected error for unknown id 999, got nil")
	}
}

func TestTracker_ValidateUnknownIDOnEmptyTracker(t *testing.T) {
	tr := newInFlightTracker()
	if err := tr.Validate([]int64{5}); err == nil {
		t.Fatal("expected error for unknown id on empty tracker, got nil")
	}
}

func TestTracker_ValidateRejectsDuplicateIDs(t *testing.T) {
	tr := newInFlightTracker()
	tr.Register(pglogrepl.LSN(100), []int64{1, 2, 3})
	if err := tr.Validate([]int64{1, 1}); err == nil {
		t.Fatal("expected error for duplicate id in confirm batch, got nil")
	}
}

func TestTracker_ApplyDrainsBatchAdvancesLSN(t *testing.T) {
	tr := newInFlightTracker()
	tr.Register(pglogrepl.LSN(100), []int64{1, 2, 3})

	lsn, advanced := tr.Apply([]int64{1, 2, 3})

	if !advanced {
		t.Fatal("expected advanced=true when head batch fully drained")
	}
	if lsn != pglogrepl.LSN(100) {
		t.Errorf("expected LSN=100, got %v", lsn)
	}
	if tr.ConfirmedLSN() != pglogrepl.LSN(100) {
		t.Errorf("expected ConfirmedLSN=100, got %v", tr.ConfirmedLSN())
	}
	if tr.tracks(1) || tr.tracks(2) || tr.tracks(3) {
		t.Error("expected drained ids to be removed from index")
	}
}

func TestTracker_ApplyPartialHeadNoAdvance(t *testing.T) {
	tr := newInFlightTracker()
	tr.Register(pglogrepl.LSN(100), []int64{1, 2, 3})

	lsn, advanced := tr.Apply([]int64{1, 3})

	if advanced {
		t.Error("expected advanced=false with head batch still pending id 2")
	}
	if lsn != 0 {
		t.Errorf("expected returned LSN=0, got %v", lsn)
	}
	if tr.ConfirmedLSN() != 0 {
		t.Errorf("expected ConfirmedLSN=0, got %v", tr.ConfirmedLSN())
	}

	lsn, advanced = tr.Apply([]int64{2})
	if !advanced || lsn != pglogrepl.LSN(100) {
		t.Errorf("expected advance to 100 after draining id 2, got lsn=%v advanced=%v", lsn, advanced)
	}
}

func TestTracker_ApplyLaterBatchDoesNotAdvancePastUndelivered(t *testing.T) {
	tr := newInFlightTracker()
	tr.Register(pglogrepl.LSN(100), []int64{1, 2, 3})
	tr.Register(pglogrepl.LSN(200), []int64{4, 5, 6})

	lsn, advanced := tr.Apply([]int64{4, 5, 6})

	if advanced {
		t.Error("expected advanced=false while batch A still has pending ids")
	}
	if lsn != 0 {
		t.Errorf("expected returned LSN=0, got %v", lsn)
	}
	if tr.ConfirmedLSN() != 0 {
		t.Errorf("expected ConfirmedLSN=0, got %v (BUG: advanced past undelivered)", tr.ConfirmedLSN())
	}

	lsn, advanced = tr.Apply([]int64{1, 2, 3})
	if !advanced || lsn != pglogrepl.LSN(200) {
		t.Errorf("expected advance to 200 after draining A, got lsn=%v advanced=%v", lsn, advanced)
	}
}

func TestTracker_ApplyCrossBatch(t *testing.T) {
	tr := newInFlightTracker()
	tr.Register(pglogrepl.LSN(100), []int64{1, 2, 3})
	tr.Register(pglogrepl.LSN(200), []int64{4, 5, 6})

	lsn, advanced := tr.Apply([]int64{2, 3, 4})
	if advanced {
		t.Error("expected advanced=false: A still has id 1 pending")
	}
	if tr.ConfirmedLSN() != 0 {
		t.Errorf("expected ConfirmedLSN=0, got %v", tr.ConfirmedLSN())
	}

	lsn, advanced = tr.Apply([]int64{1})
	if !advanced || lsn != pglogrepl.LSN(100) {
		t.Errorf("expected advance to 100, got lsn=%v advanced=%v", lsn, advanced)
	}

	lsn, advanced = tr.Apply([]int64{5, 6})
	if !advanced || lsn != pglogrepl.LSN(200) {
		t.Errorf("expected advance to 200, got lsn=%v advanced=%v", lsn, advanced)
	}
}
