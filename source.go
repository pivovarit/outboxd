package outboxd

import "context"

type source interface {
	// Next returns the next message and the number of messages still sitting
	// in the source's internal buffer after this one has been popped. A
	// remaining count of 0 means the caller has drained the current in-memory
	// batch and a subsequent Next call will have to fetch or block.
	Next(ctx context.Context) (Message, int, error)
	Confirm(ctx context.Context, ids ...int64) error
	Close(ctx context.Context)
}
