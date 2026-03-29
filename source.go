package outbox

import "context"

type source interface {
	Next(ctx context.Context) (Message, error)
	Buffered() int
	Confirm(ctx context.Context, ids ...int64) error
	Close(ctx context.Context)
}
