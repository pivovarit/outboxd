package outbox

import "context"

type source interface {
	Next(ctx context.Context) (Message, error)
	Confirm(ctx context.Context, id int64) error
	Close(ctx context.Context)
}
