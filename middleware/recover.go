package middleware

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/pivovarit/outboxd"
)

// Recover catches any panic raised by a downstream handler or middleware and
// converts it to an error, so that the relay's retry/drop path handles it
// normally instead of crashing the delivery goroutine. The returned error
// wraps both the panic value and a stack trace.
//
// Recover should usually be the first entry in outboxd.Config.Middlewares
// so that it catches panics from every other middleware in the chain, not
// just from the user handler.
func Recover() outboxd.Middleware {
	return func(next outboxd.Handler) outboxd.Handler {
		return func(ctx context.Context, msg outboxd.Message) (err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("outboxd: handler panic: %v\n%s", r, debug.Stack())
				}
			}()
			return next(ctx, msg)
		}
	}
}
