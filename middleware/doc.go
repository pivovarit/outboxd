// Package middleware provides reusable outboxd Middleware values that wrap
// the user's Handler with cross-cutting concerns such as panic recovery.
//
// Middlewares are registered via outboxd.Config.Middlewares. The first entry
// in that slice is outermost and is entered first. Retry is applied outside
// the middleware chain by the relay itself, so each retry attempt flows
// through every middleware.
package middleware
