// Package otel provides outboxd Middleware for OpenTelemetry instrumentation.
//
// Tracing emits a span for every delivery attempt. Metrics records a delivery
// counter and a latency histogram per attempt. Both follow the OpenTelemetry
// semantic conventions for messaging (messaging.*), with messaging.system
// defaulting to "outboxd".
//
// Because outboxd applies retry outside the middleware chain, each middleware
// produces one observation per attempt. Place these middlewares after Recover()
// so panics surfaced as errors are recorded.
package otel
