// Package otel provides an outboxd Middleware that emits an OpenTelemetry
// span for every delivery attempt. Attributes follow the OpenTelemetry
// semantic conventions for messaging (messaging.*), with messaging.system
// defaulting to "outboxd".
//
// Because outboxd applies retry outside the middleware chain, the middleware
// produces one span per attempt. Place this middleware after Recover() so
// panics surfaced as errors are recorded on the span.
package otel
