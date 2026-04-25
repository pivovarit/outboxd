package otel

import (
	"context"
	"strconv"
	"time"

	"github.com/pivovarit/outboxd"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "github.com/pivovarit/outboxd/middleware/otel"
	defaultSystem       = "outboxd"
	operation           = "publish"
)

type config struct {
	tp     trace.TracerProvider
	mp     metric.MeterProvider
	system string
}

type Option func(*config)

// WithTracerProvider overrides the TracerProvider. Defaults to otel.GetTracerProvider().
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(c *config) {
		if tp != nil {
			c.tp = tp
		}
	}
}

// WithMeterProvider overrides the MeterProvider. Defaults to otel.GetMeterProvider().
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) {
		if mp != nil {
			c.mp = mp
		}
	}
}

// WithMessagingSystem overrides the value reported as messaging.system. Defaults to "outboxd".
func WithMessagingSystem(s string) Option {
	return func(c *config) {
		if s != "" {
			c.system = s
		}
	}
}

// Tracing returns an outboxd.Middleware that starts a span per delivery attempt.
// Errors returned by the downstream handler are recorded on the span and set
// its status to Error; success leaves the status unset (Unset/Ok semantics).
func Tracing(opts ...Option) outboxd.Middleware {
	cfg := config{tp: otel.GetTracerProvider(), system: defaultSystem}
	for _, o := range opts {
		o(&cfg)
	}
	tracer := cfg.tp.Tracer(instrumentationName)

	return func(next outboxd.Handler) outboxd.Handler {
		return func(ctx context.Context, msg outboxd.Message) error {
			ctx, span := tracer.Start(ctx,
				msg.Topic+" "+operation,
				trace.WithSpanKind(trace.SpanKindProducer),
				trace.WithAttributes(
					attribute.String("messaging.system", cfg.system),
					attribute.String("messaging.operation.name", operation),
					attribute.String("messaging.destination.name", msg.Topic),
					attribute.String("messaging.message.id", strconv.FormatInt(msg.ID, 10)),
					attribute.Int("messaging.message.body.size", len(msg.Payload)),
				),
			)
			defer span.End()

			if err := next(ctx, msg); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return err
			}
			return nil
		}
	}
}

// Metrics returns an outboxd.Middleware that records delivery metrics per
// handler invocation: a counter for the number of publish attempts and a
// histogram for handler latency. Each instrument carries messaging.system,
// messaging.destination.name, and messaging.publish.status attributes.
func Metrics(opts ...Option) outboxd.Middleware {
	cfg := config{mp: otel.GetMeterProvider(), system: defaultSystem}
	for _, o := range opts {
		o(&cfg)
	}
	meter := cfg.mp.Meter(instrumentationName)

	messages, err := meter.Int64Counter("messaging.publish.messages",
		metric.WithUnit("{message}"),
		metric.WithDescription("Number of publish attempts"))
	if err != nil {
		otel.Handle(err)
	}

	duration, err := meter.Int64Histogram("messaging.publish.duration",
		metric.WithUnit("ms"),
		metric.WithDescription("Handler latency per publish attempt"))
	if err != nil {
		otel.Handle(err)
	}

	return func(next outboxd.Handler) outboxd.Handler {
		return func(ctx context.Context, msg outboxd.Message) error {
			start := time.Now()
			err := next(ctx, msg)

			status := "delivered"
			if err != nil {
				status = "failed"
			}

			attrs := metric.WithAttributes(
				attribute.String("messaging.system", cfg.system),
				attribute.String("messaging.destination.name", msg.Topic),
				attribute.String("messaging.publish.status", status),
			)
			messages.Add(ctx, 1, attrs)
			duration.Record(ctx, time.Since(start).Milliseconds(), attrs)

			return err
		}
	}
}
