package otel

import (
	"context"
	"errors"
	"testing"

	"github.com/pivovarit/outboxd"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func newTestProvider(t *testing.T) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return tp, exp
}

func attrMap(attrs []attribute.KeyValue) map[string]attribute.Value {
	m := make(map[string]attribute.Value, len(attrs))
	for _, a := range attrs {
		m[string(a.Key)] = a.Value
	}
	return m
}

func TestTracing_SuccessfulDeliveryEmitsSpan(t *testing.T) {
	tp, exp := newTestProvider(t)

	mw := Tracing(WithTracerProvider(tp))
	wrapped := mw(func(_ context.Context, _ outboxd.Message) error { return nil })

	msg := outboxd.Message{ID: 42, Topic: "orders", Payload: []byte("hello")}
	if err := wrapped(context.Background(), msg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	s := spans[0]

	if s.Name != "orders publish" {
		t.Errorf("span name: got %q, want %q", s.Name, "orders publish")
	}
	if s.SpanKind != trace.SpanKindProducer {
		t.Errorf("span kind: got %v, want Producer", s.SpanKind)
	}
	if s.Status.Code != codes.Unset {
		t.Errorf("status code: got %v, want Unset", s.Status.Code)
	}

	attrs := attrMap(s.Attributes)
	checks := map[string]string{
		"messaging.system":           "outboxd",
		"messaging.operation.name":   "publish",
		"messaging.destination.name": "orders",
		"messaging.message.id":       "42",
	}
	for k, want := range checks {
		got, ok := attrs[k]
		if !ok {
			t.Errorf("missing attribute %s", k)
			continue
		}
		if got.AsString() != want {
			t.Errorf("attr %s: got %q, want %q", k, got.AsString(), want)
		}
	}
	if size, ok := attrs["messaging.message.body.size"]; !ok {
		t.Error("missing attribute messaging.message.body.size")
	} else if size.AsInt64() != int64(len(msg.Payload)) {
		t.Errorf("body size: got %d, want %d", size.AsInt64(), len(msg.Payload))
	}
}

func TestTracing_HandlerErrorRecordsErrorAndSetsStatus(t *testing.T) {
	tp, exp := newTestProvider(t)

	sentinel := errors.New("handler failed")
	mw := Tracing(WithTracerProvider(tp))
	wrapped := mw(func(_ context.Context, _ outboxd.Message) error { return sentinel })

	err := wrapped(context.Background(), outboxd.Message{ID: 1, Topic: "t"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	s := spans[0]

	if s.Status.Code != codes.Error {
		t.Errorf("status code: got %v, want Error", s.Status.Code)
	}
	if s.Status.Description != sentinel.Error() {
		t.Errorf("status description: got %q, want %q", s.Status.Description, sentinel.Error())
	}
	if len(s.Events) == 0 {
		t.Error("expected at least one event (recorded error), got none")
	}
}

func TestTracing_CustomMessagingSystem(t *testing.T) {
	tp, exp := newTestProvider(t)

	mw := Tracing(WithTracerProvider(tp), WithMessagingSystem("kafka"))
	wrapped := mw(func(_ context.Context, _ outboxd.Message) error { return nil })

	if err := wrapped(context.Background(), outboxd.Message{ID: 1, Topic: "t"}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	attrs := attrMap(spans[0].Attributes)
	if got := attrs["messaging.system"].AsString(); got != "kafka" {
		t.Errorf("messaging.system: got %q, want %q", got, "kafka")
	}
}

func TestTracing_OneSpanPerCall(t *testing.T) {
	tp, exp := newTestProvider(t)

	mw := Tracing(WithTracerProvider(tp))
	wrapped := mw(func(_ context.Context, _ outboxd.Message) error { return nil })

	const n = 3
	for i := 0; i < n; i++ {
		if err := wrapped(context.Background(), outboxd.Message{ID: int64(i), Topic: "t"}); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if got := len(exp.GetSpans()); got != n {
		t.Errorf("expected %d spans, got %d", n, got)
	}
}

func newTestMeterProvider(t *testing.T) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return mp, reader
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

func TestMetrics_SuccessfulDelivery(t *testing.T) {
	mp, reader := newTestMeterProvider(t)

	mw := Metrics(WithMeterProvider(mp))
	wrapped := mw(func(_ context.Context, _ outboxd.Message) error { return nil })

	msg := outboxd.Message{ID: 42, Topic: "orders", Payload: []byte("hello")}
	if err := wrapped(context.Background(), msg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	rm := collectMetrics(t, reader)

	counter := findMetric(rm, "messaging.publish.messages")
	if counter == nil {
		t.Fatal("missing messaging.publish.messages metric")
	}
	sum, ok := counter.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("expected Sum[int64], got %T", counter.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("expected 1 data point, got %d", len(sum.DataPoints))
	}
	dp := sum.DataPoints[0]
	if dp.Value != 1 {
		t.Errorf("counter value: got %d, want 1", dp.Value)
	}
	attrs := attrMap(dp.Attributes.ToSlice())
	if got := attrs["messaging.publish.status"].AsString(); got != "delivered" {
		t.Errorf("status: got %q, want %q", got, "delivered")
	}

	histogram := findMetric(rm, "messaging.publish.duration")
	if histogram == nil {
		t.Fatal("missing messaging.publish.duration metric")
	}
	hist, ok := histogram.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("expected Histogram[float64], got %T", histogram.Data)
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("expected 1 histogram data point, got %d", len(hist.DataPoints))
	}
	if hist.DataPoints[0].Count != 1 {
		t.Errorf("histogram count: got %d, want 1", hist.DataPoints[0].Count)
	}
}

func TestMetrics_HandlerError(t *testing.T) {
	mp, reader := newTestMeterProvider(t)

	sentinel := errors.New("handler failed")
	mw := Metrics(WithMeterProvider(mp))
	wrapped := mw(func(_ context.Context, _ outboxd.Message) error { return sentinel })

	err := wrapped(context.Background(), outboxd.Message{ID: 1, Topic: "t"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}

	rm := collectMetrics(t, reader)

	counter := findMetric(rm, "messaging.publish.messages")
	if counter == nil {
		t.Fatal("missing messaging.publish.messages metric")
	}
	sum := counter.Data.(metricdata.Sum[int64])
	if len(sum.DataPoints) != 1 {
		t.Fatalf("expected 1 data point, got %d", len(sum.DataPoints))
	}
	attrs := attrMap(sum.DataPoints[0].Attributes.ToSlice())
	if got := attrs["messaging.publish.status"].AsString(); got != "failed" {
		t.Errorf("status: got %q, want %q", got, "failed")
	}

	histogram := findMetric(rm, "messaging.publish.duration")
	if histogram == nil {
		t.Fatal("missing messaging.publish.duration metric")
	}
	hist := histogram.Data.(metricdata.Histogram[float64])
	if len(hist.DataPoints) != 1 {
		t.Fatalf("expected 1 histogram data point, got %d", len(hist.DataPoints))
	}
	if hist.DataPoints[0].Count != 1 {
		t.Errorf("histogram count: got %d, want 1", hist.DataPoints[0].Count)
	}
}

func TestMetrics_AttributeValues(t *testing.T) {
	mp, reader := newTestMeterProvider(t)

	mw := Metrics(WithMeterProvider(mp), WithMessagingSystem("kafka"))
	wrapped := mw(func(_ context.Context, _ outboxd.Message) error { return nil })

	msg := outboxd.Message{ID: 7, Topic: "events"}
	if err := wrapped(context.Background(), msg); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	rm := collectMetrics(t, reader)

	counter := findMetric(rm, "messaging.publish.messages")
	if counter == nil {
		t.Fatal("missing messaging.publish.messages metric")
	}
	sum := counter.Data.(metricdata.Sum[int64])
	dp := sum.DataPoints[0]
	attrs := attrMap(dp.Attributes.ToSlice())

	checks := map[string]string{
		"messaging.system":           "kafka",
		"messaging.destination.name": "events",
		"messaging.publish.status":   "delivered",
	}
	for k, want := range checks {
		got, ok := attrs[k]
		if !ok {
			t.Errorf("missing attribute %s", k)
			continue
		}
		if got.AsString() != want {
			t.Errorf("attr %s: got %q, want %q", k, got.AsString(), want)
		}
	}
}
