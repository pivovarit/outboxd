package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/pivovarit/outboxd"
	"github.com/pivovarit/outboxd/middleware"
	outboxotel "github.com/pivovarit/outboxd/middleware/otel"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	name := envOrDefault("RELAY_NAME", "relay")
	slotName := envOrDefault("SLOT_NAME", "outbox_relay")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	shutdown := initOTel()
	defer shutdown(context.Background())

	rmqConn, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		log.Fatalf("[%s] rabbitmq connect failed: %v", name, err)
	}
	defer rmqConn.Close()

	ch, err := rmqConn.Channel()
	if err != nil {
		log.Fatalf("[%s] rabbitmq channel failed: %v", name, err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("outbox", "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("[%s] exchange declare failed: %v", name, err)
	}

	relay := outboxd.New(os.Getenv("DATABASE_URL"),
		func(ctx context.Context, msg outboxd.Message) error {
			fmt.Printf("[%s] delivering message id=%d topic=%s payload=%s\n", name, msg.ID, msg.Topic, msg.Payload)
			return ch.PublishWithContext(ctx, "outbox", msg.Topic, false, false, amqp.Publishing{
				ContentType: "application/json",
				Body:        msg.Payload,
				MessageId:   fmt.Sprintf("%d", msg.ID),
				Timestamp:   msg.CreatedAt,
			})
		}, outboxd.Config{
			SlotName:     slotName,
			Publications: []string{"outbox_pub"},
			RetryDelay:   time.Second,
			Schema: outboxd.SchemaConfig{
				Table:           "outbox",
				IDColumn:        "id",
				TopicColumn:     "topic",
				PayloadColumn:   "payload",
				CreatedAtColumn: "created_at",
			},
			Middlewares: []outboxd.Middleware{
				middleware.Recover(),
				outboxotel.Tracing(),
				outboxotel.Metrics(),
			},
		})

	fmt.Printf("[%s] relay started (slot=%s), waiting for outbox messages\n", name, slotName)

	var logged bool
	for {
		err := relay.Start(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		if !logged {
			fmt.Printf("[%s] relay failed, retrying: %v\n", name, err)
			logged = true
		}
		time.Sleep(5 * time.Second)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func initOTel() func(context.Context) {
	traceExp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatalf("stdout trace exporter: %v", err)
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExp))
	otel.SetTracerProvider(tp)

	metricExp, err := stdoutmetric.New()
	if err != nil {
		log.Fatalf("stdout metric exporter: %v", err)
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)))
	otel.SetMeterProvider(mp)

	return func(ctx context.Context) {
		tp.Shutdown(ctx)
		mp.Shutdown(ctx)
	}
}
