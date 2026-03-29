package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	outbox "github.com/pivovarit/outboxd"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	name := envOrDefault("RELAY_NAME", "relay")
	slotName := envOrDefault("SLOT_NAME", "outbox_relay")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

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

	relay := outbox.New(os.Getenv("DATABASE_URL"),
		func(ctx context.Context, msg outbox.Message) error {
			fmt.Printf("[%s] delivering message id=%d topic=%s payload=%s\n", name, msg.ID, msg.Topic, msg.Payload)
			return ch.PublishWithContext(ctx, "outbox", msg.Topic, false, false, amqp.Publishing{
				ContentType: "application/json",
				Body:        msg.Payload,
				MessageId:   fmt.Sprintf("%d", msg.ID),
				Timestamp:   msg.CreatedAt,
			})
		}, outbox.Config{
			SlotName:     slotName,
			Publications: []string{"outbox_pub"},
			RetryDelay:   time.Second,
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
