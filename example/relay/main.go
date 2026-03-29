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
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	rmqConn, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		log.Fatalf("rabbitmq connect failed: %v", err)
	}
	defer rmqConn.Close()

	ch, err := rmqConn.Channel()
	if err != nil {
		log.Fatalf("rabbitmq channel failed: %v", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("outbox", "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("exchange declare failed: %v", err)
	}

	relay := outboxd.New(os.Getenv("DATABASE_URL"),
		func(ctx context.Context, msg outboxd.Message) error {
			return ch.PublishWithContext(ctx, "outbox", msg.Topic, false, false, amqp.Publishing{
				ContentType: "application/json",
				Body:        msg.Payload,
				MessageId:   fmt.Sprintf("%d", msg.ID),
				Timestamp:   msg.CreatedAt,
			})
		}, outboxd.Config{
			SlotName:     "outbox_relay",
			Publications: []string{"outbox_pub"},
			RetryDelay:   time.Second,
			Schema: outboxd.SchemaConfig{
				Table:           "outbox",
				IDColumn:        "id",
				TopicColumn:     "topic",
				PayloadColumn:   "payload",
				CreatedAtColumn: "created_at",
			},
		})

	fmt.Println("relay started, waiting for outbox messages")

	for {
		err := relay.Start(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		fmt.Printf("relay failed, restarting: %v\n", err)
		time.Sleep(5 * time.Second)
	}
}
