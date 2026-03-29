package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	conn, err := amqp.Dial(os.Getenv("RABBITMQ_URL"))
	if err != nil {
		log.Fatalf("rabbitmq connect failed: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel failed: %v", err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare("outbox", "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("exchange declare failed: %v", err)
	}
	q, err := ch.QueueDeclare("order-events", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("queue declare failed: %v", err)
	}
	if err := ch.QueueBind(q.Name, "orders", "outbox", false, nil); err != nil {
		log.Fatalf("queue bind failed: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume failed: %v", err)
	}

	fmt.Println("consumer started, waiting for messages on queue 'order-events'")

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				return
			}
			fmt.Printf("[%s] %s\n", msg.RoutingKey, msg.Body)
		}
	}
}
