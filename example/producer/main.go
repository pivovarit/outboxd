package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5"
)

type OrderEvent struct {
	OrderID string `json:"order_id"`
	Item    string `json:"item"`
	Amount  int    `json:"amount"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("connect failed: %v", err)
	}
	defer conn.Close(ctx)

	fmt.Println("producer started")

	for i := 1; ; i++ {
		event := OrderEvent{
			OrderID: fmt.Sprintf("ORD-%04d", i),
			Item:    fmt.Sprintf("Widget #%d", i),
			Amount:  i * 10,
		}
		payload, _ := json.Marshal(event)

		_, err := conn.Exec(ctx,
			"INSERT INTO outbox (topic, payload) VALUES ($1, $2)",
			"orders", payload,
		)
		if err != nil {
			log.Fatalf("insert failed: %v", err)
		}
		fmt.Printf("inserted outbox message: %s\n", event.OrderID)

		delay := time.Duration(200+rand.IntN(4800)) * time.Millisecond
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}
