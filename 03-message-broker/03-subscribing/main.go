package main

import (
	"context"
	"fmt"
	"os"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/redis/go-redis/v9"
)

func main() {

	logger := watermill.NewStdLogger(false, false)
	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})
	subscriber, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()
	messages, err := subscriber.Subscribe(ctx, "progress")
	if err != nil {
		panic(err)
	}
	for msg := range messages {
		orderID := string(msg.Payload)
		fmt.Printf("Message ID: %s - %s%%!", msg.UUID, orderID)
		msg.Ack()
	}
}
