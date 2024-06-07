package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/redis/go-redis/v9"
)

func main() {
	logger := watermill.NewStdLogger(false, false)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	sub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}

	router.AddNoPublisherHandler(
		"handler_name",
		"temperature-fahrenheit",
		sub,
		func(msg *message.Message) error {
			temperature := string(msg.Payload)
			// fahrenheit, err := celsiusToFahrenheit(temperature)
			if err != nil {
				return err
			}
			fmt.Printf("Temperature read: %v\n", temperature)
			return nil
		},
	)

	err = router.Run(context.Background())
	if err != nil {
		panic(err)
	}

}

func celsiusToFahrenheit(temperature string) (string, error) {
	celsius, err := strconv.Atoi(temperature)
	if err != nil {
		return "", err
	}

	return strconv.Itoa(celsius*9/5 + 32), nil
}
