package main

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
)

type AlarmClient interface {
	StartAlarm() error
	StopAlarm() error
}

func ConsumeMessages(sub message.Subscriber, alarmClient AlarmClient) {
	messages, err := sub.Subscribe(context.Background(), "smoke_sensor")
	if err != nil {
		panic(err)
	}

	for msg := range messages {
		action := string(msg.Payload)
		var err error
		switch action {
		case "1":
			err = alarmClient.StartAlarm()
		case "0":
			err = alarmClient.StopAlarm()
		}
		if err != nil {
			fmt.Println("Error while processing message:", err)
			msg.Nack()
		} else {
			fmt.Println("Received message", msg.Payload)
			msg.Ack()
		}

	}
}
