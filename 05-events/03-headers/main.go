package main

import (
	"encoding/json"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
)

type ProductOutOfStock struct {
	ProductID string `json:"product_id"`
	Header    header `json:"header"`
}

type ProductBackInStock struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Header    header `json:"header"`
}
type header struct {
	ID            string `json:"id"`
	EventName     string `json:"event_name"`
	CorrelationID string `json:"correlation_id"`
	OccurredAt    string `json:"occurred_at"`
}

func NewHeader(eventName string) header {
	return header{
		ID:         uuid.NewString(),
		EventName:  eventName,
		OccurredAt: time.Now().Format(time.RFC3339),
	}
}

type Publisher struct {
	pub message.Publisher
}

func NewPublisher(pub message.Publisher) Publisher {
	return Publisher{
		pub: pub,
	}
}

func (p Publisher) PublishProductOutOfStock(productID string) error {
	event := ProductOutOfStock{
		ProductID: productID,
		Header:    NewHeader("ProductOutOfStock"),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)

	return p.pub.Publish("product-updates", msg)
}

func (p Publisher) PublishProductBackInStock(productID string, quantity int) error {
	event := ProductBackInStock{
		ProductID: productID,
		Quantity:  quantity,
		Header:    NewHeader("ProductBackInStock"),
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := message.NewMessage(watermill.NewUUID(), payload)

	return p.pub.Publish("product-updates", msg)
}
