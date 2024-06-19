package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type TicketsStatusRequest struct {
	Tickets []TicketStatus `json:"tickets"`
}

type TicketStatus struct {
	TicketID      string `json:"ticket_id"`
	Status        string `json:"status"`
	Price         Money  `json:"price"`
	CustomerEmail string `json:"customer_email"`
}

type EventHeader struct {
	ID          string    `json:"id"`
	PublishedAt time.Time `json:"published_at"`
}

type TicketBookingConfirmed struct {
	Header        EventHeader `json:"header"`
	TicketID      string      `json:"ticket_id"`
	CustomerEmail string      `json:"customer_email"`
	Price         Money       `json:"price"`
}

func main() {
	log.Init(logrus.InfoLevel)

	clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	receiptsClient := NewReceiptsClient(clients)
	spreadsheetsClient := NewSpreadsheetsClient(clients)

	watermillLogger := log.NewWatermill(logrus.NewEntry(logrus.StandardLogger()))

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	pub, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	issueReceiptSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "issue-receipt",
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	appendToTrackerSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
		Client:        rdb,
		ConsumerGroup: "append-to-tracker",
	}, watermillLogger)
	if err != nil {
		panic(err)
	}

	e := commonHTTP.NewEcho()

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	e.POST("/tickets-status", func(c echo.Context) error {
		var request TicketsStatusRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			if ticket.Status != "confirmed" {
				continue
			}

			event := TicketBookingConfirmed{
				Header:        NewHeader(),
				TicketID:      ticket.TicketID,
				CustomerEmail: ticket.CustomerEmail,
				Price:         ticket.Price,
			}

			payload, err := json.Marshal(event)
			if err != nil {
				return err
			}

			msg := message.NewMessage(watermill.NewUUID(), payload)
			err = pub.Publish("TicketBookingConfirmed", msg)
			if err != nil {
				return err
			}
		}

		return c.NoContent(http.StatusOK)
	})

	router, err := message.NewRouter(message.RouterConfig{}, watermillLogger)
	if err != nil {
		panic(err)
	}

	router.AddNoPublisherHandler(
		"issue_receipt",
		"TicketBookingConfirmed",
		issueReceiptSub,
		func(msg *message.Message) error {
			var event TicketBookingConfirmed
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}

			return receiptsClient.IssueReceipt(msg.Context(), IssueReceiptRequest{
				TicketID: event.TicketID,
				Price: Money{
					Amount:   event.Price.Amount,
					Currency: event.Price.Currency,
				},
			})
		},
	)

	router.AddNoPublisherHandler(
		"print_ticket",
		"TicketBookingConfirmed",
		appendToTrackerSub,
		func(msg *message.Message) error {
			var event TicketBookingConfirmed
			err := json.Unmarshal(msg.Payload, &event)
			if err != nil {
				return err
			}

			return spreadsheetsClient.AppendRow(
				msg.Context(),
				"tickets-to-print",
				[]string{event.TicketID, event.CustomerEmail, event.Price.Amount, event.Price.Currency},
			)
		},
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	errgrp, ctx := errgroup.WithContext(ctx)

	errgrp.Go(func() error {
		return router.Run(ctx)
	})

	errgrp.Go(func() error {
		// we don't want to start HTTP server before Watermill router (so service won't be healthy before it's ready)
		<-router.Running()

		err := e.Start(":8080")

		if err != nil && err != http.ErrServerClosed {
			return err
		}

		return nil
	})

	errgrp.Go(func() error {
		<-ctx.Done()
		return e.Shutdown(context.Background())
	})

	if err := errgrp.Wait(); err != nil {
		panic(err)
	}
}

func NewHeader() EventHeader {
	return EventHeader{
		ID:          uuid.NewString(),
		PublishedAt: time.Now().UTC(),
	}
}
