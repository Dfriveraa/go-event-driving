package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/signal"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type TicketsConfirmationRequest struct {
	Tickets []string `json:"tickets"`
}

type price struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}
type TicketInfoEvent struct {
	TicketID      string `json:"ticket_id"`
	CustomerEmail string `json:"customer_email"`
	Price         price  `json:"price"`
}

type TicketStatus struct {
	TicketID      string `json:"ticket_id"`
	Status        string `json:"status"`
	CustomerEmail string `json:"customer_email"`
	Price         price  `json:"price"`
}

type TicketStatusRequest struct {
	Tickets []TicketStatus `json:"tickets"`
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
	logger := watermill.NewStdLogger(false, false)

	router, err := message.NewRouter(message.RouterConfig{}, logger)
	if err != nil {
		panic(err)
	}

	router.AddNoPublisherHandler(
		"issue-handler",
		"issue-receipt",
		issueReceiptSub,
		func(msg *message.Message) error {
			var payload TicketInfoEvent
			err := json.Unmarshal(msg.Payload, &payload)
			if err != nil {
				return err
			}
			err = receiptsClient.IssueReceipt(msg.Context(),
				IssueReceiptRequest{TicketID: payload.TicketID,
					Price: Money{Amount: payload.Price.Amount,
						Currency: payload.Price.Currency,
					}})
			if err != nil {
				return err
			}
			return err
		},
	)
	router.AddNoPublisherHandler(
		"tracker-handler",
		"append-to-tracker",
		appendToTrackerSub,
		func(msg *message.Message) error {
			var payload TicketInfoEvent
			err := json.Unmarshal(msg.Payload, &payload)
			if err != nil {
				return err
			}
			err = spreadsheetsClient.AppendRow(msg.Context(), "tickets-to-print", []string{payload.TicketID, payload.CustomerEmail, payload.Price.Amount, payload.Price.Currency})
			return err
		},
	)
	e := commonHTTP.NewEcho()

	e.POST("/tickets-confirmation", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			msg := message.NewMessage(watermill.NewUUID(), []byte(ticket))
			err = pub.Publish("issue-receipt", msg)
			if err != nil {
				return err
			}

			err = pub.Publish("append-to-tracker", msg)
			if err != nil {
				return err
			}
		}
		return c.NoContent(http.StatusOK)
	})

	e.POST("/tickets-status", func(c echo.Context) error {
		var request TicketStatusRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			ticketInfo := TicketInfoEvent{
				TicketID:      ticket.TicketID,
				CustomerEmail: ticket.CustomerEmail,
				Price: price{
					Amount:   ticket.Price.Amount,
					Currency: ticket.Price.Currency,
				},
			}
			jsonData, err := json.Marshal(ticketInfo)
			if err != nil {
				return err
			}

			msg := message.NewMessage(watermill.NewUUID(), []byte(jsonData))
			err = pub.Publish("issue-receipt", msg)
			if err != nil {
				return err
			}
			msg = message.NewMessage(watermill.NewUUID(), []byte(jsonData))
			err = pub.Publish("append-to-tracker", msg)
			if err != nil {
				return err
			}
		}
		return c.NoContent(http.StatusOK)
	})

	e.GET("/health", func(c echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	logrus.Info("Server starting...")
	ctx := context.Background()
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return router.Run(ctx)
	})

	g.Go(func() error {
		<-router.Running()

		err := e.Start(":8080")
		if err != nil && err != http.ErrServerClosed {
			return err
		}

		return nil
	})

	g.Go(func() error {
		// Shut down the HTTP server
		<-ctx.Done()
		return e.Shutdown(ctx)
	})

	// Will block until all goroutines finish
	err = g.Wait()
	if err != nil {
		panic(err)
	}
}
