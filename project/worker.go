package main

import (
	"context"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/sirupsen/logrus"
	"os"
)

type Task int

const (
	TaskIssueReceipt Task = iota
	TaskAppendToTracker
)

type Message struct {
	Task     Task
	TicketID string
}

type Worker struct {
	queue              chan Message
	receiptsClient     ReceiptsClient
	spreadsheetsClient SpreadsheetsClient
}

func NewWorker() *Worker {
	clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	return &Worker{
		queue:              make(chan Message, 100),
		receiptsClient:     NewReceiptsClient(clients),
		spreadsheetsClient: NewSpreadsheetsClient(clients),
	}
}

func (w *Worker) Send(msg ...Message) {
	for _, m := range msg {
		w.queue <- m
	}
}

func (w *Worker) Run() {
	for msg := range w.queue {
		c := context.Background()
		switch msg.Task {
		case TaskIssueReceipt:
			err := w.receiptsClient.IssueReceipt(c, msg.TicketID)
			if err != nil {
				logrus.WithError(err).Error("Failed to issue receipt, retrying...")
				w.Send(msg)
			}
		case TaskAppendToTracker:
			err := w.spreadsheetsClient.AppendRow(c, "tickets-to-print", []string{msg.TicketID})
			if err != nil {
				logrus.WithError(err).Error("Failed to SpreadsheetsClient.AppendRow, retrying...")
				w.Send(msg)
			}
		}
	}
}
