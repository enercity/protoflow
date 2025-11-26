package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/enercity/protoflow"
)

type IncomingOrder struct {
	OrderID   string    `json:"order_id"`
	Customer  string    `json:"customer"`
	CreatedAt time.Time `json:"created_at"`
}

type OutgoingOrder struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

func main() {
	ctx := context.Background()
	baseLogger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	logger := protoflow.NewSlogServiceLogger(baseLogger)

	cfg := &protoflow.Config{
		PubSubSystem: "channel",
		PoisonQueue:  "orders.poison",
	}

	svc := protoflow.NewService(cfg, logger, ctx, protoflow.ServiceDependencies{})

	err := protoflow.RegisterJSONHandler(svc, protoflow.JSONHandlerRegistration[*IncomingOrder, *OutgoingOrder]{
		Name:         "json-orders",
		ConsumeQueue: "orders.incoming.json",
		PublishQueue: "orders.processed.json",
		Handler: func(ctx context.Context, evt protoflow.JSONMessageContext[*IncomingOrder]) ([]protoflow.JSONMessageOutput[*OutgoingOrder], error) {
			resp := &OutgoingOrder{OrderID: evt.Payload.OrderID, Status: "processed"}
			metadata := evt.Metadata.With("processed_at", time.Now().UTC().Format(time.RFC3339))
			return []protoflow.JSONMessageOutput[*OutgoingOrder]{
				{Message: resp, Metadata: metadata},
			}, nil
		},
	})
	if err != nil {
		panic(err)
	}

	if err := svc.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("router stopped", err, protoflow.LogFields{"handler": "json-orders"})
	}
}
