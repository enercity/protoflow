package main

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/enercity/protoflow"
	"github.com/enercity/protoflow/examples/models"
)

func main() {
	ctx := context.Background()
	baseLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger := protoflow.NewSlogServiceLogger(baseLogger)

	cfg := &protoflow.Config{
		PubSubSystem: "aws",
		AWSRegion:    "eu-west-1",
		AWSAccountID: "000000000000",
		AWSEndpoint:  "http://localhost:4566",
		PoisonQueue:  "orders.poison",
	}

	svc := protoflow.NewService(cfg, logger, ctx, protoflow.ServiceDependencies{})

	err := protoflow.RegisterProtoHandler(svc, protoflow.ProtoHandlerRegistration[*models.OrderCreated]{
		Name:             "proto-orders",
		ConsumeQueue:     "orders.created.proto",
		PublishQueue:     "orders.processed.proto",
		ValidateOutgoing: false,
		Handler: func(ctx context.Context, evt protoflow.ProtoMessageContext[*models.OrderCreated]) ([]protoflow.ProtoMessageOutput, error) {
			processed := &models.OrderProcessed{
				OrderId: evt.Payload.GetOrderId(),
				Status:  "processed",
			}
			metadata := evt.Metadata.With("event_version", "v1")
			return []protoflow.ProtoMessageOutput{{
				Message:  processed,
				Metadata: metadata,
			}}, nil
		},
		Options: []protoflow.ProtoHandlerOption{
			protoflow.WithPublishMessageTypes(&models.OrderProcessed{}),
		},
	})
	if err != nil {
		panic(err)
	}

	if err := svc.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("router stopped", err, protoflow.LogFields{"handler": "proto-orders"})
	}
}
