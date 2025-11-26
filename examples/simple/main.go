package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/enercity/protoflow"
)

var ErrTemporary = errors.New("temporary error")

// Simple example showing how to register an untyped handler.
func main() {
	ctx := context.Background()

	baseLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	logger := protoflow.NewSlogServiceLogger(baseLogger)

	cfg := &protoflow.Config{
		PubSubSystem: "channel",
		PoisonQueue:  "simple.poison",
	}

	// Define a custom retry middleware that only retries on ErrTemporary
	retryMiddleware := protoflow.RetryMiddleware(protoflow.RetryMiddlewareConfig{
		MaxRetries:      3,
		InitialInterval: time.Second,
		RetryIf: func(err error) bool {
			return errors.Is(err, ErrTemporary)
		},
	})

	// Reconstruct the default middleware chain, replacing the default retry middleware
	middlewares := []protoflow.MiddlewareRegistration{
		protoflow.CorrelationIDMiddleware(),
		protoflow.LogMessagesMiddleware(nil),
		protoflow.ProtoValidateMiddleware(),
		protoflow.OutboxMiddleware(),
		protoflow.TracerMiddleware(),
		protoflow.MetricsMiddleware(),
		retryMiddleware, // Use our custom retry middleware
		protoflow.PoisonQueueMiddleware(nil),
		protoflow.RecovererMiddleware(),
	}

	svc := protoflow.NewService(cfg, logger, ctx, protoflow.ServiceDependencies{
		DisableDefaultMiddlewares: true,
		Middlewares:               middlewares,
	})

	err := protoflow.RegisterMessageHandler(svc, protoflow.MessageHandlerRegistration{
		Name:         "raw-logger",
		ConsumeQueue: "simple.incoming",
		PublishQueue: "simple.outgoing",
		Handler:      createHandler(logger),
	})
	if err != nil {
		panic(err)
	}

	if err := svc.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("router stopped", err, protoflow.LogFields{"handler": "raw-logger"})
	}
}

func createHandler(logger protoflow.ServiceLogger) func(msg *message.Message) ([]*message.Message, error) {
	return func(msg *message.Message) ([]*message.Message, error) {
		// Simulate a temporary failure for demonstration
		if msg.Metadata.Get("simulate_error") == "true" {
			logger.Info("simulating temporary error", nil)
			return nil, ErrTemporary
		}

		logger.Info("received raw event", protoflow.LogFields{
			"payload":  string(msg.Payload),
			"metadata": msg.Metadata,
		})

		ack := message.NewMessage(protoflow.CreateUUID(), []byte("acknowledged"))
		ack.Metadata = message.Metadata{
			"event_source":         "simple_example",
			"event_message_schema": "example.AckV1",
		}
		return []*message.Message{ack}, nil
	}
}
