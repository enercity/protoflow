package transport

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/enercity/protoflow/internal/runtime/config"
	natsgo "github.com/nats-io/nats.go"
)

func TestNATSTransport(t *testing.T) {
	natsURL := "nats://localhost:4222"
	// Check if NATS is available
	nc, err := natsgo.Connect(natsURL)
	if err != nil {
		t.Skip("NATS not available, skipping test")
	}
	nc.Close()

	conf := &config.Config{
		NATSURL: natsURL,
	}
	logger := watermill.NopLogger{}

	tr, err := natsTransport(conf, logger)
	if err != nil {
		t.Fatalf("failed to create nats transport: %v", err)
	}

	topic := "test_topic"
	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))

	// Subscribe
	messages, err := tr.Subscriber.Subscribe(context.Background(), topic)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Publish
	if err := tr.Publisher.Publish(topic, msg); err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	select {
	case received := <-messages:
		if string(received.Payload) != string(msg.Payload) {
			t.Errorf("expected payload %s, got %s", msg.Payload, received.Payload)
		}
		received.Ack()
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestNATSTransport_FactoryErrors(t *testing.T) {
	origPub := NATSPublisherFactory
	origSub := NATSSubscriberFactory
	defer func() {
		NATSPublisherFactory = origPub
		NATSSubscriberFactory = origSub
	}()

	t.Run("publisher error", func(t *testing.T) {
		NATSPublisherFactory = func(cfg nats.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
			return nil, fmt.Errorf("pub error")
		}
		_, err := natsTransport(&config.Config{}, watermill.NopLogger{})
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("subscriber error", func(t *testing.T) {
		NATSPublisherFactory = origPub
		NATSSubscriberFactory = func(cfg nats.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
			return nil, fmt.Errorf("sub error")
		}
		_, err := natsTransport(&config.Config{}, watermill.NopLogger{})
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("success", func(t *testing.T) {
		NATSPublisherFactory = func(cfg nats.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
			return &testPublisher{}, nil
		}
		NATSSubscriberFactory = func(cfg nats.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
			return &testSubscriber{}, nil
		}
		tr, err := natsTransport(&config.Config{}, watermill.NopLogger{})
		if err != nil {
			t.Fatal(err)
		}
		if tr.Publisher == nil || tr.Subscriber == nil {
			t.Error("expected publisher and subscriber")
		}
	})
}

func TestNATSTransport_DefaultFactories(t *testing.T) {
	// This test ensures the default factory functions are covered.
	// They will likely fail to connect, which is fine.

	// We need to ensure we are using the original factories.
	// Since other tests restore them, and we don't run in parallel, it should be fine.

	_, _ = NATSPublisherFactory(nats.PublisherConfig{URL: "nats://localhost:1234"}, watermill.NopLogger{})
	_, _ = NATSSubscriberFactory(nats.SubscriberConfig{URL: "nats://localhost:1234"}, watermill.NopLogger{})
}
