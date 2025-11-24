package transport

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/enercity/protoflow/internal/runtime/config"
)

func TestChannelTransport(t *testing.T) {
	conf := &config.Config{}
	logger := watermill.NopLogger{}

	tr, err := channelTransport(conf, logger)
	if err != nil {
		t.Fatalf("failed to create channel transport: %v", err)
	}

	topic := "test_topic"
	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))

	// Subscribe first
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
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}
