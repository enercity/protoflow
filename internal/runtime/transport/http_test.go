package transport

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-http/v2/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/enercity/protoflow/internal/runtime/config"
)

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func TestHTTPTransport(t *testing.T) {
	port, err := getFreePort()
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}
	address := fmt.Sprintf("localhost:%d", port)
	baseURL := fmt.Sprintf("http://%s/", address)

	conf := &config.Config{
		HTTPServerAddress: address,
		HTTPPublisherURL:  baseURL,
	}
	logger := watermill.NopLogger{}

	tr, err := httpTransport(conf, logger)
	if err != nil {
		t.Fatalf("failed to create http transport: %v", err)
	}

	// Give the server a moment to start
	time.Sleep(200 * time.Millisecond)

	topic := "test_topic"
	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))

	// Subscribe
	messages, err := tr.Subscriber.Subscribe(context.Background(), topic)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Publish in a goroutine because HTTP publisher waits for response (Ack)
	// and Subscriber waits for message to be consumed and Acked.
	go func() {
		if err := tr.Publisher.Publish(topic, msg); err != nil {
			t.Logf("failed to publish: %v", err)
		}
	}()

	select {
	case received := <-messages:
		if string(received.Payload) != string(msg.Payload) {
			t.Errorf("expected payload %s, got %s", msg.Payload, received.Payload)
		}
		received.Ack()
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	if err := tr.Subscriber.Close(); err != nil {
		t.Errorf("failed to close subscriber: %v", err)
	}
}

func TestHTTPTransport_FactoryErrors(t *testing.T) {
	origPub := HTTPPublisherFactory
	origSub := HTTPSubscriberFactory
	defer func() {
		HTTPPublisherFactory = origPub
		HTTPSubscriberFactory = origSub
	}()

	t.Run("publisher error", func(t *testing.T) {
		HTTPPublisherFactory = func(config http.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
			return nil, fmt.Errorf("pub error")
		}
		_, err := httpTransport(&config.Config{}, watermill.NopLogger{})
		if err == nil {
			t.Error("expected error")
		}
	})

	t.Run("subscriber error", func(t *testing.T) {
		HTTPPublisherFactory = origPub
		HTTPSubscriberFactory = func(addr string, config http.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
			return nil, fmt.Errorf("sub error")
		}
		_, err := httpTransport(&config.Config{}, watermill.NopLogger{})
		if err == nil {
			t.Error("expected error")
		}
	})
}
