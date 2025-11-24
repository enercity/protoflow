package transport

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/enercity/protoflow/internal/runtime/config"
)

func TestIOTransport(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "protoflow_io_test_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	conf := &config.Config{
		IOFile: tmpFile.Name(),
	}
	logger := watermill.NopLogger{}

	tr, err := ioTransport(conf, logger)
	if err != nil {
		t.Fatalf("failed to create io transport: %v", err)
	}

	topic := "test_topic"
	msg := message.NewMessage(watermill.NewUUID(), []byte("payload"))

	// Subscribe
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messages, err := tr.Subscriber.Subscribe(ctx, topic)
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

	if err := tr.Publisher.Close(); err != nil {
		t.Errorf("failed to close publisher: %v", err)
	}
	if err := tr.Subscriber.Close(); err != nil {
		t.Errorf("failed to close subscriber: %v", err)
	}
}

func TestIOTransport_DefaultFile(t *testing.T) {
	conf := &config.Config{
		IOFile: "",
	}
	logger := watermill.NopLogger{}

	tr, err := ioTransport(conf, logger)
	if err != nil {
		t.Fatalf("failed to create io transport: %v", err)
	}
	defer os.Remove("messages.log")

	if tr.Publisher == nil || tr.Subscriber == nil {
		t.Error("expected publisher and subscriber to be created")
	}
}

func TestIOTransport_Filtering(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "protoflow_io_filter_test_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	conf := &config.Config{IOFile: tmpFile.Name()}
	logger := watermill.NopLogger{}
	tr, err := ioTransport(conf, logger)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subA, err := tr.Subscriber.Subscribe(ctx, "topicA")
	if err != nil {
		t.Fatal(err)
	}

	if err := tr.Publisher.Publish("topicB", message.NewMessage(watermill.NewUUID(), []byte("payloadB"))); err != nil {
		t.Fatal(err)
	}
	if err := tr.Publisher.Publish("topicA", message.NewMessage(watermill.NewUUID(), []byte("payloadA"))); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-subA:
		if string(msg.Payload) != "payloadA" {
			t.Errorf("expected payloadA, got %s", msg.Payload)
		}
		msg.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for topicA message")
	}
}

func TestIOTransport_CorruptData(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "protoflow_io_corrupt_test_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// Write corrupt data
	if _, err := tmpFile.WriteString("not a json\n"); err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()

	conf := &config.Config{IOFile: tmpFile.Name()}
	logger := watermill.NopLogger{}
	tr, err := ioTransport(conf, logger)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := tr.Subscriber.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatal(err)
	}

	// Publish valid message
	if err := tr.Publisher.Publish("topic", message.NewMessage(watermill.NewUUID(), []byte("valid"))); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-sub:
		if string(msg.Payload) != "valid" {
			t.Errorf("expected valid, got %s", msg.Payload)
		}
		msg.Ack()
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for valid message")
	}
}

func TestIOTransport_PublishError(t *testing.T) {
	// Use a directory as file path to trigger open error
	tmpDir, err := os.MkdirTemp("", "protoflow_io_dir")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	conf := &config.Config{IOFile: tmpDir}
	logger := watermill.NopLogger{}
	tr, err := ioTransport(conf, logger)
	if err != nil {
		t.Fatal(err)
	}

	if err := tr.Publisher.Publish("topic", message.NewMessage(watermill.NewUUID(), nil)); err == nil {
		t.Error("expected error publishing to directory")
	}
}

func TestIOTransport_SubscribeError(t *testing.T) {
	// Use a directory as file path to trigger open error in Subscribe goroutine
	// Note: Subscribe itself returns nil error because it starts a goroutine.
	// We can't easily check the error from the goroutine without a custom logger or checking if channel closes immediately.

	tmpDir, err := os.MkdirTemp("", "protoflow_io_dir_sub")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	conf := &config.Config{IOFile: tmpDir}
	logger := watermill.NopLogger{}
	tr, err := ioTransport(conf, logger)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgs, err := tr.Subscriber.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatal(err)
	}

	// The channel should be closed or empty. Since we can't easily verify the log error,
	// we just ensure it doesn't panic and maybe closes the channel.
	select {
	case _, ok := <-msgs:
		if ok {
			t.Error("expected channel to be closed or empty")
		}
	case <-time.After(100 * time.Millisecond):
		// If it hangs, it might be retrying or stuck.
		// In the code: if os.OpenFile fails, it logs error and returns, closing the channel.
	}
}

func TestIOTransport_Nack(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "protoflow_io_nack_test_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	conf := &config.Config{IOFile: tmpFile.Name()}
	logger := watermill.NopLogger{}
	tr, err := ioTransport(conf, logger)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := tr.Subscriber.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatal(err)
	}

	if err := tr.Publisher.Publish("topic", message.NewMessage(watermill.NewUUID(), []byte("payload"))); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-sub:
		msg.Nack()
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestIOTransport_RecoversAfterEOF(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "protoflow_io_recover_test_*.log")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())
	tmpFile.Close()

	conf := &config.Config{IOFile: tmpFile.Name()}
	logger := watermill.NopLogger{}
	tr, err := ioTransport(conf, logger)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := tr.Subscriber.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatal(err)
	}

	// Give the subscriber time to hit EOF before any messages are published.
	time.Sleep(200 * time.Millisecond)

	msg := message.NewMessage(watermill.NewUUID(), []byte("recovered"))
	if err := tr.Publisher.Publish("topic", msg); err != nil {
		t.Fatal(err)
	}

	select {
	case received := <-sub:
		if string(received.Payload) != "recovered" {
			t.Fatalf("expected recovered payload, got %s", received.Payload)
		}
		received.Ack()
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for recovered message")
	}
}
