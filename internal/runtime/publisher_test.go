package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/protobuf/types/known/structpb"

	errspkg "github.com/enercity/protoflow/internal/runtime/errors"
	metadatapkg "github.com/enercity/protoflow/internal/runtime/metadata"
)

type publisherTestContextKey struct{}

var testCtxKey = publisherTestContextKey{}

func TestNewMessageFromProto(t *testing.T) {
	// Test nil event
	_, err := NewMessageFromProto(nil, nil)
	if err == nil {
		t.Fatal("expected error when event is nil")
	}

	// Test success
	msg, err := NewMessageFromProto(&structpb.Struct{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg == nil {
		t.Fatal("expected message")
	}
}

func TestNewMessageFromProtoValidations(t *testing.T) {
	if _, err := NewMessageFromProto(nil, nil); err == nil {
		t.Fatal("expected error when event nil")
	}

	metadata := metadatapkg.Metadata{"origin": "unit"}
	msg, err := NewMessageFromProto(&structpb.Struct{}, metadata)
	if err != nil {
		t.Fatalf("unexpected error creating message: %v", err)
	}
	if msg.Metadata["event_message_schema"] == "" {
		t.Fatal("expected schema metadata to be set")
	}
	if msg.Metadata["origin"] != "unit" {
		t.Fatalf("expected metadata to be preserved, got %#v", msg.Metadata)
	}
}

func TestNewMessageFromProto_MarshalError(t *testing.T) {
	// Invalid UTF-8 in string field should cause marshal error
	m := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"key": {Kind: &structpb.Value_StringValue{StringValue: "\xff"}},
		},
	}
	_, err := NewMessageFromProto(m, nil)
	if err == nil {
		t.Fatal("expected marshal error for invalid UTF-8")
	}
}

func TestPublishProtoValidations(t *testing.T) {
	payload := &structpb.Struct{}
	if err := PublishProto(context.Background(), nil, "topic", payload, nil); !errors.Is(err, errspkg.ErrPublisherRequired) {
		t.Fatalf("expected publisher required error, got %v", err)
	}
	if err := PublishProto(context.Background(), &recordingPublisher{}, "", payload, nil); !errors.Is(err, errspkg.ErrTopicRequired) {
		t.Fatalf("expected topic required error, got %v", err)
	}
}

func TestPublishProto_MarshalError(t *testing.T) {
	m := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"key": {Kind: &structpb.Value_StringValue{StringValue: "\xff"}},
		},
	}
	err := PublishProto(context.Background(), &testPublisher{}, "topic", m, nil)
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestPublishProtoSetsContextAndTopic(t *testing.T) {
	payload := &structpb.Struct{}
	recorder := &recordingPublisher{}
	ctx := context.WithValue(context.Background(), testCtxKey, "ctx")
	metadata := metadatapkg.Metadata{"origin": "test"}

	if err := PublishProto(ctx, recorder, "orders", payload, metadata); err != nil {
		t.Fatalf("unexpected publish error: %v", err)
	}
	if len(recorder.topics) != 1 || recorder.topics[0] != "orders" {
		t.Fatalf("expected topic to be recorded, got %#v", recorder.topics)
	}
	if recorder.messages[0].Context().Value(testCtxKey) != "ctx" {
		t.Fatal("expected context to be attached to message")
	}
}

func TestServicePublishProtoValidatesReceiver(t *testing.T) {
	var svc *Service
	if err := svc.PublishProto(context.Background(), "topic", &structpb.Struct{}, nil); err == nil {
		t.Fatal("expected error when service nil")
	}
}

func TestServicePublishProto(t *testing.T) {
	svc := newTestService(t)
	pub := &testPublisher{}
	svc.publisher = pub

	err := svc.PublishProto(context.Background(), "topic", &structpb.Struct{}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.published) != 1 {
		t.Fatal("expected message to be published")
	}
}

type recordingPublisher struct {
	topics   []string
	messages []*message.Message
	err      error
}

func (p *recordingPublisher) Publish(topic string, messages ...*message.Message) error {
	if p.err != nil {
		return p.err
	}
	p.topics = append(p.topics, topic)
	p.messages = append(p.messages, messages...)
	return nil
}

func (p *recordingPublisher) Close() error { return nil }
