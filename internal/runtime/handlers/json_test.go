package handlers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"

	errspkg "github.com/enercity/protoflow/internal/runtime/errors"
	loggingpkg "github.com/enercity/protoflow/internal/runtime/logging"
	metadatapkg "github.com/enercity/protoflow/internal/runtime/metadata"
)

func TestBuildJSONHandlerProcessesPayload(t *testing.T) {
	handler, err := BuildJSONHandler(func(ctx context.Context, evt JSONMessageContext[*jsonIncoming]) ([]JSONMessageOutput[*jsonOutgoing], error) {
		if ctx == nil {
			t.Fatalf("context should not be nil")
		}
		if evt.Payload == nil || evt.Payload.ID != 42 {
			t.Fatalf("unexpected payload: %#v", evt.Payload)
		}
		md := evt.CloneMetadata()
		md["processed"] = "true"
		return []JSONMessageOutput[*jsonOutgoing]{
			{
				Message:  &jsonOutgoing{ID: evt.Payload.ID, Processed: time.Unix(100, 0)},
				Metadata: md,
			},
		}, nil
	}, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err != nil {
		t.Fatalf("unexpected error building handler: %v", err)
	}

	msg := message.NewMessage(uuid.New().String(), []byte(`{"id":42}`))
	msg.Metadata = message.Metadata{"origin": "test"}

	produced, err := handler(msg)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if len(produced) != 1 {
		t.Fatalf("expected single outgoing message, got %d", len(produced))
	}
	if produced[0].Metadata["processed"] != "true" {
		t.Fatalf("metadata not propagated: %#v", produced[0].Metadata)
	}
	if produced[0].Metadata["event_message_schema"] == "" {
		t.Fatal("expected schema metadata to be set")
	}
}

func TestBuildJSONHandlerUnmarshalError(t *testing.T) {
	handler, err := BuildJSONHandler(func(ctx context.Context, evt JSONMessageContext[*jsonIncoming]) ([]JSONMessageOutput[*jsonOutgoing], error) {
		return nil, nil
	}, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err != nil {
		t.Fatalf("unexpected error building handler: %v", err)
	}

	msg := message.NewMessage(uuid.New().String(), []byte(`{invalid-json`))
	_, err = handler(msg)
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestBuildJSONHandlerHandlerError(t *testing.T) {
	handler, err := BuildJSONHandler(func(ctx context.Context, evt JSONMessageContext[*jsonIncoming]) ([]JSONMessageOutput[*jsonOutgoing], error) {
		return nil, errors.New("handler failed")
	}, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err != nil {
		t.Fatalf("unexpected error building handler: %v", err)
	}

	msg := message.NewMessage(uuid.New().String(), []byte(`{"id":42}`))
	_, err = handler(msg)
	if err == nil {
		t.Fatal("expected handler error")
	}
}

func TestBuildJSONHandlerValidatesInputs(t *testing.T) {
	if _, err := BuildJSONHandler[*jsonIncoming, *jsonOutgoing](nil, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{})); !errors.Is(err, errspkg.ErrHandlerRequired) {
		t.Fatalf("expected handler required error, got %v", err)
	}
}

func TestJSONPrototypeFactoryValidations(t *testing.T) {
	_, err := jsonPrototypeFactory[any]()
	if !errors.Is(err, errspkg.ErrConsumeMessageTypeRequired) {
		t.Fatalf("expected consume type required error, got %v", err)
	}

	_, err = jsonPrototypeFactory[jsonIncoming]()
	if !errors.Is(err, errspkg.ErrConsumeMessagePointerNeeded) {
		t.Fatalf("expected pointer needed error, got %v", err)
	}

	factory, err := jsonPrototypeFactory[*jsonIncoming]()
	if err != nil {
		t.Fatalf("unexpected error creating factory: %v", err)
	}
	first := factory()
	second := factory()
	if first == second {
		t.Fatalf("expected distinct instances")
	}
}

func TestConvertJSONOutputsValidatesMessages(t *testing.T) {
	msgs, err := convertJSONOutputs([]JSONMessageOutput[*jsonOutgoing]{}, nil)
	if err != nil {
		t.Fatalf("unexpected error for empty output: %v", err)
	}
	if msgs != nil {
		t.Fatalf("expected nil when no outputs")
	}

	_, err = convertJSONOutputs([]JSONMessageOutput[*jsonOutgoing]{
		{Metadata: nil},
	}, nil)
	if err == nil {
		t.Fatal("expected error for zero-value message")
	}

	fallback := metadatapkg.Metadata{"origin": "fallback"}
	produced, err := convertJSONOutputs([]JSONMessageOutput[*jsonOutgoing]{
		{Message: &jsonOutgoing{ID: 7}, Metadata: nil},
	}, fallback)
	if err != nil {
		t.Fatalf("unexpected error converting outputs: %v", err)
	}
	if produced[0].Metadata.Get("origin") != "fallback" {
		t.Fatalf("expected fallback metadata to be used")
	}
}

func TestBuildJSONHandlerInvalidType(t *testing.T) {
	_, err := BuildJSONHandler[jsonIncoming, *jsonOutgoing](func(ctx context.Context, evt JSONMessageContext[jsonIncoming]) ([]JSONMessageOutput[*jsonOutgoing], error) {
		return nil, nil
	}, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err == nil {
		t.Fatal("expected error for non-pointer type")
	}
}

func TestConvertJSONOutputsZeroValue(t *testing.T) {
	outputs := []JSONMessageOutput[*jsonOutgoing]{
		{Message: nil},
	}
	_, err := convertJSONOutputs(outputs, metadatapkg.Metadata{})
	if err == nil {
		t.Fatal("expected error for zero value message")
	}
}

func TestConvertJSONOutputs_ZeroValue(t *testing.T) {
	outputs := []JSONMessageOutput[*jsonOutgoing]{
		{Message: nil},
	}
	_, err := convertJSONOutputs(outputs, nil)
	if err == nil || err.Error() != "json handler emitted zero-value message" {
		t.Fatalf("expected zero value error, got %v", err)
	}
}

func TestConvertJSONOutputs_Empty(t *testing.T) {
	msgs, err := convertJSONOutputs[*jsonOutgoing](nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatal("expected empty result")
	}
}

func TestConvertJSONOutputs_FallbackMetadata(t *testing.T) {
	outputs := []JSONMessageOutput[*jsonOutgoing]{
		{Message: &jsonOutgoing{ID: 1}},
	}
	fallback := metadatapkg.Metadata{"key": "value"}
	msgs, err := convertJSONOutputs(outputs, fallback)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msgs[0].Metadata["key"] != "value" {
		t.Fatal("expected fallback metadata")
	}
}

type jsonIncoming struct {
	ID int `json:"id"`
}

type jsonOutgoing struct {
	ID        int       `json:"id"`
	Processed time.Time `json:"processed"`
}
