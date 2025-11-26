package handlers

import (
	"context"
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/structpb"

	errspkg "github.com/enercity/protoflow/internal/runtime/errors"
	loggingpkg "github.com/enercity/protoflow/internal/runtime/logging"
	metadatapkg "github.com/enercity/protoflow/internal/runtime/metadata"
)

func TestBuildProtoHandlerProcessesPayload(t *testing.T) {
	prototype := &structpb.Struct{}
	validated := 0
	capturedMetadata := make([]metadatapkg.Metadata, 0, 1)
	factory := func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		capturedMetadata = append(capturedMetadata, metadata.Clone())
		return message.NewMessage(uuid.New().String(), []byte("ok")), nil
	}

	handler, err := BuildProtoHandler(prototype, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		if ctx == nil {
			t.Fatalf("context should not be nil")
		}
		md := evt.CloneMetadata()
		md["seen"] = "true"
		return []ProtoMessageOutput{{Message: evt.Payload, Metadata: md}}, nil
	}, func(msg proto.Message) error {
		validated++
		if msg == nil {
			return errors.New("message missing")
		}
		return nil
	}, factory, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err != nil {
		t.Fatalf("unexpected error building handler: %v", err)
	}

	payload := &structpb.Struct{}
	bytes, err := payload.MarshalJSON()
	if err != nil {
		t.Fatalf("failed to marshal proto: %v", err)
	}

	msg := message.NewMessage(uuid.New().String(), bytes)
	msg.Metadata = message.Metadata{"origin": "test"}

	produced, err := handler(msg)
	if err != nil {
		t.Fatalf("handler returned error: %v", err)
	}
	if len(produced) != 1 {
		t.Fatalf("expected single outgoing message, got %d", len(produced))
	}
	if validated != 1 {
		t.Fatalf("expected validator to run once, got %d", validated)
	}
	if capturedMetadata[0]["seen"] != "true" {
		t.Fatalf("expected metadata to be cloned")
	}
}

func TestBuildProtoHandlerUnmarshalError(t *testing.T) {
	prototype := &structpb.Struct{}
	factory := func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		return nil, nil
	}
	handler, err := BuildProtoHandler(prototype, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return nil, nil
	}, nil, factory, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err != nil {
		t.Fatalf("unexpected error building handler: %v", err)
	}

	msg := message.NewMessage(uuid.New().String(), []byte(`{invalid-json`))
	_, err = handler(msg)
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestBuildProtoHandlerHandlerError(t *testing.T) {
	prototype := &structpb.Struct{}
	factory := func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		return nil, nil
	}
	handler, err := BuildProtoHandler(prototype, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return nil, errors.New("handler failed")
	}, nil, factory, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err != nil {
		t.Fatalf("unexpected error building handler: %v", err)
	}

	payload := &structpb.Struct{}
	bytes, _ := payload.MarshalJSON()
	msg := message.NewMessage(uuid.New().String(), bytes)
	_, err = handler(msg)
	if err == nil {
		t.Fatal("expected handler error")
	}
}

func TestBuildProtoHandlerValidationFailure(t *testing.T) {
	prototype := &structpb.Struct{}
	factory := func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		return nil, nil
	}
	handler, err := BuildProtoHandler(prototype, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return []ProtoMessageOutput{{Message: evt.Payload}}, nil
	}, func(msg proto.Message) error {
		return errors.New("validation failed")
	}, factory, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err != nil {
		t.Fatalf("unexpected error building handler: %v", err)
	}

	payload := &structpb.Struct{}
	bytes, _ := payload.MarshalJSON()
	msg := message.NewMessage(uuid.New().String(), bytes)
	_, err = handler(msg)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestBuildProtoHandlerFactoryFailure(t *testing.T) {
	prototype := &structpb.Struct{}
	factory := func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		return nil, errors.New("factory failed")
	}
	handler, err := BuildProtoHandler(prototype, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return []ProtoMessageOutput{{Message: evt.Payload}}, nil
	}, nil, factory, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err != nil {
		t.Fatalf("unexpected error building handler: %v", err)
	}

	payload := &structpb.Struct{}
	bytes, _ := payload.MarshalJSON()
	msg := message.NewMessage(uuid.New().String(), bytes)
	_, err = handler(msg)
	if err == nil {
		t.Fatal("expected factory error")
	}
}

func TestBuildProtoHandlerValidations(t *testing.T) {
	_, err := BuildProtoHandler[*structpb.Struct](nil, nil, nil, nil, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if !errors.Is(err, errspkg.ErrHandlerRequired) {
		t.Fatalf("expected handler required error, got %v", err)
	}

	_, err = BuildProtoHandler[*structpb.Struct](nil, func(context.Context, ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return nil, nil
	}, nil, func(proto.Message, metadatapkg.Metadata) (*message.Message, error) {
		return nil, nil
	}, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if !errors.Is(err, errspkg.ErrConsumeMessageTypeRequired) {
		t.Fatalf("expected consume type required error, got %v", err)
	}

	_, err = BuildProtoHandler(&structpb.Struct{}, func(context.Context, ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return nil, nil
	}, nil, nil, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err == nil {
		t.Fatal("expected error when factory missing")
	}
}

func TestClonePrototypeValidations(t *testing.T) {
	var zero *structpb.Struct
	if _, err := clonePrototype(zero); !errors.Is(err, errspkg.ErrConsumeMessageTypeRequired) {
		t.Fatalf("expected consume type required error, got %v", err)
	}

	prototype := &structpb.Struct{}
	cloned, err := clonePrototype(prototype)
	if err != nil {
		t.Fatalf("unexpected clone error: %v", err)
	}
	if cloned == prototype {
		t.Fatalf("expected clone to return new instance")
	}
}

func TestApplyProtoHandlerOptions(t *testing.T) {
	opts := ApplyProtoHandlerOptions([]ProtoHandlerOption{
		WithPublishMessageTypes(&structpb.Struct{}),
		nil, // Should be ignored
	})
	if len(opts.AdditionalPublishTypes) != 1 {
		t.Fatal("expected 1 additional publish type")
	}
}

func TestEnsureProtoPrototype(t *testing.T) {
	// Test with nil input (should return error)
	// We need to pass a typed nil.
	var nilStruct *structpb.Struct
	_, err := EnsureProtoPrototype(nilStruct)
	if err == nil {
		// Wait, EnsureProtoPrototype(nil) returns a new instance!
		// My previous analysis was that it returns error if typ is nil.
		// But here typ is *structpb.Struct.
		// So it returns a new instance.
		// So err should be nil.
	} else {
		t.Fatalf("unexpected error for nil pointer: %v", err)
	}

	// Test with non-nil input
	s := &structpb.Struct{}
	res, err := EnsureProtoPrototype(s)
	if err != nil {
		t.Fatalf("unexpected error for non-nil input: %v", err)
	}
	if res != s {
		t.Fatal("expected same instance for non-nil input")
	}
}

func TestIsNilProto(t *testing.T) {
	var nilStruct *structpb.Struct
	if !isNilProto(nilStruct) {
		t.Fatal("expected nil pointer to be detected")
	}

	nonNil := &structpb.Struct{}
	if isNilProto(nonNil) {
		t.Fatal("expected non-nil pointer to be detected")
	}
}

func TestBuildProtoHandlerNilFactory(t *testing.T) {
	_, err := BuildProtoHandler[*structpb.Struct](&structpb.Struct{}, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return nil, nil
	}, nil, nil, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err == nil {
		t.Fatal("expected error for nil factory")
	}
}

func TestConvertProtoOutputsFactoryError(t *testing.T) {
	factory := func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		return nil, errors.New("factory fail")
	}
	outputs := []ProtoMessageOutput{{Message: &structpb.Struct{}}}
	_, err := convertProtoOutputs(outputs, metadatapkg.Metadata{}, factory)
	if err == nil {
		t.Fatal("expected error when factory fails")
	}
}

func TestClonePrototypeNil(t *testing.T) {
	_, err := clonePrototype[*structpb.Struct](nil)
	if err == nil {
		t.Fatal("expected error for nil prototype")
	}
}

func TestBuildProtoHandlerNilPrototype(t *testing.T) {
	_, err := BuildProtoHandler[*structpb.Struct](nil, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return nil, nil
	}, nil, func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) { return nil, nil }, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err == nil {
		t.Fatal("expected error for nil prototype")
	}
}

func TestBuildProtoHandlerValidatorError(t *testing.T) {
	handler, _ := BuildProtoHandler(&structpb.Struct{}, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return []ProtoMessageOutput{{Message: &structpb.Struct{}}}, nil
	}, func(msg proto.Message) error {
		return errors.New("validate fail")
	}, func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		return message.NewMessage("1", nil), nil
	}, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))

	msg := message.NewMessage("1", []byte("{}"))
	_, err := handler(msg)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestBuildProtoHandlerValidatorNilMessage(t *testing.T) {
	handler, _ := BuildProtoHandler(&structpb.Struct{}, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return []ProtoMessageOutput{{Message: nil}}, nil
	}, func(msg proto.Message) error { return nil }, func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		return message.NewMessage("1", nil), nil
	}, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))

	msg := message.NewMessage("1", []byte("{}"))
	_, err := handler(msg)
	if err == nil {
		t.Fatal("expected error for nil message in output")
	}
}

func TestBuildProtoHandler_NilMessageWithValidator(t *testing.T) {
	handler, _ := BuildProtoHandler(&structpb.Struct{}, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return []ProtoMessageOutput{{Message: nil}}, nil
	}, func(msg proto.Message) error { return nil }, func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		return nil, nil
	}, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))

	msg := message.NewMessage(uuid.New().String(), []byte("{}"))
	_, err := handler(msg)
	if err == nil || err.Error() != "proto handler emitted nil message" {
		t.Fatalf("expected nil message error, got %v", err)
	}
}

func TestBuildProtoHandler_MissingFactory(t *testing.T) {
	prototype := &structpb.Struct{}
	_, err := BuildProtoHandler(prototype, func(ctx context.Context, evt ProtoMessageContext[*structpb.Struct]) ([]ProtoMessageOutput, error) {
		return nil, nil
	}, nil, nil, loggingpkg.NewWatermillServiceLogger(watermill.NopLogger{}))
	if err == nil || err.Error() != "proto message factory is required" {
		t.Fatalf("expected factory required error, got %v", err)
	}
}

type MapProto map[string]string

func (m MapProto) ProtoReflect() protoreflect.Message {
	return nil
}

func TestEnsureProtoPrototype_NonPointer(t *testing.T) {
	// Use a nil map that implements proto.Message.
	// isNilProto will return true, but it's not a pointer, so EnsureProtoPrototype should fail.
	var val MapProto
	_, err := EnsureProtoPrototype(val)
	if err != errspkg.ErrConsumeMessagePointerNeeded {
		t.Fatalf("expected pointer needed error, got %v", err)
	}
}

func TestConvertProtoOutputs_FactoryError(t *testing.T) {
	outputs := []ProtoMessageOutput{
		{Message: &structpb.Struct{}},
	}
	factory := func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		return nil, errors.New("factory error")
	}
	_, err := convertProtoOutputs(outputs, nil, factory)
	if err == nil || err.Error() != "factory error" {
		t.Fatalf("expected factory error, got %v", err)
	}
}

type StructProto struct{}

func (s StructProto) ProtoReflect() protoreflect.Message {
	return nil
}

func TestIsNilProto_Struct(t *testing.T) {
	val := StructProto{}
	if isNilProto(val) {
		t.Fatal("expected struct to be non-nil")
	}
}

func TestEnsureProtoPrototype_NilInterface(t *testing.T) {
	var val proto.Message // nil interface
	_, err := EnsureProtoPrototype(val)
	if err != errspkg.ErrConsumeMessageTypeRequired {
		t.Fatalf("expected type required error, got %v", err)
	}
}

func TestConvertProtoOutputs_FallbackMetadata(t *testing.T) {
	outputs := []ProtoMessageOutput{
		{Message: &structpb.Struct{}},
	}
	fallback := metadatapkg.Metadata{"key": "value"}
	factory := func(msg proto.Message, metadata metadatapkg.Metadata) (*message.Message, error) {
		if metadata["key"] != "value" {
			return nil, errors.New("metadata not propagated")
		}
		return message.NewMessage(uuid.New().String(), []byte("ok")), nil
	}
	_, err := convertProtoOutputs(outputs, fallback, factory)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestConvertProtoOutputs_Empty(t *testing.T) {
	msgs, err := convertProtoOutputs(nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatal("expected empty result")
	}
}
