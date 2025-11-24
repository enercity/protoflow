package handlers

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ThreeDotsLabs/watermill/message"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	errspkg "github.com/enercity/protoflow/internal/runtime/errors"
	loggingpkg "github.com/enercity/protoflow/internal/runtime/logging"
	metadatapkg "github.com/enercity/protoflow/internal/runtime/metadata"
)

// ProtoHandlerRegistration configures a typed protobuf handler that automatically
// unmarshals incoming payloads and marshals emitted events.
type ProtoHandlerRegistration[T proto.Message] struct {
	Name             string
	ConsumeQueue     string
	PublishQueue     string
	Handler          ProtoMessageHandler[T]
	Options          []ProtoHandlerOption
	ValidateOutgoing bool
}

// ProtoHandlerOption customises handler registration.
type ProtoHandlerOption func(*protoHandlerOptions)

type protoHandlerOptions struct {
	additionalPublishTypes []proto.Message
}

// ProtoHandlerOptions exposes the resolved handler configuration to callers.
type ProtoHandlerOptions struct {
	AdditionalPublishTypes []proto.Message
}

// ApplyProtoHandlerOptions resolves the supplied options into a concrete configuration.
func ApplyProtoHandlerOptions(opts []ProtoHandlerOption) ProtoHandlerOptions {
	config := protoHandlerOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}
	return ProtoHandlerOptions{AdditionalPublishTypes: config.additionalPublishTypes}
}

// WithPublishMessageTypes registers extra proto schemas emitted by this handler.
// Use this when the handler may emit multiple message types.
func WithPublishMessageTypes(msgs ...proto.Message) ProtoHandlerOption {
	return func(cfg *protoHandlerOptions) {
		cfg.additionalPublishTypes = append(cfg.additionalPublishTypes, msgs...)
	}
}

// ProtoMessageContext provides strongly typed access to the incoming message payload.
type ProtoMessageContext[T proto.Message] struct {
	Payload  T
	Metadata metadatapkg.Metadata
	Logger   loggingpkg.ServiceLogger
}

// CloneMetadata returns a copy of the current metadata map so handlers can safely
// mutate headers for outgoing events without touching the original map.
func (c ProtoMessageContext[T]) CloneMetadata() metadatapkg.Metadata {
	return c.Metadata.Clone()
}

// ProtoMessageOutput describes an event that should be emitted after the handler succeeds.
type ProtoMessageOutput struct {
	Message  proto.Message
	Metadata metadatapkg.Metadata
}

// ProtoMessageHandler processes a typed protobuf payload and returns the events to emit.
type ProtoMessageHandler[T proto.Message] func(ctx context.Context, event ProtoMessageContext[T]) ([]ProtoMessageOutput, error)

// ProtoMessageFactory converts proto payloads into Watermill messages.
type ProtoMessageFactory func(proto.Message, metadatapkg.Metadata) (*message.Message, error)

// BuildProtoHandler converts the typed handler into a Watermill handler using the provided factory.
func BuildProtoHandler[T proto.Message](prototype T, handler ProtoMessageHandler[T], validate func(proto.Message) error, factory ProtoMessageFactory, logger loggingpkg.ServiceLogger) (message.HandlerFunc, error) {
	if handler == nil {
		return nil, errspkg.ErrHandlerRequired
	}
	if isNilProto(prototype) {
		return nil, errspkg.ErrConsumeMessageTypeRequired
	}
	if factory == nil {
		return nil, errors.New("proto message factory is required")
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		typed, err := clonePrototype(prototype)
		if err != nil {
			return nil, err
		}

		if err := protojson.Unmarshal(msg.Payload, typed); err != nil {
			return nil, fmt.Errorf("failed to unmarshal %T payload: %w", prototype, err)
		}

		ctx := ProtoMessageContext[T]{
			Payload:  typed,
			Metadata: metadatapkg.FromWatermill(msg.Metadata),
			Logger:   logger,
		}

		outgoing, err := handler(msg.Context(), ctx)
		if err != nil {
			return nil, err
		}

		if validate != nil {
			for _, out := range outgoing {
				if out.Message == nil {
					return nil, errors.New("proto handler emitted nil message")
				}
				if err := validate(out.Message); err != nil {
					return nil, err
				}
			}
		}

		return convertProtoOutputs(outgoing, ctx.Metadata, factory)
	}, nil
}

func clonePrototype[T proto.Message](prototype T) (T, error) {
	if isNilProto(prototype) {
		var zero T
		return zero, errspkg.ErrConsumeMessageTypeRequired
	}

	cloned := proto.Clone(prototype)
	proto.Reset(cloned)

	typed, ok := cloned.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("unexpected prototype type %T", cloned)
	}

	return typed, nil
}

func EnsureProtoPrototype[T proto.Message](candidate T) (T, error) {
	if !isNilProto(candidate) {
		return candidate, nil
	}

	var zero T
	typ := reflect.TypeOf(candidate)
	if typ == nil {
		return zero, errspkg.ErrConsumeMessageTypeRequired
	}
	if typ.Kind() != reflect.Ptr {
		return zero, errspkg.ErrConsumeMessagePointerNeeded
	}

	inst := reflect.New(typ.Elem()).Interface()
	typed, ok := inst.(T)
	if !ok {
		return zero, fmt.Errorf("unexpected prototype type %s", typ)
	}
	return typed, nil
}

func convertProtoOutputs(outputs []ProtoMessageOutput, fallback metadatapkg.Metadata, factory ProtoMessageFactory) ([]*message.Message, error) {
	if len(outputs) == 0 {
		return nil, nil
	}

	result := make([]*message.Message, len(outputs))
	for i, out := range outputs {
		metadata := out.Metadata
		if metadata == nil {
			metadata = fallback
		}

		msg, err := factory(out.Message, metadata)
		if err != nil {
			return nil, err
		}
		result[i] = msg
	}

	return result, nil
}

func isNilProto[T proto.Message](prototype T) bool {
	msg := proto.Message(prototype)
	if msg == nil {
		return true
	}

	val := reflect.ValueOf(msg)
	switch val.Kind() {
	case reflect.Interface, reflect.Ptr, reflect.Slice, reflect.Map, reflect.Func:
		return val.IsNil()
	default:
		return false
	}
}
