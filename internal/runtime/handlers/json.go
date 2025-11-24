package handlers

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/ThreeDotsLabs/watermill/message"

	errspkg "github.com/enercity/protoflow/internal/runtime/errors"
	idspkg "github.com/enercity/protoflow/internal/runtime/ids"
	jsoncodec "github.com/enercity/protoflow/internal/runtime/jsoncodec"
	loggingpkg "github.com/enercity/protoflow/internal/runtime/logging"
	metadatapkg "github.com/enercity/protoflow/internal/runtime/metadata"
)

// JSONHandlerRegistration wires a typed JSON handler to the router.
type JSONHandlerRegistration[T any, O any] struct {
	Name         string
	ConsumeQueue string
	PublishQueue string
	Handler      JSONMessageHandler[T, O]
}

// JSONMessageContext exposes the incoming payload and metadata for JSON handlers.
type JSONMessageContext[T any] struct {
	Payload  T
	Metadata metadatapkg.Metadata
	Logger   loggingpkg.ServiceLogger
}

// CloneMetadata copies the current metadata map so handlers can mutate headers safely.
func (c JSONMessageContext[T]) CloneMetadata() metadatapkg.Metadata {
	return c.Metadata.Clone()
}

// JSONMessageOutput represents an event emitted by a JSON handler.
type JSONMessageOutput[T any] struct {
	Message  T
	Metadata metadatapkg.Metadata
}

// JSONMessageHandler processes a JSON payload and returns the events to publish.
type JSONMessageHandler[T any, O any] func(ctx context.Context, event JSONMessageContext[T]) ([]JSONMessageOutput[O], error)

// BuildJSONHandler converts a typed JSON handler into a Watermill handler.
func BuildJSONHandler[T any, O any](handler JSONMessageHandler[T, O], logger loggingpkg.ServiceLogger) (message.HandlerFunc, error) {
	if handler == nil {
		return nil, errspkg.ErrHandlerRequired
	}

	prototypeFactory, err := jsonPrototypeFactory[T]()
	if err != nil {
		return nil, err
	}

	return func(msg *message.Message) ([]*message.Message, error) {
		typed := prototypeFactory()

		if err := jsoncodec.Unmarshal(msg.Payload, typed); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON payload: %w", err)
		}

		ctx := JSONMessageContext[T]{
			Payload:  typed,
			Metadata: metadatapkg.FromWatermill(msg.Metadata),
			Logger:   logger,
		}

		outgoing, err := handler(msg.Context(), ctx)
		if err != nil {
			return nil, err
		}

		return convertJSONOutputs(outgoing, ctx.Metadata)
	}, nil
}

func jsonPrototypeFactory[T any]() (func() T, error) {
	var zero T
	typ := reflect.TypeOf(zero)
	if typ == nil {
		return nil, errspkg.ErrConsumeMessageTypeRequired
	}
	if typ.Kind() != reflect.Ptr {
		return nil, errspkg.ErrConsumeMessagePointerNeeded
	}
	elem := typ.Elem()
	return func() T {
		clone := reflect.New(elem).Interface()
		return clone.(T)
	}, nil
}

func convertJSONOutputs[T any](outputs []JSONMessageOutput[T], fallback metadatapkg.Metadata) ([]*message.Message, error) {
	if len(outputs) == 0 {
		return nil, nil
	}

	result := make([]*message.Message, len(outputs))
	for i, out := range outputs {
		if reflect.ValueOf(out.Message).IsZero() {
			return nil, errors.New("json handler emitted zero-value message")
		}

		payload, err := jsoncodec.Marshal(out.Message)
		if err != nil {
			return nil, err
		}

		metadata := out.Metadata
		if metadata == nil {
			metadata = fallback
		}
		if metadata == nil {
			metadata = metadatapkg.Metadata{}
		}
		metadata = metadata.Clone()
		metadata["event_message_schema"] = fmt.Sprintf("%T", out.Message)

		msg := message.NewMessage(idspkg.CreateULID(), payload)
		msg.Metadata = metadatapkg.ToWatermill(metadata)
		result[i] = msg
	}

	return result, nil
}
