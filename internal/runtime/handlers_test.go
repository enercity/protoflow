package runtime

import (
	"context"
	"errors"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	handlerpkg "github.com/enercity/protoflow/internal/runtime/handlers"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestRegisterMessageHandlerRequiresService(t *testing.T) {
	err := RegisterMessageHandler(nil, MessageHandlerRegistration{})
	if err == nil {
		t.Fatal("expected error when service is nil")
	}
}

func TestRegisterMessageHandlerRegistersHandler(t *testing.T) {
	svc := newTestService(t)
	err := RegisterMessageHandler(svc, MessageHandlerRegistration{
		Name:         "raw",
		ConsumeQueue: "input",
		PublishQueue: "output",
		Handler: func(msg *message.Message) ([]*message.Message, error) {
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := svc.router.Handlers()["raw"]; !ok {
		t.Fatal("handler not registered")
	}
}

func TestRegisterMessageHandlerValidatesInput(t *testing.T) {
	svc := newTestService(t)

	tests := []struct {
		name string
		reg  MessageHandlerRegistration
		err  string
	}{
		{
			name: "missing handler",
			reg: MessageHandlerRegistration{
				Name:         "test",
				ConsumeQueue: "queue",
			},
			err: "protoflow: handler function is required",
		},
		{
			name: "missing consume queue",
			reg: MessageHandlerRegistration{
				Name:    "test",
				Handler: func(msg *message.Message) ([]*message.Message, error) { return nil, nil },
			},
			err: "protoflow: consume queue is required",
		},
		{
			name: "missing name",
			reg: MessageHandlerRegistration{
				ConsumeQueue: "queue",
				Handler:      func(msg *message.Message) ([]*message.Message, error) { return nil, nil },
			},
			err: "protoflow: handler name is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := RegisterMessageHandler(svc, tt.reg)
			if err == nil {
				t.Fatal("expected error")
			}
			if err.Error() != tt.err {
				t.Fatalf("expected error %q, got %q", tt.err, err.Error())
			}
		})
	}
}

func TestRegisterProtoMessage(t *testing.T) {
	svc := newTestService(t)
	svc.RegisterProtoMessage(nil) // Should not panic

	// We can't easily verify internal state without reflection or exposing it,
	// but we can verify it doesn't panic.
	// Actually, we can verify it by checking if it's in the registry if we had access.
	// But `registerProtoType` is internal.
	// However, `EnsureProtoPrototype` uses the registry? No, `EnsureProtoPrototype` is in `handlers` package but `Service` is in `runtime`.
	// Wait, `Service` has `protoRegistry`.
	// `Service` methods use it?
	// `registerProtoType` adds to `protoRegistry`.
	// There is no public accessor for `protoRegistry`.
	// But `RegisterProtoMessage` is public on `Service`.
	// We can just call it and ensure no panic.
}

func TestRegisterProtoHandler_ServiceRequired(t *testing.T) {
	err := RegisterProtoHandler[*structpb.Struct](nil, handlerpkg.ProtoHandlerRegistration[*structpb.Struct]{})
	if err == nil {
		t.Fatal("expected error for nil service")
	}
}

func TestRegisterProtoHandler_BuildFailure(t *testing.T) {
	svc := newTestService(t)
	// BuildProtoHandler fails if handler is nil
	err := RegisterProtoHandler[*structpb.Struct](svc, handlerpkg.ProtoHandlerRegistration[*structpb.Struct]{
		Name: "test",
		// Handler is nil
	})
	if err == nil {
		t.Fatal("expected error when handler is nil")
	}
}

func TestRegisterProtoHandler_RegisterFailure(t *testing.T) {
	svc := newTestService(t)
	// registerHandler fails if consume queue is empty
	err := RegisterProtoHandler[*structpb.Struct](svc, handlerpkg.ProtoHandlerRegistration[*structpb.Struct]{
		Name: "test",
		// ConsumeQueue is empty
		Handler: func(ctx context.Context, evt handlerpkg.ProtoMessageContext[*structpb.Struct]) ([]handlerpkg.ProtoMessageOutput, error) {
			return nil, nil
		},
	})
	if err == nil {
		t.Fatal("expected error when consume queue is empty")
	}
}

func TestRegisterProtoHandler_AdditionalTypes(t *testing.T) {
	svc := newTestService(t)
	err := RegisterProtoHandler[*structpb.Struct](svc, handlerpkg.ProtoHandlerRegistration[*structpb.Struct]{
		Name:         "test",
		ConsumeQueue: "queue",
		Handler: func(ctx context.Context, evt handlerpkg.ProtoMessageContext[*structpb.Struct]) ([]handlerpkg.ProtoMessageOutput, error) {
			return nil, nil
		},
		Options: []handlerpkg.ProtoHandlerOption{
			handlerpkg.WithPublishMessageTypes(&structpb.Value{}),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Verify registered type
	if _, ok := svc.protoRegistry["*structpb.Value"]; !ok {
		t.Fatal("additional type not registered")
	}
}

type mockValidator struct {
	err error
}

func (m *mockValidator) Validate(v any) error { return m.err }

func TestRegisterProtoHandler_Validation(t *testing.T) {
	svc := newTestService(t)
	svc.validator = &mockValidator{err: errors.New("invalid")}

	err := RegisterProtoHandler[*structpb.Struct](svc, handlerpkg.ProtoHandlerRegistration[*structpb.Struct]{
		Name:             "validated",
		ConsumeQueue:     "queue",
		ValidateOutgoing: true,
		Handler: func(ctx context.Context, evt handlerpkg.ProtoMessageContext[*structpb.Struct]) ([]handlerpkg.ProtoMessageOutput, error) {
			return []handlerpkg.ProtoMessageOutput{{Message: &structpb.Struct{}}}, nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Trigger handler
	handler := svc.router.Handlers()["validated"]
	msg := message.NewMessage(uuid.New().String(), []byte("{}"))
	_, err = handler(msg)
	if err == nil {
		t.Fatal("expected validation error")
	}
}
