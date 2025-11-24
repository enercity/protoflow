package runtime

import (
	"context"
	"fmt"
	"testing"

	handlerpkg "github.com/enercity/protoflow/internal/runtime/handlers"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestRegisterProtoHandlerValidations(t *testing.T) {
	svc := newTestService(t)
	err := RegisterProtoHandler(nil, handlerpkg.ProtoHandlerRegistration[*structpb.Struct]{
		Handler: func(context.Context, handlerpkg.ProtoMessageContext[*structpb.Struct]) ([]handlerpkg.ProtoMessageOutput, error) {
			return nil, nil
		},
	})
	if err == nil {
		t.Fatalf("expected error when service nil")
	}

	err = RegisterProtoHandler(svc, handlerpkg.ProtoHandlerRegistration[*structpb.Struct]{
		ConsumeQueue: "queue",
		Handler:      nil,
	})
	if err == nil {
		t.Fatalf("expected error when handler nil")
	}

	if err := RegisterProtoHandler(svc, handlerpkg.ProtoHandlerRegistration[*structpb.Struct]{
		ConsumeQueue: "queue",
		PublishQueue: "out",
		Handler: func(context.Context, handlerpkg.ProtoMessageContext[*structpb.Struct]) ([]handlerpkg.ProtoMessageOutput, error) {
			return nil, nil
		},
	}); err != nil {
		t.Fatalf("unexpected error registering handler: %v", err)
	}
	if _, ok := svc.router.Handlers()[fmt.Sprintf("%T-Handler", &structpb.Struct{})]; !ok {
		t.Fatalf("typed handler not registered")
	}
	if err := RegisterProtoHandler(svc, handlerpkg.ProtoHandlerRegistration[*structpb.Struct]{
		Name:         "typed_inferred",
		ConsumeQueue: "queue",
		PublishQueue: "out",
		Handler: func(context.Context, handlerpkg.ProtoMessageContext[*structpb.Struct]) ([]handlerpkg.ProtoMessageOutput, error) {
			return nil, nil
		},
	}); err != nil {
		t.Fatalf("expected handler to infer consume type: %v", err)
	}
	if _, ok := svc.router.Handlers()["typed_inferred"]; !ok {
		t.Fatalf("typed handler (inferred) not registered")
	}
}

func TestRegisterProtoHandlerRegistersPublishTypes(t *testing.T) {
	svc := newTestService(t)
	primary := MustProtoMessage[*structpb.Struct]()
	extra := MustProtoMessage[*structpb.ListValue]()

	if err := RegisterProtoHandler(svc, handlerpkg.ProtoHandlerRegistration[*structpb.Struct]{
		Name:         "typed",
		ConsumeQueue: "queue",
		PublishQueue: "out",
		Options: []handlerpkg.ProtoHandlerOption{
			handlerpkg.WithPublishMessageTypes(primary, extra),
		},
		Handler: func(context.Context, handlerpkg.ProtoMessageContext[*structpb.Struct]) ([]handlerpkg.ProtoMessageOutput, error) {
			return nil, nil
		},
	}); err != nil {
		t.Fatalf("unexpected error registering handler: %v", err)
	}

	if _, ok := svc.protoRegistry[fmt.Sprintf("%T", primary)]; !ok {
		t.Fatalf("primary publish type not registered")
	}
	if _, ok := svc.protoRegistry[fmt.Sprintf("%T", extra)]; !ok {
		t.Fatalf("option publish type not registered")
	}
}
