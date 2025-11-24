package runtime

import (
	"context"
	"errors"
	"testing"

	errspkg "github.com/enercity/protoflow/internal/runtime/errors"
	handlerpkg "github.com/enercity/protoflow/internal/runtime/handlers"
)

func TestRegisterJSONHandlerValidations(t *testing.T) {
	svc := newTestService(t)

	err := RegisterJSONHandler(nil, handlerpkg.JSONHandlerRegistration[*struct{}, *struct{}]{})
	if err == nil {
		t.Fatalf("expected error when service nil")
	}

	err = RegisterJSONHandler(svc, handlerpkg.JSONHandlerRegistration[*struct{}, *struct{}]{
		ConsumeQueue: "queue",
		PublishQueue: "out",
		Handler:      nil,
	})
	if err == nil {
		t.Fatalf("expected error when handler missing")
	}

	err = RegisterJSONHandler(svc, handlerpkg.JSONHandlerRegistration[struct{}, *outgoingMessage]{
		ConsumeQueue: "queue",
		PublishQueue: "out",
		Handler: func(context.Context, handlerpkg.JSONMessageContext[struct{}]) ([]handlerpkg.JSONMessageOutput[*outgoingMessage], error) {
			return nil, nil
		},
	})
	if !errors.Is(err, errspkg.ErrConsumeMessagePointerNeeded) {
		t.Fatalf("expected pointer error, got %v", err)
	}

	err = RegisterJSONHandler(svc, handlerpkg.JSONHandlerRegistration[*incomingMessage, *outgoingMessage]{
		Name:         "json_handler",
		ConsumeQueue: "queue",
		PublishQueue: "out",
		Handler: func(context.Context, handlerpkg.JSONMessageContext[*incomingMessage]) ([]handlerpkg.JSONMessageOutput[*outgoingMessage], error) {
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error registering handler: %v", err)
	}
	if _, ok := svc.router.Handlers()["json_handler"]; !ok {
		t.Fatalf("json handler not registered")
	}

	err = RegisterJSONHandler(svc, handlerpkg.JSONHandlerRegistration[*incomingMessage, *outgoingMessage]{
		Name:         "json_handler_inferred",
		ConsumeQueue: "queue",
		PublishQueue: "out",
		Handler: func(context.Context, handlerpkg.JSONMessageContext[*incomingMessage]) ([]handlerpkg.JSONMessageOutput[*outgoingMessage], error) {
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("expected handler to infer consume type: %v", err)
	}
}

type incomingMessage struct{}

type outgoingMessage struct{}
