package protoflow

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

func TestHandlerExportsPropagateErrors(t *testing.T) {
	if err := RegisterJSONHandler[*structpb.Struct, *structpb.Struct](nil, JSONHandlerRegistration[*structpb.Struct, *structpb.Struct]{}); !errors.Is(err, ErrServiceRequired) {
		t.Fatalf("expected service required error, got %v", err)
	}

	if err := RegisterProtoHandler[*structpb.Struct](nil, ProtoHandlerRegistration[*structpb.Struct]{}); !errors.Is(err, ErrServiceRequired) {
		t.Fatalf("expected service required error, got %v", err)
	}
}

func TestProtoMessageHelpers(t *testing.T) {
	msg, err := NewProtoMessage[*structpb.Struct]()
	if err != nil {
		t.Fatalf("unexpected error creating proto: %v", err)
	}
	if msg == nil {
		t.Fatal("expected proto message instance")
	}

	must := MustProtoMessage[*structpb.Struct]()
	if must == nil {
		t.Fatal("expected must helper to return instance")
	}
}

func TestLoggerExports(t *testing.T) {
	logger := NewEntryServiceLogger(&stubEntry{})
	logger.Info(context.Background(), "boot", LogFields{"component": "test"})
}

func TestEncodingExportAliases(t *testing.T) {
	payload := map[string]string{"hello": "world"}
	if _, err := Marshal(payload); err != nil {
		t.Fatalf("marshal alias failed: %v", err)
	}
	if _, err := MarshalIndent(payload, "", "  "); err != nil {
		t.Fatalf("marshal indent alias failed: %v", err)
	}
	if err := Unmarshal([]byte(`{"hello":"world"}`), &payload); err != nil {
		t.Fatalf("unmarshal alias failed: %v", err)
	}
}

func TestMetadataExport(t *testing.T) {
	md := NewMetadata("key", "value")
	if md["key"] != "value" {
		t.Fatalf("expected metadata to contain key, got %#v", md)
	}
}

type stubEntry struct {
	fields LogFields
	err    error
}

func (s *stubEntry) Error(_ context.Context, args ...any) {}
func (s *stubEntry) Info(_ context.Context, args ...any)  {}
func (s *stubEntry) Debug(_ context.Context, args ...any) {}
func (s *stubEntry) Trace(_ context.Context, args ...any) {}

func (s *stubEntry) WithError(err error) *stubEntry {
	clone := *s
	clone.err = err
	return &clone
}

func (s *stubEntry) WithField(key string, value any) *stubEntry {
	clone := *s
	if clone.fields == nil {
		clone.fields = make(LogFields)
	}
	clone.fields[key] = value
	return &clone
}
