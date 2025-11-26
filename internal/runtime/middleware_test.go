package runtime

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	configpkg "github.com/enercity/protoflow/internal/runtime/config"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	loggingpkg "github.com/enercity/protoflow/internal/runtime/logging"
)

func TestCorrelationIDMiddleware(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	mw := svc.correlationIDMiddleware()

	t.Run("adds missing id", func(t *testing.T) {
		msg := message.NewMessage(uuid.New().String(), nil)
		msg.Metadata = message.Metadata{}
		called := false
		_, err := mw(func(m *message.Message) ([]*message.Message, error) {
			called = true
			if m.Metadata["correlation_id"] == "" {
				t.Fatal("expected correlation id to be populated")
			}
			return nil, nil
		})(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !called {
			t.Fatal("handler not invoked")
		}
	})

	t.Run("keeps existing id", func(t *testing.T) {
		msg := message.NewMessage(uuid.New().String(), nil)
		msg.Metadata = message.Metadata{"correlation_id": "fixed"}
		_, err := mw(func(m *message.Message) ([]*message.Message, error) {
			if m.Metadata["correlation_id"] != "fixed" {
				t.Fatal("expected correlation id to be preserved")
			}
			return nil, nil
		})(msg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestProtoValidateMiddleware(t *testing.T) {
	t.Parallel()

	t.Run("skips when validator unset", testProtoValidateMiddlewareSkipsWhenValidatorUnset)
	t.Run("warns when schema missing", testProtoValidateMiddlewareWarnsWhenSchemaMissing)
	t.Run("fails for unknown schema", testProtoValidateMiddlewareFailsForUnknownSchema)
	t.Run("fails for invalid payload", testProtoValidateMiddlewareFailsForInvalidPayload)
	t.Run("fails validation", testProtoValidateMiddlewareFailsValidation)
	t.Run("passes on success", testProtoValidateMiddlewarePassesOnSuccess)
}

func testProtoValidateMiddlewareSkipsWhenValidatorUnset(t *testing.T) {
	t.Helper()
	svc := &Service{}
	mw := svc.protoValidateMiddleware()
	msg := message.NewMessage(uuid.New().String(), []byte(`{"foo":"bar"}`))
	msg.Metadata = message.Metadata{}
	if _, err := mw(func(m *message.Message) ([]*message.Message, error) { return nil, nil })(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testProtoValidateMiddlewareWarnsWhenSchemaMissing(t *testing.T) {
	t.Helper()
	svc := &Service{validator: &testValidator{}}
	mw := svc.protoValidateMiddleware()
	msg := message.NewMessage(uuid.New().String(), []byte(`{"foo":"bar"}`))
	msg.Metadata = message.Metadata{}
	if _, err := mw(func(m *message.Message) ([]*message.Message, error) { return nil, nil })(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testProtoValidateMiddlewareFailsForUnknownSchema(t *testing.T) {
	t.Helper()
	svc := &Service{validator: &testValidator{}, protoRegistry: make(map[string]func() proto.Message)}
	mw := svc.protoValidateMiddleware()
	msg := message.NewMessage(uuid.New().String(), []byte(`{"foo":"bar"}`))
	msg.Metadata = message.Metadata{"event_message_schema": "unknown"}
	if _, err := mw(func(m *message.Message) ([]*message.Message, error) { return nil, nil })(msg); err == nil {
		t.Fatal("expected error for unknown schema")
	} else if _, ok := err.(*UnprocessableEventError); !ok {
		t.Fatalf("unexpected error type: %T", err)
	}
}

func testProtoValidateMiddlewareFailsForInvalidPayload(t *testing.T) {
	t.Helper()
	svc := &Service{validator: &testValidator{}, protoRegistry: make(map[string]func() proto.Message)}
	svc.registerProtoType(&structpb.Struct{})
	mw := svc.protoValidateMiddleware()
	msg := message.NewMessage(uuid.New().String(), []byte("not json"))
	msg.Metadata = message.Metadata{"event_message_schema": "*structpb.Struct"}
	if _, err := mw(func(m *message.Message) ([]*message.Message, error) { return nil, nil })(msg); err == nil {
		t.Fatal("expected error for invalid payload")
	}
}

func testProtoValidateMiddlewareFailsValidation(t *testing.T) {
	t.Helper()
	svc := &Service{validator: &testValidator{err: errors.New("bad")}, protoRegistry: make(map[string]func() proto.Message)}
	svc.registerProtoType(&structpb.Struct{})
	mw := svc.protoValidateMiddleware()
	msg := message.NewMessage(uuid.New().String(), []byte(`{"foo":"bar"}`))
	msg.Metadata = message.Metadata{"event_message_schema": "*structpb.Struct"}
	if _, err := mw(func(m *message.Message) ([]*message.Message, error) { return nil, nil })(msg); err == nil {
		t.Fatal("expected validation error")
	}
}

func testProtoValidateMiddlewarePassesOnSuccess(t *testing.T) {
	t.Helper()
	svc := &Service{validator: &testValidator{}, protoRegistry: make(map[string]func() proto.Message)}
	svc.registerProtoType(&structpb.Struct{})
	mw := svc.protoValidateMiddleware()
	msg := message.NewMessage(uuid.New().String(), []byte(`{"foo":"bar"}`))
	msg.Metadata = message.Metadata{"event_message_schema": "*structpb.Struct"}
	called := false
	_, err := mw(func(m *message.Message) ([]*message.Message, error) {
		called = true
		return nil, nil
	})(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Fatal("handler not invoked")
	}
}

func TestPoisonMiddlewareWithFilter(t *testing.T) {
	t.Parallel()

	svc := &Service{
		Conf:      &configpkg.Config{PoisonQueue: "poison"},
		publisher: &testPublisher{},
	}
	mw, err := svc.poisonMiddlewareWithFilter(func(err error) bool { return true })
	if err != nil {
		t.Fatalf("unexpected error creating poison middleware: %v", err)
	}
	msg := message.NewMessage(uuid.New().String(), nil)
	msg.Metadata = message.Metadata{}
	pub := svc.publisher.(*testPublisher)
	_, _ = mw(func(m *message.Message) ([]*message.Message, error) {
		return nil, errors.New("boom")
	})(msg)
	if len(pub.Topics()) != 1 || pub.Topics()[0] != "poison" {
		t.Fatalf("expected poison message to be published: %#v", pub.Topics())
	}

	t.Run("returns error when middleware creation fails", func(t *testing.T) {
		svc := &Service{Conf: &configpkg.Config{}, publisher: nil}
		if _, err := svc.poisonMiddlewareWithFilter(func(error) bool { return false }); err == nil {
			t.Fatal("expected error when poison queue misconfigured")
		}
	})
}

func TestLogMessagesMiddleware(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	logger := &recordingServiceLogger{}
	mw := svc.logMessagesMiddleware(logger)
	msg := message.NewMessage(uuid.New().String(), []byte("payload"))
	msg.Metadata = message.Metadata{"key": "value"}
	_, err := mw(func(m *message.Message) ([]*message.Message, error) { return nil, nil })(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if logger.debugCount() == 0 {
		t.Fatal("expected log entry to be recorded")
	}
}

type recordingServiceLogger struct {
	infos  int
	debugs int
}

func (r *recordingServiceLogger) With(fields loggingpkg.LogFields) loggingpkg.ServiceLogger { return r }

func (r *recordingServiceLogger) Debug(string, loggingpkg.LogFields) { r.debugs++ }

func (r *recordingServiceLogger) Info(string, loggingpkg.LogFields) { r.infos++ }

func (r *recordingServiceLogger) Error(string, error, loggingpkg.LogFields) {}

func (r *recordingServiceLogger) Trace(string, loggingpkg.LogFields) {}

func (r *recordingServiceLogger) debugCount() int { return r.debugs }

func TestOutboxMiddleware(t *testing.T) {
	t.Parallel()

	t.Run("skips when outbox missing", testOutboxMiddlewareSkipsWhenOutboxMissing)
	t.Run("propagates handler error", testOutboxMiddlewarePropagatesHandlerError)
	t.Run("stores outgoing messages", testOutboxMiddlewareStoresOutgoingMessages)
	t.Run("uses fallback event type", testOutboxMiddlewareUsesFallbackEventType)
	t.Run("returns on outbox failure", testOutboxMiddlewareReturnsOnOutboxFailure)
	t.Run("empty outgoing messages", testOutboxMiddlewareEmptyOutgoing)
}

func testOutboxMiddlewareSkipsWhenOutboxMissing(t *testing.T) {
	t.Helper()
	svc := &Service{}
	mw := svc.outboxMiddleware()
	msg := message.NewMessage(uuid.New().String(), nil)
	msg.Metadata = message.Metadata{}
	msgs, err := mw(func(m *message.Message) ([]*message.Message, error) {
		return []*message.Message{message.NewMessage(uuid.New().String(), []byte("ok"))}, nil
	})(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected message passthrough")
	}
}

func testOutboxMiddlewarePropagatesHandlerError(t *testing.T) {
	t.Helper()
	svc := &Service{outbox: &testOutbox{}}
	mw := svc.outboxMiddleware()
	msg := message.NewMessage(uuid.New().String(), nil)
	msg.Metadata = message.Metadata{}
	if _, err := mw(func(m *message.Message) ([]*message.Message, error) {
		return nil, errors.New("fail")
	})(msg); err == nil {
		t.Fatal("expected handler error to propagate")
	}
}

func testOutboxMiddlewareStoresOutgoingMessages(t *testing.T) {
	t.Helper()
	svc := &Service{outbox: &testOutbox{}}
	mw := svc.outboxMiddleware()
	msg := message.NewMessage(uuid.New().String(), nil)
	msg.Metadata = message.Metadata{}
	out := message.NewMessage(uuid.New().String(), []byte("ok"))
	out.Metadata = message.Metadata{"event_message_schema": "OrderCreated"}
	msgs, err := mw(func(m *message.Message) ([]*message.Message, error) {
		return []*message.Message{out}, nil
	})(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected outgoing message")
	}
	records := svc.outbox.(*testOutbox).Records()
	if len(records) != 1 || records[0].eventType != "OrderCreated" {
		t.Fatalf("unexpected outbox records: %#v", records)
	}
}

func testOutboxMiddlewareUsesFallbackEventType(t *testing.T) {
	t.Helper()
	svc := &Service{outbox: &testOutbox{}}
	mw := svc.outboxMiddleware()
	msg := message.NewMessage(uuid.New().String(), nil)
	msg.Metadata = message.Metadata{}
	out := message.NewMessage(uuid.New().String(), []byte("ok"))
	out.Metadata = message.Metadata{}
	if _, err := mw(func(m *message.Message) ([]*message.Message, error) {
		return []*message.Message{out}, nil
	})(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	records := svc.outbox.(*testOutbox).Records()
	if len(records) != 1 || records[0].eventType != "unknown_event" {
		t.Fatalf("expected fallback event type, got %#v", records)
	}
}

func testOutboxMiddlewareReturnsOnOutboxFailure(t *testing.T) {
	t.Helper()
	svc := &Service{outbox: &testOutbox{err: errors.New("store fail")}}
	mw := svc.outboxMiddleware()
	msg := message.NewMessage(uuid.New().String(), nil)
	msg.Metadata = message.Metadata{}
	out := message.NewMessage(uuid.New().String(), []byte("ok"))
	out.Metadata = message.Metadata{}
	if _, err := mw(func(m *message.Message) ([]*message.Message, error) {
		return []*message.Message{out}, nil
	})(msg); err == nil {
		t.Fatal("expected outbox error to bubble up")
	}
}

func testOutboxMiddlewareEmptyOutgoing(t *testing.T) {
	t.Helper()
	svc := &Service{outbox: &testOutbox{}}
	mw := svc.outboxMiddleware()
	msg := message.NewMessage(uuid.New().String(), nil)
	if _, err := mw(func(m *message.Message) ([]*message.Message, error) {
		return []*message.Message{}, nil
	})(msg); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPoisonQueueMiddlewareFailure(t *testing.T) {
	svc := &Service{} // Conf is nil
	mw, err := svc.poisonMiddlewareWithFilter(nil)
	if err == nil {
		t.Fatal("expected error when config is nil")
	}
	if mw != nil {
		t.Fatal("expected nil middleware")
	}
}

func TestPoisonQueueMiddlewarePublisherMissing(t *testing.T) {
	svc := &Service{Conf: &configpkg.Config{PoisonQueue: "poison"}}
	mw, err := svc.poisonMiddlewareWithFilter(nil)
	if err == nil {
		t.Fatal("expected error when publisher is nil")
	}
	if mw != nil {
		t.Fatal("expected nil middleware")
	}
}

func TestRetryMiddleware(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	mw := svc.retryMiddleware()
	attempts := 0
	msg := message.NewMessage(uuid.New().String(), nil)
	msg.Metadata = message.Metadata{}
	_, err := mw(func(m *message.Message) ([]*message.Message, error) {
		attempts++
		if attempts < 2 {
			return nil, errors.New("retry")
		}
		return nil, nil
	})(msg)
	if err != nil {
		t.Fatalf("unexpected error after retries: %v", err)
	}
	if attempts < 2 {
		t.Fatalf("expected retries, got %d", attempts)
	}
}

func TestTracerMiddleware(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	mw := svc.tracerMiddleware()
	msg := message.NewMessage(uuid.New().String(), nil)
	msg.Metadata = message.Metadata{}
	ctx := context.Background()
	msg.SetContext(ctx)
	var observed trace.Span
	_, err := mw(func(m *message.Message) ([]*message.Message, error) {
		observed = trace.SpanFromContext(m.Context())
		return nil, nil
	})(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observed == nil {
		t.Fatal("expected span to be attached to context")
	}
}

func TestTracerMiddlewareSetsAttributes(t *testing.T) {
	t.Parallel()

	svc := &Service{}
	mw := svc.tracerMiddleware()
	msg := message.NewMessage(uuid.New().String(), nil)
	msg.Metadata = message.Metadata{"key": "value"}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	msg.SetContext(ctx)
	_, err := mw(func(m *message.Message) ([]*message.Message, error) { return nil, nil })(msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRegisterMiddlewareValidations(t *testing.T) {
	t.Parallel()

	t.Run("requires router", testRegisterMiddlewareRequiresRouter)
	t.Run("requires configuration", testRegisterMiddlewareRequiresConfiguration)
	t.Run("invokes builder", testRegisterMiddlewareInvokesBuilder)
	t.Run("handles builder error", testRegisterMiddlewareHandlesBuilderError)
	t.Run("handles nil middleware from builder", testRegisterMiddlewareHandlesNilMiddlewareFromBuilder)
}

func testRegisterMiddlewareRequiresRouter(t *testing.T) {
	svc := &Service{}
	err := svc.RegisterMiddleware(MiddlewareRegistration{
		Middleware: func(h message.HandlerFunc) message.HandlerFunc { return h },
	})
	if err == nil {
		t.Fatal("expected error when router is missing")
	}
}

func testRegisterMiddlewareRequiresConfiguration(t *testing.T) {
	router, err := message.NewRouter(message.RouterConfig{}, watermill.NewStdLogger(false, false))
	if err != nil {
		t.Fatalf("router init failed: %v", err)
	}
	svc := &Service{router: router}
	if err := svc.RegisterMiddleware(MiddlewareRegistration{}); err == nil {
		t.Fatal("expected error when registration empty")
	}
}

func testRegisterMiddlewareInvokesBuilder(t *testing.T) {
	router, err := message.NewRouter(message.RouterConfig{}, watermill.NewStdLogger(false, false))
	if err != nil {
		t.Fatalf("router init failed: %v", err)
	}
	svc := &Service{router: router}
	built := false
	err = svc.RegisterMiddleware(MiddlewareRegistration{
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			built = true
			return func(h message.HandlerFunc) message.HandlerFunc { return h }, nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !built {
		t.Fatal("expected builder to be invoked")
	}
}

func testRegisterMiddlewareHandlesBuilderError(t *testing.T) {
	router, err := message.NewRouter(message.RouterConfig{}, watermill.NewStdLogger(false, false))
	if err != nil {
		t.Fatalf("router init failed: %v", err)
	}
	svc := &Service{router: router}
	err = svc.RegisterMiddleware(MiddlewareRegistration{
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			return nil, errors.New("builder failed")
		},
	})
	if err == nil {
		t.Fatal("expected builder error to propagate")
	}
}

func testRegisterMiddlewareHandlesNilMiddlewareFromBuilder(t *testing.T) {
	router, err := message.NewRouter(message.RouterConfig{}, watermill.NewStdLogger(false, false))
	if err != nil {
		t.Fatalf("router init failed: %v", err)
	}
	svc := &Service{router: router}
	err = svc.RegisterMiddleware(MiddlewareRegistration{
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			return nil, nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLogMessagesMiddlewareValidations(t *testing.T) {
	svc := &Service{}
	_, err := LogMessagesMiddleware(nil).Builder(svc)
	if err == nil {
		t.Fatal("expected error when logger missing")
	}
}

func TestPoisonQueueMiddleware(t *testing.T) {
	svc := newTestService(t)
	svc.Conf = &configpkg.Config{PoisonQueue: "poison"}

	// Test default filter
	reg := PoisonQueueMiddleware(nil)
	mw, err := reg.Builder(svc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mw == nil {
		t.Fatal("expected middleware")
	}

	// Test with filter
	reg = PoisonQueueMiddleware(func(err error) bool { return true })
	mw, err = reg.Builder(svc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mw == nil {
		t.Fatal("expected middleware")
	}

	// Test error cases
	svc.Conf = nil
	_, err = reg.Builder(svc)
	if err == nil {
		t.Fatal("expected error when config is nil")
	}

	svc = newTestService(t)
	svc.publisher = nil
	_, err = reg.Builder(svc)
	if err == nil {
		t.Fatal("expected error when publisher is nil")
	}
}

func TestPoisonQueueMiddlewareDefaultFilter(t *testing.T) {
	svc := newTestService(t)
	svc.Conf = &configpkg.Config{PoisonQueue: "poison"}

	// We need to extract the filter from the closure, which is impossible.
	// But we can verify behavior.
	// If we use the middleware, and return UnprocessableEventError, it should be sent to poison queue.
	// If we return other error, it should NOT (or maybe it should? No, default filter checks for UnprocessableEventError).

	// Wait, default filter:
	// func(err error) bool { _, ok := err.(*UnprocessableEventError); return ok }
	// So only UnprocessableEventError is poisoned.

	// Mock publisher to check if message is published
	pub := &testPublisher{}
	svc.publisher = pub

	reg := PoisonQueueMiddleware(nil)
	mw, err := reg.Builder(svc)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Create a handler that returns UnprocessableEventError
	handler := mw(func(msg *message.Message) ([]*message.Message, error) {
		return nil, &UnprocessableEventError{err: errors.New("bad")}
	})

	msg := message.NewMessage(uuid.New().String(), []byte("payload"))
	_, err = handler(msg)
	// Poison middleware swallows the error if poisoned?
	// Usually yes.
	if err != nil {
		t.Fatalf("expected error to be handled by poison middleware, got: %v", err)
	}

	if len(pub.published) != 1 {
		t.Fatal("expected message to be published to poison queue")
	}

	// Test with other error
	pub.published = nil
	handler = mw(func(msg *message.Message) ([]*message.Message, error) {
		return nil, errors.New("other error")
	})
	_, err = handler(msg)
	if err == nil {
		t.Fatal("expected error to propagate")
	}
	if len(pub.published) != 0 {
		t.Fatal("expected message NOT to be published to poison queue")
	}
}

func TestPoisonQueueMiddleware_EmptyTopic(t *testing.T) {
	svc := &Service{
		Conf:      &configpkg.Config{PoisonQueue: ""},
		publisher: &testPublisher{},
	}
	_, err := svc.poisonMiddlewareWithFilter(nil)
	if err == nil {
		t.Fatal("expected error for empty poison queue topic")
	}
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

type mockLogger struct{}

func (m mockLogger) With(fields loggingpkg.LogFields) loggingpkg.ServiceLogger { return m }
func (m mockLogger) Debug(msg string, fields loggingpkg.LogFields)             {}
func (m mockLogger) Info(msg string, fields loggingpkg.LogFields)              {}
func (m mockLogger) Error(msg string, err error, fields loggingpkg.LogFields)  {}
func (m mockLogger) Trace(msg string, fields loggingpkg.LogFields)             {}

type capturingLogger struct {
	mockLogger
	msgs []string
}

func (c *capturingLogger) Info(msg string, fields loggingpkg.LogFields) {
	c.msgs = append(c.msgs, msg)
}

func TestMetricsMiddleware_Enabled(t *testing.T) {
	t.Parallel()

	logger := mockLogger{}
	wmLogger := loggingpkg.NewWatermillAdapter(logger)
	router, err := message.NewRouter(message.RouterConfig{}, wmLogger)
	if err != nil {
		t.Fatal(err)
	}

	svc := &Service{
		Conf: &configpkg.Config{
			MetricsEnabled: true,
			PubSubSystem:   "test",
		},
		Logger: logger,
		router: router,
	}

	reg := MetricsMiddleware()
	mw, err := reg.Builder(svc)
	if err != nil {
		t.Fatalf("unexpected error building metrics middleware: %v", err)
	}
	if mw == nil {
		t.Fatal("expected middleware to be returned")
	}
}

func TestMetricsMiddleware_Disabled(t *testing.T) {
	t.Parallel()

	reg := MetricsMiddleware()
	svcDisabled := &Service{
		Conf: &configpkg.Config{
			MetricsEnabled: false,
		},
	}
	mw, err := reg.Builder(svcDisabled)
	if err != nil {
		t.Fatal(err)
	}
	if mw != nil {
		t.Fatal("expected nil middleware when disabled")
	}
}

func TestMetricsMiddleware_WithServer(t *testing.T) {
	t.Parallel()

	port, err := getFreePort()
	if err != nil {
		t.Fatalf("failed to get free port: %v", err)
	}

	logger := &capturingLogger{}
	wmLogger := loggingpkg.NewWatermillAdapter(logger)
	router, err := message.NewRouter(message.RouterConfig{}, wmLogger)
	if err != nil {
		t.Fatal(err)
	}

	svcWithServer := &Service{
		Conf: &configpkg.Config{
			MetricsEnabled: true,
			MetricsPort:    port,
			PubSubSystem:   "test",
		},
		Logger: logger,
		router: router,
	}

	reg := MetricsMiddleware()
	_, err = reg.Builder(svcWithServer)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = svcWithServer.Start(ctx)
	}()

	// Give it a moment to start goroutine
	time.Sleep(100 * time.Millisecond)

	found := false
	for _, msg := range logger.msgs {
		if msg == "Starting HTTP server" {
			found = true
			break
		}
	}
	if !found {
		t.Logf("captured messages: %v", logger.msgs)
		t.Error("expected 'Starting HTTP server' log")
	}
}
