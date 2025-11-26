package runtime

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-aws/sns"
	"github.com/ThreeDotsLabs/watermill-aws/sqs"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	configpkg "github.com/enercity/protoflow/internal/runtime/config"
	loggingpkg "github.com/enercity/protoflow/internal/runtime/logging"
	transportpkg "github.com/enercity/protoflow/internal/runtime/transport"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func newTestSlogLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func newTestLogger() loggingpkg.ServiceLogger {
	return loggingpkg.NewSlogServiceLogger(newTestSlogLogger())
}

func TestNewServiceConfiguresChannel(t *testing.T) {
	cfg := &configpkg.Config{
		PubSubSystem: "channel",
		PoisonQueue:  "poison",
	}
	logger := newTestLogger()
	svc := NewService(cfg, logger, context.Background(), ServiceDependencies{})

	if svc.publisher == nil {
		t.Fatal("expected channel publisher to be assigned")
	}
	if svc.subscriber == nil {
		t.Fatal("expected channel subscriber to be assigned")
	}
	if svc.Conf != cfg {
		t.Fatal("service config not set")
	}
	if svc.router == nil {
		t.Fatal("router should not be nil")
	}
}

func TestNewService_MiddlewareBuilderError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	cfg := &configpkg.Config{PubSubSystem: "channel"}
	logger := newTestLogger()

	badMiddleware := MiddlewareRegistration{
		Name: "bad",
		Builder: func(s *Service) (message.HandlerMiddleware, error) {
			return nil, errors.New("boom")
		},
	}

	NewService(cfg, logger, context.Background(), ServiceDependencies{
		Middlewares: []MiddlewareRegistration{badMiddleware},
	})
}

func TestNewServiceConfiguresAWS(t *testing.T) {

	origLoader := transportpkg.AWSDefaultConfigLoader
	origTopic := transportpkg.SNSTopicResolverFactory
	origPub := transportpkg.SNSPublisherFactory
	origSub := transportpkg.SNSSubscriberFactory
	t.Cleanup(func() {
		transportpkg.AWSDefaultConfigLoader = origLoader
		transportpkg.SNSTopicResolverFactory = origTopic
		transportpkg.SNSPublisherFactory = origPub
		transportpkg.SNSSubscriberFactory = origSub
	})

	transportpkg.AWSDefaultConfigLoader = func(ctx context.Context, optFns ...func(*awsconfig.LoadOptions) error) (aws.Config, error) {
		return aws.Config{Region: "initial"}, nil
	}

	pub := &testPublisher{}
	sub := &testSubscriber{}
	transportpkg.SNSTopicResolverFactory = func(accountID, region string) (*sns.GenerateArnTopicResolver, error) {
		if accountID != "123456789012" {
			t.Fatalf("unexpected account id: %s", accountID)
		}
		return origTopic(accountID, region)
	}
	transportpkg.SNSPublisherFactory = func(cfg sns.PublisherConfig, _ watermill.LoggerAdapter) (message.Publisher, error) {
		return pub, nil
	}
	transportpkg.SNSSubscriberFactory = func(cfg sns.SubscriberConfig, sqsCfg sqs.SubscriberConfig, _ watermill.LoggerAdapter) (message.Subscriber, error) {
		if sqsCfg.AWSConfig.Region != "eu-west-1" {
			t.Fatalf("unexpected sqs region %s", sqsCfg.AWSConfig.Region)
		}
		return sub, nil
	}

	cfg := &configpkg.Config{
		PubSubSystem: "aws",
		AWSRegion:    "eu-west-1",
		AWSAccountID: "123456789012",
		AWSEndpoint:  "http://localhost:4566",
		PoisonQueue:  "poison",
	}
	svc := NewService(cfg, newTestLogger(), context.Background(), ServiceDependencies{})

	if svc.publisher != pub {
		t.Fatalf("expected aws publisher assignment")
	}
	if svc.subscriber != sub {
		t.Fatalf("expected aws subscriber assignment")
	}
}

func TestNewServicePanicsWhenFactoryFails(t *testing.T) {
	logger := newTestLogger()
	deps := ServiceDependencies{
		TransportFactory:          failingTransportFactory{err: errors.New("boom")},
		DisableDefaultMiddlewares: true,
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when transport factory fails")
		}
	}()
	NewService(&configpkg.Config{}, logger, context.Background(), deps)
}

func TestNewServicePanicsWhenRouterFails(t *testing.T) {
	// This is hard to test because message.NewRouter only fails if logger is nil or config is invalid,
	// but we control those. However, we can simulate a panic in middleware registration.
	logger := newTestLogger()
	deps := ServiceDependencies{
		DisableDefaultMiddlewares: true,
		Middlewares: []MiddlewareRegistration{
			{
				Name: "failing",
				Builder: func(s *Service) (message.HandlerMiddleware, error) {
					return nil, errors.New("middleware fail")
				},
			},
		},
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when middleware registration fails")
		}
	}()
	NewService(&configpkg.Config{PubSubSystem: "channel"}, logger, context.Background(), deps)
}

func TestMustProtoMessagePanicsOnError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when proto message creation fails")
		}
	}()
	// We use proto.Message interface which will result in a nil type when instantiated as zero value,
	// causing EnsureProtoPrototype to fail.
	MustProtoMessage[proto.Message]()
}

func TestNewServiceExposesProvidedLogger(t *testing.T) {
	pub := &testPublisher{}
	sub := &testSubscriber{}
	logger := newTestLogger()
	svc := NewService(&configpkg.Config{PubSubSystem: "custom"}, logger, context.Background(), ServiceDependencies{
		TransportFactory:          failingTransportFactory{transport: transportpkg.Transport{Publisher: pub, Subscriber: sub}},
		DisableDefaultMiddlewares: true,
	})

	if svc.Logger != logger {
		t.Fatal("expected service to expose provided logger")
	}
	if svc.publisher != pub || svc.subscriber != sub {
		t.Fatal("expected transport components to be assigned")
	}
}

func TestNewServiceUnsupportedPubSubPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for unsupported pubsub system")
		}
	}()

	NewService(&configpkg.Config{PubSubSystem: "gcp"}, newTestLogger(), context.Background(), ServiceDependencies{})
}

func TestServiceStartReturnsWhenContextCancelled(t *testing.T) {

	origRun := routerRun
	defer func() { routerRun = origRun }()
	called := make(chan struct{}, 1)
	routerRun = func(_ *message.Router, runCtx context.Context) error {
		called <- struct{}{}
		<-runCtx.Done()
		return runCtx.Err()
	}
	svc := &Service{
		router: nil,
		Conf:   &configpkg.Config{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- svc.Start(ctx)
	}()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("routerRun override not invoked")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("service start did not return after context cancellation")
	}
}

func TestServiceStart(t *testing.T) {
	svc := newTestService(t)

	called := false
	originalRouterRun := routerRun
	defer func() { routerRun = originalRouterRun }()

	routerRun = func(router *message.Router, ctx context.Context) error {
		called = true
		return nil
	}

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !called {
		t.Fatal("expected routerRun to be called")
	}
}

func TestRegisterHandlerValidations(t *testing.T) {

	t.Run("missing handler", testRegisterHandlerValidationsMissingHandler)
	t.Run("missing queue", testRegisterHandlerValidationsMissingQueue)
	t.Run("missing name", testRegisterHandlerValidationsMissingName)
	t.Run("autoname from proto", testRegisterHandlerValidationsAutonameFromProto)
	t.Run("explicit name", testRegisterHandlerValidationsExplicitName)
}

func testRegisterHandlerValidationsMissingHandler(t *testing.T) {
	t.Helper()
	svc := newTestService(t)
	if err := svc.registerHandler(handlerRegistration{ConsumeQueue: "queue"}); err == nil {
		t.Fatal("expected error when handler nil")
	}
}

func testRegisterHandlerValidationsMissingQueue(t *testing.T) {
	t.Helper()
	svc := newTestService(t)
	err := svc.registerHandler(handlerRegistration{Handler: func(msg *message.Message) ([]*message.Message, error) {
		return nil, nil
	}})
	if err == nil {
		t.Fatal("expected error when queue missing")
	}
}

func testRegisterHandlerValidationsMissingName(t *testing.T) {
	t.Helper()
	svc := newTestService(t)
	if err := svc.registerHandler(handlerRegistration{
		ConsumeQueue: "queue",
		Handler: func(msg *message.Message) ([]*message.Message, error) {
			return nil, nil
		},
	}); err == nil {
		t.Fatal("expected error when name missing")
	}
}

func testRegisterHandlerValidationsAutonameFromProto(t *testing.T) {
	t.Helper()
	svc := newTestService(t)
	msg := &structpb.Struct{}
	if err := svc.registerHandler(handlerRegistration{
		ConsumeQueue:       "queue",
		Handler:            func(msg *message.Message) ([]*message.Message, error) { return nil, nil },
		consumeMessageType: msg,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := svc.protoRegistry["*structpb.Struct"]; !ok {
		t.Fatalf("message prototype not registered")
	}
	handlers := svc.router.Handlers()
	if _, ok := handlers["*structpb.Struct-Handler"]; !ok {
		t.Fatalf("handler not registered with generated name")
	}
}

func testRegisterHandlerValidationsExplicitName(t *testing.T) {
	t.Helper()
	svc := newTestService(t)
	if err := svc.registerHandler(handlerRegistration{
		Name:         "custom",
		ConsumeQueue: "queue",
		Handler:      func(msg *message.Message) ([]*message.Message, error) { return nil, nil },
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := svc.router.Handlers()["custom"]; !ok {
		t.Fatalf("handler not registered with explicit name")
	}
}

func TestRegisterProtoMessageAndCloning(t *testing.T) {

	svc := &Service{protoRegistry: make(map[string]func() proto.Message)}
	m := &structpb.Struct{}
	svc.RegisterProtoMessage(m)
	factory, ok := svc.protoRegistry["*structpb.Struct"]
	if !ok {
		t.Fatalf("prototype not stored")
	}
	first := factory()
	second := factory()
	if first == second {
		t.Fatalf("expected distinct clones")
	}
}

func TestUnprocessableEventError(t *testing.T) {

	err := &UnprocessableEventError{eventMessage: "payload", err: errors.New("invalid")}
	if got := err.Error(); got != "unprocessable event: payload error: invalid" {
		t.Fatalf("unexpected error string: %s", got)
	}
}

type failingTransportFactory struct {
	transport transportpkg.Transport
	err       error
}

func (f failingTransportFactory) Build(ctx context.Context, conf *configpkg.Config, logger watermill.LoggerAdapter) (transportpkg.Transport, error) {
	if f.err != nil {
		return transportpkg.Transport{}, f.err
	}
	return f.transport, nil
}

type mockTransportFactory struct{}

func (m *mockTransportFactory) Build(ctx context.Context, conf *configpkg.Config, logger watermill.LoggerAdapter) (transportpkg.Transport, error) {
	return transportpkg.Transport{
		Publisher:  &testPublisher{},
		Subscriber: &testSubscriber{},
	}, nil
}

func TestNewServiceRegistersMiddlewares(t *testing.T) {
	logger := newTestLogger()
	mwCalled := false
	deps := ServiceDependencies{
		TransportFactory: failingTransportFactory{transport: transportpkg.Transport{
			Publisher:  &testPublisher{},
			Subscriber: &testSubscriber{},
		}},
		Middlewares: []MiddlewareRegistration{
			{
				Name: "custom",
				Builder: func(s *Service) (message.HandlerMiddleware, error) {
					mwCalled = true
					return func(h message.HandlerFunc) message.HandlerFunc {
						return h
					}, nil
				},
			},
		},
	}
	NewService(&configpkg.Config{PoisonQueue: "poison"}, logger, context.Background(), deps)
	if !mwCalled {
		t.Fatal("expected custom middleware builder to be called")
	}
}

func TestNewService_MiddlewarePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	NewService(&configpkg.Config{}, newTestLogger(), context.Background(), ServiceDependencies{
		Middlewares: []MiddlewareRegistration{{Name: "bad", Builder: nil}},
	})
}

func TestNewService_AnonymousMiddlewarePanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	NewService(&configpkg.Config{}, newTestLogger(), context.Background(), ServiceDependencies{
		Middlewares: []MiddlewareRegistration{{Builder: nil}},
	})
}

func TestNewService_DisableDefaultMiddlewares(t *testing.T) {
	NewService(&configpkg.Config{}, newTestLogger(), context.Background(), ServiceDependencies{
		DisableDefaultMiddlewares: true,
		TransportFactory:          &mockTransportFactory{},
	})
}
