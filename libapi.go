package protoflow

import (
	"github.com/google/uuid"

	runtimepkg "github.com/enercity/protoflow/internal/runtime"
	configpkg "github.com/enercity/protoflow/internal/runtime/config"
	errspkg "github.com/enercity/protoflow/internal/runtime/errors"
	handlerpkg "github.com/enercity/protoflow/internal/runtime/handlers"
	jsoncodec "github.com/enercity/protoflow/internal/runtime/jsoncodec"
	loggingpkg "github.com/enercity/protoflow/internal/runtime/logging"
	metadatapkg "github.com/enercity/protoflow/internal/runtime/metadata"
	transportpkg "github.com/enercity/protoflow/internal/runtime/transport"
	"google.golang.org/protobuf/proto"
)

type (
	Config              = configpkg.Config
	Service             = runtimepkg.Service
	ServiceDependencies = runtimepkg.ServiceDependencies
	ProtoValidator      = runtimepkg.ProtoValidator
	OutboxStore         = runtimepkg.OutboxStore
	Transport           = transportpkg.Transport
	TransportFactory    = transportpkg.Factory

	MessageHandlerRegistration                = runtimepkg.MessageHandlerRegistration
	JSONHandlerRegistration[T any, O any]     = handlerpkg.JSONHandlerRegistration[T, O]
	JSONMessageContext[T any]                 = handlerpkg.JSONMessageContext[T]
	JSONMessageOutput[T any]                  = handlerpkg.JSONMessageOutput[T]
	JSONMessageHandler[T any, O any]          = handlerpkg.JSONMessageHandler[T, O]
	ProtoHandlerRegistration[T proto.Message] = handlerpkg.ProtoHandlerRegistration[T]
	ProtoHandlerOption                        = handlerpkg.ProtoHandlerOption
	ProtoMessageContext[T proto.Message]      = handlerpkg.ProtoMessageContext[T]
	ProtoMessageOutput                        = handlerpkg.ProtoMessageOutput
	ProtoMessageHandler[T proto.Message]      = handlerpkg.ProtoMessageHandler[T]

	MiddlewareBuilder      = runtimepkg.MiddlewareBuilder
	MiddlewareRegistration = runtimepkg.MiddlewareRegistration
	RetryMiddlewareConfig  = runtimepkg.RetryMiddlewareConfig

	Producer = runtimepkg.Producer

	Metadata = metadatapkg.Metadata

	LogFields                 = loggingpkg.LogFields
	ServiceLogger             = loggingpkg.ServiceLogger
	EntryLogger               = loggingpkg.EntryLogger
	EntryLoggerAdapter[T any] = loggingpkg.EntryLoggerAdapter[T]

	UnprocessableEventError = runtimepkg.UnprocessableEventError

	HandlerInfo  = runtimepkg.HandlerInfo
	HandlerStats = runtimepkg.HandlerStats
)

var (
	NewService = runtimepkg.NewService

	RegisterMessageHandler  = runtimepkg.RegisterMessageHandler
	WithPublishMessageTypes = handlerpkg.WithPublishMessageTypes

	DefaultMiddlewares      = runtimepkg.DefaultMiddlewares
	CorrelationIDMiddleware = runtimepkg.CorrelationIDMiddleware
	LogMessagesMiddleware   = runtimepkg.LogMessagesMiddleware
	ProtoValidateMiddleware = runtimepkg.ProtoValidateMiddleware
	OutboxMiddleware        = runtimepkg.OutboxMiddleware
	TracerMiddleware        = runtimepkg.TracerMiddleware
	MetricsMiddleware       = runtimepkg.MetricsMiddleware
	RetryMiddleware         = runtimepkg.RetryMiddleware
	PoisonQueueMiddleware   = runtimepkg.PoisonQueueMiddleware
	RecovererMiddleware     = runtimepkg.RecovererMiddleware

	Marshal       = jsoncodec.Marshal
	MarshalIndent = jsoncodec.MarshalIndent
	Unmarshal     = jsoncodec.Unmarshal
	Encode        = jsoncodec.Encode
	Decode        = jsoncodec.Decode

	ErrServiceRequired             = errspkg.ErrServiceRequired
	ErrHandlerRequired             = errspkg.ErrHandlerRequired
	ErrConsumeQueueRequired        = errspkg.ErrConsumeQueueRequired
	ErrHandlerNameRequired         = errspkg.ErrHandlerNameRequired
	ErrConsumeMessageTypeRequired  = errspkg.ErrConsumeMessageTypeRequired
	ErrConsumeMessagePointerNeeded = errspkg.ErrConsumeMessagePointerNeeded
	ErrPublisherRequired           = errspkg.ErrPublisherRequired
	ErrTopicRequired               = errspkg.ErrTopicRequired

	NewSlogServiceLogger = loggingpkg.NewSlogServiceLogger

	NewMetadata = metadatapkg.New
)

// CreateUUID returns a new random UUID v4 string.
func CreateUUID() string {
	return uuid.New().String()
}

func RegisterJSONHandler[T any, O any](svc *Service, cfg JSONHandlerRegistration[T, O]) error {
	return runtimepkg.RegisterJSONHandler[T, O](svc, cfg)
}

func RegisterProtoHandler[T proto.Message](svc *Service, cfg ProtoHandlerRegistration[T]) error {
	return runtimepkg.RegisterProtoHandler[T](svc, cfg)
}

func NewProtoMessage[T proto.Message]() (T, error) {
	return runtimepkg.NewProtoMessage[T]()
}

func MustProtoMessage[T proto.Message]() T {
	return runtimepkg.MustProtoMessage[T]()
}

func NewEntryServiceLogger[T EntryLoggerAdapter[T]](entry T) ServiceLogger {
	return loggingpkg.NewEntryServiceLogger(entry)
}
