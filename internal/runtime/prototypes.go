package runtime

import (
	handlerpkg "github.com/enercity/protoflow/internal/runtime/handlers"
	"google.golang.org/protobuf/proto"
)

// NewProtoMessage instantiates a zero-value protobuf message for the provided generic type.
func NewProtoMessage[T proto.Message]() (T, error) {
	var zero T
	return handlerpkg.EnsureProtoPrototype(zero)
}

// MustProtoMessage instantiates the protobuf message and panics if the type cannot be created.
func MustProtoMessage[T proto.Message]() T {
	msg, err := NewProtoMessage[T]()
	if err != nil {
		panic(err)
	}
	return msg
}
