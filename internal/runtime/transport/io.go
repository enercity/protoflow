package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/enercity/protoflow/internal/runtime/config"
)

var (
	IOPublisherFactory = func(filePath string, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return &ioPublisher{filePath: filePath, logger: logger}, nil
	}
	IOSubscriberFactory = func(filePath string, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return &ioSubscriber{filePath: filePath, logger: logger}, nil
	}
)

func ioTransport(conf *config.Config, logger watermill.LoggerAdapter) (Transport, error) {
	filePath := conf.IOFile
	if filePath == "" {
		filePath = "messages.log"
	}

	pub, err := IOPublisherFactory(filePath, logger)
	if err != nil {
		return Transport{}, err
	}
	sub, err := IOSubscriberFactory(filePath, logger)
	if err != nil {
		return Transport{}, err
	}

	return Transport{
		Publisher:  pub,
		Subscriber: sub,
	}, nil
}

type ioPublisher struct {
	filePath string
	logger   watermill.LoggerAdapter
	mu       sync.Mutex
}

type storedMessage struct {
	UUID     string            `json:"uuid"`
	Metadata map[string]string `json:"metadata"`
	Payload  []byte            `json:"payload"`
	Topic    string            `json:"topic"`
}

func (p *ioPublisher) Publish(topic string, messages ...*message.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	f, err := os.OpenFile(p.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, msg := range messages {
		sm := storedMessage{
			UUID:     msg.UUID,
			Metadata: msg.Metadata,
			Payload:  msg.Payload,
			Topic:    topic,
		}

		b, err := json.Marshal(sm)
		if err != nil {
			return err
		}

		if _, err := f.Write(b); err != nil {
			return err
		}
		if _, err := f.WriteString("\n"); err != nil {
			return err
		}
	}
	return nil
}

func (p *ioPublisher) Close() error {
	return nil
}

type ioSubscriber struct {
	filePath string
	logger   watermill.LoggerAdapter
}

func (s *ioSubscriber) Subscribe(ctx context.Context, topic string) (<-chan *message.Message, error) {
	out := make(chan *message.Message)

	go func() {
		defer close(out)

		f, err := os.OpenFile(s.filePath, os.O_RDONLY|os.O_CREATE, 0600)
		if err != nil {
			s.logger.Error("Failed to open file", err, nil)
			return
		}
		defer f.Close()

		reader := bufio.NewReader(f)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				line, err := reader.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						reader.Reset(f)
						time.Sleep(100 * time.Millisecond)
						continue
					}
					s.logger.Error("Failed to read file", err, nil)
					return
				}

				var sm storedMessage
				if err := json.Unmarshal(line, &sm); err != nil {
					s.logger.Error("Failed to unmarshal message", err, nil)
					continue
				}

				if sm.Topic != topic {
					continue
				}

				msg := message.NewMessage(sm.UUID, sm.Payload)
				msg.Metadata = sm.Metadata

				select {
				case out <- msg:
					select {
					case <-msg.Acked():
						// good
					case <-msg.Nacked():
						s.logger.Info("Message nacked", watermill.LogFields{"uuid": msg.UUID})
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out, nil
}

func (s *ioSubscriber) Close() error {
	return nil
}
