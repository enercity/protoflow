package transport

import (
	net_http "net/http"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-http/v2/pkg/http"
	"github.com/ThreeDotsLabs/watermill/message"

	"github.com/enercity/protoflow/internal/runtime/config"
)

var (
	HTTPPublisherFactory = func(config http.PublisherConfig, logger watermill.LoggerAdapter) (message.Publisher, error) {
		return http.NewPublisher(config, logger)
	}
	HTTPSubscriberFactory = func(addr string, config http.SubscriberConfig, logger watermill.LoggerAdapter) (message.Subscriber, error) {
		return http.NewSubscriber(addr, config, logger)
	}
)

func httpTransport(conf *config.Config, logger watermill.LoggerAdapter) (Transport, error) {
	publisher, err := HTTPPublisherFactory(
		http.PublisherConfig{
			MarshalMessageFunc: func(topic string, msg *message.Message) (*net_http.Request, error) {
				url := conf.HTTPPublisherURL + topic
				return http.DefaultMarshalMessageFunc(url, msg)
			},
		},
		logger,
	)
	if err != nil {
		return Transport{}, err
	}

	subscriber, err := HTTPSubscriberFactory(
		conf.HTTPServerAddress,
		http.SubscriberConfig{
			UnmarshalMessageFunc: http.DefaultUnmarshalMessageFunc,
		},
		logger,
	)
	if err != nil {
		return Transport{}, err
	}

	go func() {
		// We can't easily call StartHTTPServer on the interface message.Subscriber
		// But we know it's *http.Subscriber if we used the default factory.
		// If it's mocked, we might not be able to cast it.
		// However, for the purpose of this transport, we assume it's http.Subscriber or compatible.
		// But wait, if I mock it, I return a mock Subscriber which doesn't have StartHTTPServer.
		// So the type assertion will fail.
		// I should probably check if it implements an interface with StartHTTPServer or just ignore if it doesn't.
		if s, ok := subscriber.(*http.Subscriber); ok {
			if err := s.StartHTTPServer(); err != nil {
				logger.Error("Failed to start HTTP subscriber server", err, nil)
			}
		}
	}()

	return Transport{
		Publisher:  publisher,
		Subscriber: subscriber,
	}, nil
}
