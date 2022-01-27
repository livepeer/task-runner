package task

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Subscriber interface {
	Start() error
	Shutdown(ctx context.Context) error
}

type SubscriberOptions struct {
	AMQPUri         string
	APIExchangeName string
	QueueName       string
}

func NewSubscriber(opts SubscriberOptions) Subscriber {
	return &subscriber{
		SubscriberOptions: opts,
	}
}

type subscriber struct {
	SubscriberOptions

	consumer event.AMQPConsumer
}

func (s *subscriber) Start() error {
	if s.consumer != nil {
		return errors.New("subscriber already started")
	}

	consumer, err := event.NewAMQPConsumer(s.AMQPUri, event.NewAMQPConnectFunc(func(c event.AMQPChanSetup) error {
		err := c.ExchangeDeclarePassive(s.APIExchangeName, "topic", true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("error ensuring API exchange exists: %w", err)
		}
		_, err = c.QueueDeclare(s.QueueName, true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("error declaring task queue: %w", err)
		}
		err = c.QueueBind(s.QueueName, "task.#", s.APIExchangeName, false, nil)
		if err != nil {
			return fmt.Errorf("error binding task queue: %w", err)
		}
		return nil
	}))
	if err != nil {
		return fmt.Errorf("error creating AMQP consumer: %w", err)
	}
	err = consumer.Consume(s.QueueName, 3, s.handleTask)
	if err != nil {
		return fmt.Errorf("error consuming queue: %w", err)
	}

	s.consumer = consumer
	return nil
}

func (s *subscriber) handleTask(msg amqp.Delivery) error {
	glog.Infof("Received task: %s", msg.Body)
	return nil
}

func (s *subscriber) Shutdown(ctx context.Context) error {
	if s.consumer == nil {
		return errors.New("subscriber not started")
	}
	return s.consumer.Shutdown(ctx)
}
