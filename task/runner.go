package task

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Runner interface {
	Start() error
	Shutdown(ctx context.Context) error
}

type RunnerOptions struct {
	AMQPUri            string
	APIExchangeName    string
	QueueName          string
	LivepeerAPIOptions livepeerAPI.ClientOptions
}

func NewRunner(opts RunnerOptions) Runner {
	lapi := livepeerAPI.NewAPIClient(opts.LivepeerAPIOptions)
	return &runner{
		RunnerOptions: opts,
		lapi:          lapi,
	}
}

type runner struct {
	RunnerOptions

	lapi     *livepeerAPI.Client
	consumer event.AMQPConsumer
}

func (s *runner) Start() error {
	if s.consumer != nil {
		return errors.New("runner already started")
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

func (s *runner) handleTask(msg amqp.Delivery) error {
	glog.Infof("Received task: %s", msg.Body)
	parsedEvt, err := data.ParseEvent(msg.Body)
	if err != nil {
		glog.Errorf("Error parsing AMQP message: %w", err)
		// Return nil err so the event is acked. We shouldn't retry malformed messages.
		return nil
	}
	taskEvt, ok := parsedEvt.(*data.TaskEvent)
	if evType := parsedEvt.Type(); !ok || evType != data.EventTypeTask {
		glog.Errorf("Unexpected AMQP message type=%q", evType)
		return nil
	}
	switch taskEvt.Task.Type {
	case "import":
		return TaskImport(taskEvt.Task, s.lapi)
	default:
		glog.Errorf("Unknown task type=%q id=%s", taskEvt.Task.Type, taskEvt.Task.ID)
		return nil
	}
}

func (s *runner) Shutdown(ctx context.Context) error {
	if s.consumer == nil {
		return errors.New("runner not started")
	}
	return s.consumer.Shutdown(ctx)
}
