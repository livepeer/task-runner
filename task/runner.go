package task

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	amqp "github.com/rabbitmq/amqp091-go"
)

const globalTaskTimeout = 10 * time.Minute

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

	ctx, cancel := context.WithTimeout(context.Background(), globalTaskTimeout)
	defer cancel()
	taskCtx, err := buildTaskContext(ctx, taskEvt.Task, s.lapi)
	if err != nil {
		return nilIfUnretriable(err)
	}

	var (
		handled bool
		result  interface{}
	)
	switch taskEvt.Task.Type {
	case "import":
		handled, result, err = TaskImport(taskCtx)
	default:
		glog.Errorf("Unknown task type=%q id=%s", taskEvt.Task.Type, taskEvt.Task.ID)
		return nil
	}
	// TODO: Register success or error on the API. Or rather, send an event with the task
	// completion to the API
	if !handled || err != nil {
		if err == nil {
			glog.Errorf("Task handler failed to process task id=%s", taskEvt.Task.ID)
			err = errors.New("unknown error")
		}
		return err
	}
	glog.Infof("Task handler completed task id=%s result=%+v", taskEvt.Task.ID, result)
	return nil
}

func (s *runner) Shutdown(ctx context.Context) error {
	if s.consumer == nil {
		return errors.New("runner not started")
	}
	return s.consumer.Shutdown(ctx)
}

type TaskContext struct {
	context.Context
	data.TaskInfo
	*livepeerAPI.Task
	*livepeerAPI.Asset
	*livepeerAPI.ObjectStore
	lapi *livepeerAPI.Client
}

func buildTaskContext(ctx context.Context, info data.TaskInfo, lapi *livepeerAPI.Client) (*TaskContext, error) {
	task, err := lapi.GetTask(info.ID)
	if err != nil {
		return nil, err
	}
	asset, err := lapi.GetAsset(task.ParentAssetID)
	if err != nil {
		return nil, err
	}
	objectStore, err := lapi.GetObjectStore(asset.ObjectStoreID)
	if err != nil {
		return nil, err
	}
	return &TaskContext{ctx, info, task, asset, objectStore, lapi}, nil
}

func nilIfUnretriable(err error) error {
	if err == livepeerAPI.ErrNotExists {
		return nil
	}
	return err
}
