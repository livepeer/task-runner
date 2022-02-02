package task

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-livepeer/drivers"
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
	ctx, cancel := context.WithTimeout(context.Background(), globalTaskTimeout)
	defer cancel()
	taskCtx, err := buildTaskContext(ctx, msg, s.lapi)
	if err != nil {
		glog.Errorf("Error building task context err=%q unretriable=%s msg=%q", err, IsUnretriable(err), msg.Body)
		return NilIfUnretriable(err)
	}
	taskType, taskID := taskCtx.Task.Type, taskCtx.Task.ID

	err = s.lapi.UpdateTaskProgress(taskID, "Running", 0)
	if err != nil {
		glog.Errorf("Error updating task progress type=%q id=%s err=%q unretriable=%s", taskType, taskID, err, IsUnretriable(err))
		return NilIfUnretriable(err)
	}

	var result interface{}
	switch taskType {
	case "import":
		result, err = TaskImport(taskCtx)
	default:
		glog.Errorf("Unknown task type=%q id=%s", taskType, taskID)
		return nil
	}
	// TODO: Register success or error on the API. Or rather, send an event with the task
	// completion to the API
	if err != nil {
		glog.Errorf("Error executing task type=%q id=%s err=%q unretriable=%s", taskType, taskID, err, IsUnretriable(err))
		return NilIfUnretriable(err)
	}
	glog.Infof("Task handler completed task type=%q id=%s result=%+v", taskType, taskID, result)
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
	osSession drivers.OSSession
	lapi      *livepeerAPI.Client
}

func buildTaskContext(ctx context.Context, msg amqp.Delivery, lapi *livepeerAPI.Client) (*TaskContext, error) {
	parsedEvt, err := data.ParseEvent(msg.Body)
	if err != nil {
		return nil, UnretriableError{fmt.Errorf("error parsing AMQP message: %w", err)}
	}
	taskEvt, ok := parsedEvt.(*data.TaskEvent)
	if evType := parsedEvt.Type(); !ok || evType != data.EventTypeTask {
		return nil, UnretriableError{fmt.Errorf("unexpected AMQP message type=%q", evType)}
	}
	info := taskEvt.Task

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
	osDriver, err := drivers.ParseOSURL(objectStore.URL, true)
	if err != nil {
		return nil, UnretriableError{fmt.Errorf("error parsing object store url=%s: %w", objectStore.URL, err)}
	}
	osSession := osDriver.NewSession("")
	return &TaskContext{ctx, info, task, asset, objectStore, osSession, lapi}, nil
}
