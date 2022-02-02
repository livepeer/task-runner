package task

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

	lapi *livepeerAPI.Client
	amqp event.AMQPClient
}

func (s *runner) Start() error {
	if s.amqp != nil {
		return errors.New("runner already started")
	}

	amqp, err := event.NewAMQPClient(s.AMQPUri, event.NewAMQPConnectFunc(func(c event.AMQPChanSetup) error {
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
	err = amqp.Consume(s.QueueName, 3, s.handleTask)
	if err != nil {
		return fmt.Errorf("error consuming queue: %w", err)
	}

	s.amqp = amqp
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

	var output *data.TaskOutput
	switch strings.ToLower(taskType) {
	case "import":
		var result interface{}
		result, err = TaskImport(taskCtx)
		if result != nil {
			output = &data.TaskOutput{Import: result}
		}
	default:
		glog.Errorf("Unknown task type=%q id=%s", taskType, taskID)
		return nil
	}
	resultMsg := event.AMQPMessage{
		Exchange: "todo",
		Key:      "todo",
		Body: data.NewTaskResultEvent(taskCtx.TaskInfo,
			&data.ErrorInfo{Message: err.Error(), Unretriable: IsUnretriable(err)},
			output),
	}
	if amqpErr := s.amqp.Publish(ctx, resultMsg); amqpErr != nil {
		glog.Errorf("Error sending AMQP task result event type=%q id=%s err=%q message=%+v", taskType, taskID, amqpErr, resultMsg)
		return amqpErr
	}
	glog.Infof("Task handler processed task type=%q id=%s output=%+v error=%q unretriable=%s", taskType, taskID, output, err, IsUnretriable(err))
	// If we sent the AMQP event above with the result, we always want to return
	// nil so the current message is acked from the queue. Any retries will be
	// handled by the API.
	return nil
}

func (s *runner) Shutdown(ctx context.Context) error {
	if s.amqp == nil {
		return errors.New("runner not started")
	}
	return s.amqp.Shutdown(ctx)
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
	osSession := osDriver.NewSession(asset.PlaybackID)
	return &TaskContext{ctx, info, task, asset, objectStore, osSession, lapi}, nil
}
