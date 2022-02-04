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
	AMQPUri      string
	ExchangeName string
	QueueName    string

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

func (r *runner) Start() error {
	if r.amqp != nil {
		return errors.New("runner already started")
	}

	amqp, err := event.NewAMQPClient(r.AMQPUri, event.NewAMQPConnectFunc(func(c event.AMQPChanSetup) error {
		err := c.ExchangeDeclare(r.ExchangeName, "topic", true, false, false, false, nil)
		if err != nil {
			return fmt.Errorf("error ensuring API exchange exists: %w", err)
		}
		_, err = c.QueueDeclare(r.QueueName, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
		if err != nil {
			return fmt.Errorf("error declaring task queue: %w", err)
		}
		err = c.QueueBind(r.QueueName, "task.trigger.#", r.ExchangeName, false, nil)
		if err != nil {
			return fmt.Errorf("error binding task queue: %w", err)
		}
		return nil
	}))
	if err != nil {
		return fmt.Errorf("error creating AMQP consumer: %w", err)
	}
	err = amqp.Consume(r.QueueName, 3, r.handleTask)
	if err != nil {
		return fmt.Errorf("error consuming queue: %w", err)
	}

	r.amqp = amqp
	return nil
}

func (r *runner) handleTask(msg amqp.Delivery) error {
	ctx, cancel := context.WithTimeout(context.Background(), globalTaskTimeout)
	defer cancel()
	taskCtx, err := buildTaskContext(ctx, msg, r.lapi)
	if err != nil {
		glog.Errorf("Error building task context err=%q unretriable=%v msg=%q", err, IsUnretriable(err), msg.Body)
		return NilIfUnretriable(err)
	}
	taskType, taskID := taskCtx.Task.Type, taskCtx.Task.ID

	err = r.lapi.UpdateTaskStatus(taskID, "running", 0)
	if err != nil {
		glog.Errorf("Error updating task progress type=%q id=%s err=%q unretriable=%v", taskType, taskID, err, IsUnretriable(err))
		return NilIfUnretriable(err)
	}

	glog.V(10).Infof(`Starting task type=%s playbackId=%s params="%+v"`, taskType, taskCtx.PlaybackID, taskCtx.Params)
	var output *data.TaskOutput
	switch strings.ToLower(taskType) {
	case "import":
		var ito *data.ImportTaskOutput
		ito, err = TaskImport(taskCtx)
		if ito != nil {
			output = &data.TaskOutput{Import: ito}
		}
	default:
		glog.Errorf("Unknown task type=%q id=%s", taskType, taskID)
		return nil
	}
	resultMsg := event.AMQPMessage{
		Exchange: r.ExchangeName,
		Key:      fmt.Sprintf("task.result.%s.%s", taskType, taskID),
		Body:     data.NewTaskResultEvent(taskCtx.TaskInfo, errorInfo(err), output),
	}
	if amqpErr := r.amqp.Publish(ctx, resultMsg); amqpErr != nil {
		glog.Errorf("Error sending AMQP task result event type=%q id=%s err=%q message=%+v", taskType, taskID, amqpErr, resultMsg)
		return amqpErr
	}
	glog.Infof("Task handler processed task type=%q id=%s output=%+v error=%q unretriable=%v", taskType, taskID, output, err, IsUnretriable(err))
	// Since we sent the AMQP task result event above we always want to return a
	// nil error here. That is so the current message is acked from the queue as
	// per the AMQPConsumer interface. Any retries will be handled by the API.
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
}

func buildTaskContext(ctx context.Context, msg amqp.Delivery, lapi *livepeerAPI.Client) (*TaskContext, error) {
	parsedEvt, err := data.ParseEvent(msg.Body)
	if err != nil {
		return nil, UnretriableError{fmt.Errorf("error parsing AMQP message: %w", err)}
	}
	taskEvt, ok := parsedEvt.(*data.TaskTriggerEvent)
	if evType := parsedEvt.Type(); !ok || evType != data.EventTypeTaskTrigger {
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
	return &TaskContext{ctx, info, task, asset, objectStore, osSession}, nil
}

func errorInfo(err error) *data.ErrorInfo {
	if err == nil {
		return nil
	}
	return &data.ErrorInfo{Message: err.Error(), Unretriable: IsUnretriable(err)}
}
