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

const (
	globalTaskTimeout     = 10 * time.Minute
	minTaskProcessingTime = 5 * time.Second
	maxConcurrentTasks    = 3
)

type TaskContext struct {
	context.Context
	data.TaskInfo
	*livepeerAPI.Task
	*livepeerAPI.Asset
	*livepeerAPI.ObjectStore
	osSession drivers.OSSession
	lapi      *livepeerAPI.Client
}

type TaskHandler func(tctx *TaskContext) (*data.TaskOutput, error)

type Runner interface {
	Start() error
	Shutdown(ctx context.Context) error
}

type RunnerOptions struct {
	AMQPUri      string
	ExchangeName string
	QueueName    string
	TaskHandlers map[string]TaskHandler

	LivepeerAPIOptions livepeerAPI.ClientOptions
}

func NewRunner(opts RunnerOptions) Runner {
	if opts.TaskHandlers == nil {
		opts.TaskHandlers = map[string]TaskHandler{
			"import": TaskImport,
		}
	}
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
	err = amqp.Consume(r.QueueName, maxConcurrentTasks, r.handleTask)
	if err != nil {
		return fmt.Errorf("error consuming queue: %w", err)
	}

	r.amqp = amqp
	return nil
}

func (r *runner) handleTask(msg amqp.Delivery) error {
	// rate-limit task processing time to limit load
	defer blockUntil(time.After(minTaskProcessingTime))

	ctx, cancel := context.WithTimeout(context.Background(), globalTaskTimeout)
	defer cancel()
	taskCtx, err := r.buildTaskContext(ctx, msg)
	if err != nil {
		glog.Errorf("Error building task context err=%q unretriable=%v msg=%q", err, IsUnretriable(err), msg.Body)
		return NilIfUnretriable(err)
	}
	taskType, taskID := taskCtx.Task.Type, taskCtx.Task.ID

	var output *data.TaskOutput
	if handler, ok := r.TaskHandlers[strings.ToLower(taskType)]; !ok {
		err = UnretriableError{fmt.Errorf("unknown task type=%q id=%s", taskType, taskID)}
	} else {
		err = r.lapi.UpdateTaskStatus(taskID, "running", 0)
		if err != nil {
			glog.Errorf("Error updating task progress type=%q id=%s err=%q unretriable=%v", taskType, taskID, err, IsUnretriable(err))
			// execute the task anyway
		}
		glog.Infof(`Starting task type=%q id=%s assetId=%s params="%+v"`, taskType, taskID, taskCtx.Asset.ID, taskCtx.Params)
		output, err = handler(taskCtx)
	}
	glog.Infof("Task handler processed task type=%q id=%s output=%+v error=%q unretriable=%v", taskType, taskID, output, err, IsUnretriable(err))
	// return the error directly so that if publishing the result fails we nack the message to try again
	return r.publishTaskResult(ctx, taskCtx.TaskInfo, output, err)
}

func (r *runner) buildTaskContext(ctx context.Context, msg amqp.Delivery) (*TaskContext, error) {
	parsedEvt, err := data.ParseEvent(msg.Body)
	if err != nil {
		return nil, UnretriableError{fmt.Errorf("error parsing AMQP message: %w", err)}
	}
	taskEvt, ok := parsedEvt.(*data.TaskTriggerEvent)
	if evType := parsedEvt.Type(); !ok || evType != data.EventTypeTaskTrigger {
		return nil, UnretriableError{fmt.Errorf("unexpected AMQP message type=%q", evType)}
	}
	info := taskEvt.Task

	task, err := r.lapi.GetTask(info.ID)
	if err != nil {
		return nil, err
	}
	asset, err := r.lapi.GetAsset(task.ParentAssetID)
	if err != nil {
		return nil, err
	}
	objectStore, err := r.lapi.GetObjectStore(asset.ObjectStoreID)
	if err != nil {
		return nil, err
	}
	osDriver, err := drivers.ParseOSURL(objectStore.URL, true)
	if err != nil {
		return nil, UnretriableError{fmt.Errorf("error parsing object store url=%s: %w", objectStore.URL, err)}
	}
	osSession := osDriver.NewSession("")
	return &TaskContext{ctx, info, task, asset, objectStore, osSession, r.lapi}, nil
}

func (r *runner) publishTaskResult(ctx context.Context, task data.TaskInfo, output *data.TaskOutput, err error) error {
	msg := event.AMQPMessage{
		Exchange: r.ExchangeName,
		Key:      fmt.Sprintf("task.result.%s.%s", task.Type, task.ID),
		Body:     data.NewTaskResultEvent(task, errorInfo(err), output),
	}
	if err := r.amqp.Publish(ctx, msg); err != nil {
		glog.Errorf("Error sending AMQP task result event type=%q id=%s err=%q message=%+v", task.Type, task.ID, err, msg)
		return err
	}
	return nil
}

func (r *runner) Shutdown(ctx context.Context) error {
	if r.amqp == nil {
		return errors.New("runner not started")
	}
	return r.amqp.Shutdown(ctx)
}

func errorInfo(err error) *data.ErrorInfo {
	if err == nil {
		return nil
	}
	return &data.ErrorInfo{Message: err.Error(), Unretriable: IsUnretriable(err)}
}

func blockUntil(t <-chan time.Time) { <-t }
