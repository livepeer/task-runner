package task

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"runtime/debug"
	"strings"
	"time"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-livepeer/drivers"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/livepeer/task-runner/clients"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	globalTaskTimeout     = 10 * time.Minute
	minTaskProcessingTime = 5 * time.Second
	maxConcurrentTasks    = 3
)

var defaultTasks = map[string]TaskHandler{
	"import":    TaskImport,
	"export":    TaskExport,
	"transcode": TaskTranscode,
}

type TaskHandler func(tctx *TaskContext) (*data.TaskOutput, error)

type TaskContext struct {
	context.Context
	*runner
	data.TaskInfo
	*livepeerAPI.Task
	InputAsset, OutputAsset *livepeerAPI.Asset
	inputOS, outputOS       drivers.OSSession
}

func (t *TaskContext) WithContext(ctx context.Context) *TaskContext {
	t2 := new(TaskContext)
	*t2 = *t
	t2.Context = ctx
	return t2
}

type Runner interface {
	Start() error
	Shutdown(ctx context.Context) error
}

type RunnerOptions struct {
	AMQPUri            string
	ExchangeName       string
	QueueName          string
	LivepeerAPIOptions livepeerAPI.ClientOptions

	// export task
	PinataAccessToken  string
	PlayerImmutableURL *url.URL
	PlayerExternalURL  *url.URL

	TaskHandlers map[string]TaskHandler
}

func NewRunner(opts RunnerOptions) Runner {
	if opts.TaskHandlers == nil {
		opts.TaskHandlers = defaultTasks
	}
	return &runner{
		RunnerOptions: opts,
		lapi:          livepeerAPI.NewAPIClient(opts.LivepeerAPIOptions),
		ipfs: clients.NewPinataClientJWT(opts.PinataAccessToken, map[string]string{
			"apiServer": opts.LivepeerAPIOptions.Server,
			"createdBy": clients.UserAgent,
		}),
	}
}

type runner struct {
	RunnerOptions

	lapi *livepeerAPI.Client
	ipfs clients.IPFS
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
	err = amqp.Consume(r.QueueName, maxConcurrentTasks, r.handleAMQPMessage)
	if err != nil {
		return fmt.Errorf("error consuming queue: %w", err)
	}

	r.amqp = amqp
	return nil
}

func (r *runner) handleAMQPMessage(msg amqp.Delivery) error {
	// rate-limit message processing time to limit load
	defer blockUntil(time.After(minTaskProcessingTime))

	task, err := parseTaskInfo(msg)
	if err != nil {
		glog.Errorf("Error parsing AMQP message err=%q msg=%q", err, msg.Body)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), globalTaskTimeout)
	defer cancel()
	output, err := r.handleTask(ctx, task)

	// return the error directly so that if publishing the result fails we nack the message to try again
	return r.publishTaskResult(ctx, task, output, err)
}

func (r *runner) handleTask(ctx context.Context, taskInfo data.TaskInfo) (output *data.TaskOutput, err error) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Panic handling task: value=%q stack:\n%s", r, string(debug.Stack()))
			err = UnretriableError{fmt.Errorf("panic handling task: %v", r)}
		}
	}()

	taskCtx, err := r.buildTaskContext(ctx, taskInfo)
	if err != nil {
		glog.Errorf("Error building task context taskId=%s err=%q id=%s", err, IsUnretriable(err), taskInfo.ID)
		return nil, err
	}
	taskType, taskID := taskCtx.Task.Type, taskCtx.Task.ID

	handler, ok := r.TaskHandlers[strings.ToLower(taskType)]
	if !ok {
		return nil, UnretriableError{fmt.Errorf("unknown task type=%q", taskType)}
	}

	if taskCtx.Status.Phase == "running" {
		return nil, UnretriableError{errors.New("task has already been started before")}
	}
	err = r.lapi.UpdateTaskStatus(taskID, "running", 0)
	if err != nil {
		glog.Errorf("Error updating task progress type=%q id=%s err=%q unretriable=%v", taskType, taskID, err, IsUnretriable(err))
		// execute the task anyway
	}

	glog.Infof(`Starting task type=%q id=%s inputAssetId=%s outputAssetId=%s params="%+v"`, taskType, taskID, taskCtx.InputAssetID, taskCtx.OutputAssetID, taskCtx.Params)
	output, err = handler(taskCtx)
	glog.Infof("Task handler processed task type=%q id=%s output=%+v error=%q unretriable=%v", taskType, taskID, output, err, IsUnretriable(err))
	return output, err
}

func parseTaskInfo(msg amqp.Delivery) (data.TaskInfo, error) {
	parsedEvt, err := data.ParseEvent(msg.Body)
	if err != nil {
		return data.TaskInfo{}, UnretriableError{fmt.Errorf("error parsing AMQP message: %w", err)}
	}
	taskEvt, ok := parsedEvt.(*data.TaskTriggerEvent)
	if evType := parsedEvt.Type(); !ok || evType != data.EventTypeTaskTrigger {
		return data.TaskInfo{}, UnretriableError{fmt.Errorf("unexpected AMQP message type=%q", evType)}
	}
	return taskEvt.Task, nil
}

func (r *runner) buildTaskContext(ctx context.Context, info data.TaskInfo) (*TaskContext, error) {
	task, err := r.lapi.GetTask(info.ID)
	if err != nil {
		return nil, err
	}
	inputAsset, inputOS, err := r.getAssetAndOS(task.InputAssetID)
	if err != nil {
		return nil, err
	}
	outputAsset, outputOS, err := r.getAssetAndOS(task.OutputAssetID)
	if err != nil {
		return nil, err
	}
	return &TaskContext{ctx, r, info, task, inputAsset, outputAsset, inputOS, outputOS}, nil
}

func (r *runner) getAssetAndOS(assetID string) (*livepeerAPI.Asset, drivers.OSSession, error) {
	if assetID == "" {
		return nil, nil, nil
	}
	asset, err := r.lapi.GetAsset(assetID)
	if err != nil {
		return nil, nil, err
	}
	objectStore, err := r.lapi.GetObjectStore(asset.ObjectStoreID)
	if err != nil {
		return nil, nil, err
	}
	osDriver, err := drivers.ParseOSURL(objectStore.URL, true)
	if err != nil {
		return nil, nil, UnretriableError{fmt.Errorf("error parsing object store url=%s: %w", objectStore.URL, err)}
	}
	osSession := osDriver.NewSession("")
	return asset, osSession, nil
}

func (r *runner) publishTaskResult(ctx context.Context, task data.TaskInfo, output *data.TaskOutput, err error) error {
	resultCh := make(chan event.PublishResult, 1)
	msg := event.AMQPMessage{
		Exchange:   r.ExchangeName,
		Key:        fmt.Sprintf("task.result.%s.%s", task.Type, task.ID),
		Body:       data.NewTaskResultEvent(task, errorInfo(err), output),
		ResultChan: resultCh,
	}
	if err := r.amqp.Publish(ctx, msg); err != nil {
		glog.Errorf("Error enqueueing AMQP publish of task result event taskType=%q id=%s err=%q message=%+v", task.Type, task.ID, err, msg)
		return err
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("waiting for publish confirmation of task result event: %w", ctx.Err())
	case result := <-resultCh:
		if err := result.Error; err != nil {
			glog.Errorf("Failed publishing task result AMQP event taskType=%q id=%s err=%q message=%+v", task.Type, task.ID, err, msg)
			return err
		}
		return nil
	}
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
