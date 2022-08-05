package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-tools/drivers"
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
	"upload":    TaskUpload,
	"export":    TaskExport,
	"transcode": TaskTranscode,
}

type TaskHandler func(tctx *TaskContext) (*data.TaskOutput, error)

type TaskContext struct {
	context.Context
	*runner
	data.TaskInfo
	*api.Task
	InputAsset, OutputAsset *api.Asset
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
	HandleCatalysis(ctx context.Context, taskId, nextStep string, callback *clients.CatalystCallback) error
	Shutdown(ctx context.Context) error
}

type RunnerOptions struct {
	AMQPUri            string
	ExchangeName       string
	QueueName          string
	LivepeerAPIOptions api.ClientOptions
	ExportTaskConfig

	TaskHandlers map[string]TaskHandler
}

func NewRunner(opts RunnerOptions) Runner {
	if opts.TaskHandlers == nil {
		opts.TaskHandlers = defaultTasks
	}
	return &runner{
		RunnerOptions: opts,
		lapi:          api.NewAPIClient(opts.LivepeerAPIOptions),
		ipfs: clients.NewPinataClientJWT(opts.PinataAccessToken, map[string]string{
			"apiServer": opts.LivepeerAPIOptions.Server,
			"createdBy": clients.UserAgent,
		}),
	}
}

type runner struct {
	RunnerOptions

	lapi *api.Client
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
	glog.Infof("Task handler processed task type=%q id=%s output=%+v error=%q unretriable=%v", task.Type, task.ID, output, err, IsUnretriable(err))

	if output == nil {
		// If the task doesn't output anything it means it's yielding execution
		// until another event is received about it. Likely from a different step
		// triggered by an external callback (e.g. catalyst's VOD upload callback).
		return nil
	}

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	// return the error directly so that if publishing the result fails we nack the message to try again
	return r.publishTaskResult(ctx, task.ID, output, err)
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
		return nil, fmt.Errorf("error building task context: %w", err)
	}
	taskType, taskID := taskCtx.Task.Type, taskCtx.Task.ID

	handler, ok := r.TaskHandlers[strings.ToLower(taskType)]
	if !ok {
		return nil, UnretriableError{fmt.Errorf("unknown task type=%q", taskType)}
	}

	if taskCtx.Status.Phase == "running" && taskCtx.Step == "" {
		return nil, UnretriableError{errors.New("task has already been started before")}
	}
	err = r.lapi.UpdateTaskStatus(taskID, "running", 0)
	if err != nil {
		glog.Errorf("Error updating task progress type=%q id=%s err=%q unretriable=%v", taskType, taskID, err, IsUnretriable(err))
		// execute the task anyway
	}

	glog.Infof(`Starting task type=%q id=%s inputAssetId=%s outputAssetId=%s params="%+v"`, taskType, taskID, taskCtx.InputAssetID, taskCtx.OutputAssetID, taskCtx.Params)
	output, err = handler(taskCtx)
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

func (r *runner) getAssetAndOS(assetID string) (*api.Asset, drivers.OSSession, error) {
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

func (r *runner) HandleCatalysis(ctx context.Context, taskId, nextStep string, callback *clients.CatalystCallback) error {
	task, err := r.lapi.GetTask(taskId)
	if err != nil {
		return fmt.Errorf("failed to get task %s: %w", taskId, err)
	}
	if callback.Status == "error" {
		return r.publishTaskResult(ctx, taskId, nil, errors.New(callback.Error))
	} else if callback.Status == "completed" {
		return r.scheduleTaskStep(ctx, taskId, nextStep)
	}
	progress := 0.95 * callback.CompletionRatio
	err = r.lapi.UpdateTaskStatus(task.ID, "running", progress)
	if err != nil {
		return fmt.Errorf("failed to update task %s status: %w", taskId, err)
	}
	return nil
}

func (r *runner) scheduleTaskStep(ctx context.Context, taskID, step string) error {
	if step == "" {
		return errors.New("can only schedule sub-steps of tasks")
	}
	task, err := r.getTaskInfo(taskID, step)
	if err != nil {
		return err
	}
	return r.publishSafe(ctx, event.AMQPMessage{
		Exchange:   r.ExchangeName,
		Key:        fmt.Sprintf("task.trigger.%s", task.Type),
		Persistent: true,
		Body:       data.NewTaskTriggerEvent(*task),
	})
}

func (r *runner) publishTaskResult(ctx context.Context, taskID string, output *data.TaskOutput, err error) error {
	task, err := r.getTaskInfo(taskID, "")
	if err != nil {
		return err
	}
	msg := event.AMQPMessage{
		Exchange:   r.ExchangeName,
		Key:        fmt.Sprintf("task.result.%s.%s", task.Type, task.ID),
		Persistent: true,
		Body:       data.NewTaskResultEvent(*task, errorInfo(err), output),
	}
	if err := r.publishSafe(ctx, msg); err != nil {
		glog.Errorf("Error enqueueing AMQP publish of task result event taskType=%q id=%s err=%q message=%+v", task.Type, task.ID, err, msg)
		return fmt.Errorf("error publishing task result event: %w", err)
	}
	return nil
}

func (r *runner) getTaskInfo(id, step string) (*data.TaskInfo, error) {
	task, err := r.lapi.GetTask(id)
	if err != nil {
		return nil, fmt.Errorf("error getting task %q: %w", id, err)
	}
	snapshot, err := json.Marshal(task)
	if err != nil {
		return nil, fmt.Errorf("error marshalling task %q: %w", id, err)
	}
	return &data.TaskInfo{
		ID:       id,
		Type:     task.Type,
		Snapshot: snapshot,
		Step:     step,
	}, nil
}

func (r *runner) publishSafe(ctx context.Context, msg event.AMQPMessage) error {
	resultCh := make(chan event.PublishResult, 1)
	msg.ResultChan = resultCh
	if err := r.amqp.Publish(ctx, msg); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return fmt.Errorf("waiting for publish confirmation: %w", ctx.Err())
	case result := <-resultCh:
		return result.Error
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
