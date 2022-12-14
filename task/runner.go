package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	DefaultMaxTaskProcessingTime = 10 * time.Minute
	DefaultMinTaskProcessingTime = 5 * time.Second
	DefaultMaxConcurrentTasks    = 3
	taskPublishTimeout           = 1 * time.Minute
)

var (
	defaultTasks = map[string]TaskHandler{
		"import":    TaskImport,
		"upload":    TaskUpload,
		"export":    TaskExport,
		"transcode": TaskTranscode,
	}
	errInternalProcessingError = errors.New("internal error processing file")
	taskFatalErrorInfo         = &data.ErrorInfo{Message: errInternalProcessingError.Error(), Unretriable: true}
)

type TaskHandlerOutput struct {
	*data.TaskOutput
	Continue bool
}

// If this special output is returned it means the task is yielding execution
// until another async event is received about it. Likely triggered by an
// external callback (e.g. catalyst's VOD upload callback) or delayed event.
var ContinueAsync = &TaskHandlerOutput{Continue: true}

type TaskHandler func(tctx *TaskContext) (*TaskHandlerOutput, error)

type TaskContext struct {
	context.Context
	*runner
	data.TaskInfo
	*api.Task
	Progress                *ProgressReporter
	InputAsset, OutputAsset *api.Asset
	InputOSObj, OutputOSObj *api.ObjectStore
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
	HandleCatalysis(ctx context.Context, taskId, nextStep, attemptID string, callback *clients.CatalystCallback) error
	Shutdown(ctx context.Context) error
}

type RunnerOptions struct {
	AMQPUri                 string
	ExchangeName, QueueName string
	OldQueueName            string
	DeadLetter              struct {
		ExchangeName, QueueName string
	}

	MinTaskProcessingTime time.Duration
	MaxTaskProcessingTime time.Duration
	MaxConcurrentTasks    uint
	HumanizeErrors        bool

	LivepeerAPIOptions api.ClientOptions
	Catalyst           *clients.CatalystOptions
	ExportTaskConfig
	ImportTaskConfig

	TaskHandlers map[string]TaskHandler
}

func NewRunner(opts RunnerOptions) Runner {
	if opts.TaskHandlers == nil {
		opts.TaskHandlers = defaultTasks
	}
	if opts.MinTaskProcessingTime == 0 {
		opts.MinTaskProcessingTime = DefaultMinTaskProcessingTime
	}
	if opts.MaxTaskProcessingTime == 0 {
		opts.MaxTaskProcessingTime = DefaultMaxTaskProcessingTime
	}
	if opts.MaxConcurrentTasks == 0 {
		opts.MaxConcurrentTasks = DefaultMaxConcurrentTasks
	}
	if opts.DeadLetter.ExchangeName != "" && opts.DeadLetter.QueueName == "" {
		opts.DeadLetter.QueueName = opts.DeadLetter.ExchangeName
	}
	return &runner{
		RunnerOptions:   opts,
		DelayedExchange: fmt.Sprintf("%s_delayed", opts.ExchangeName),
		lapi:            api.NewAPIClient(opts.LivepeerAPIOptions),
		catalyst:        clients.NewCatalyst(*opts.Catalyst),
		ipfs: clients.NewPinataClientJWT(opts.PinataAccessToken, map[string]string{
			"apiServer": opts.LivepeerAPIOptions.Server,
			"createdBy": clients.UserAgent,
		}),
	}
}

type runner struct {
	RunnerOptions
	DelayedExchange string

	lapi     *api.Client
	ipfs     clients.IPFS
	catalyst clients.Catalyst
	amqp     event.AMQPClient
}

func (r *runner) Start() error {
	if r.amqp != nil {
		return errors.New("runner already started")
	}

	amqp, err := event.NewAMQPClient(r.AMQPUri, event.NewAMQPConnectFunc(r.setupAmqpConnection))
	if err != nil {
		return fmt.Errorf("error creating AMQP consumer: %w", err)
	}
	err = amqp.Consume(r.QueueName, int(r.MaxConcurrentTasks), r.handleAMQPMessage)
	if err != nil {
		return fmt.Errorf("error consuming queue: %w", err)
	}

	// TODO: Remove this logic after migration to dead leterred queue
	if r.OldQueueName != "" {
		go cleanUpOldQueue(r.AMQPUri, r.OldQueueName, r.ExchangeName)
	}

	r.amqp = amqp
	return nil
}

func (r *runner) setupAmqpConnection(c event.AMQPChanSetup) error {
	queueArgs := amqp.Table{"x-queue-type": "quorum"}
	if dlx := r.DeadLetter; dlx.ExchangeName != "" {
		err := declareQueueAndExchange(c, dlx.ExchangeName, dlx.QueueName, "#", queueArgs)
		if err != nil {
			return err
		}
		queueArgs["x-dead-letter-exchange"] = r.DeadLetter.ExchangeName
	}

	err := declareQueueAndExchange(c, r.ExchangeName, r.QueueName, "task.trigger.#", queueArgs)
	if err != nil {
		return err
	}

	queueArgs = amqp.Table{
		"x-message-ttl":          int32(time.Minute / time.Millisecond),
		"x-dead-letter-exchange": r.ExchangeName,
	}
	err = declareQueueAndExchange(c, r.DelayedExchange, r.DelayedExchange, "#", queueArgs)
	if err != nil {
		return err
	}

	if err := c.Qos(int(r.MaxConcurrentTasks), 0, false); err != nil {
		return fmt.Errorf("error setting QoS: %w", err)
	}
	return nil
}

func cleanUpOldQueue(amqpURI, oldQueue, exchange string) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Recovered from panic in cleanUpOldQueue: %v", r)
		}
	}()

	cleanUp := event.NewAMQPConnectFunc(func(c event.AMQPChanSetup) error {
		err := c.QueueUnbind(oldQueue, "task.trigger.#", oldQueue, nil)
		if err != nil {
			glog.Errorf("Error unbinding old queue from exchange queue=%q exchange=%q err=%q", oldQueue, exchange, err)
		}

		_, err = c.QueueDelete(oldQueue, false, true, false)
		if err != nil {
			glog.Errorf("Error deleting old queue=%q err=%q", oldQueue, err)
		}

		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cleanUp(ctx, amqpURI, nil, nil)
}

func declareQueueAndExchange(c event.AMQPChanSetup, exchange, queue, binding string, queueArgs amqp.Table) error {
	err := c.ExchangeDeclare(exchange, "topic", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("error declaring exchange %q: %w", exchange, err)
	}
	_, err = c.QueueDeclare(queue, true, false, false, false, queueArgs)
	if err != nil {
		return fmt.Errorf("error declaring queue %q: %w", queue, err)
	}
	err = c.QueueBind(queue, binding, exchange, false, nil)
	if err != nil {
		return fmt.Errorf("error binding queue %q on %q to exchange %q: %w", queue, binding, exchange, err)
	}
	return nil
}

func (r *runner) handleAMQPMessage(msg amqp.Delivery) (err error) {
	// rate-limit message processing time to limit load
	defer blockUntil(time.After(r.MinTaskProcessingTime))

	ctx, cancel := context.WithTimeout(context.Background(), r.MaxTaskProcessingTime)
	defer cancel()

	task, err := parseTaskInfo(msg)
	if err != nil {
		glog.Errorf("Error parsing AMQP message err=%q msg=%q", err, msg.Body)
		return event.UnprocessableMsgErr(err)
	}
	defer func() {
		if rec := recover(); rec != nil {
			glog.Errorf("Panic handling task type=%s id=%s step=%q panic=%v stack=%q", task.Type, task.ID, task.Step, rec, debug.Stack())
			err = simplePublishTaskFatalError(r.amqp, r.ExchangeName, task)
		}
	}()

	output, err := r.handleTask(ctx, task)
	if err == nil && output != nil && output.Continue {
		glog.Infof("Task handler will continue task async type=%q id=%s output=%+v", task.Type, task.ID, output)
		return nil
	}

	glog.Infof("Task handler processed task type=%q id=%s output=%+v error=%q unretriable=%v", task.Type, task.ID, output, err, IsUnretriable(err))

	// return the error directly so that if publishing the result fails we nack the message to try again
	return r.publishTaskResult(task, output, err)
}

func (r *runner) handleTask(ctx context.Context, taskInfo data.TaskInfo) (out *TaskHandlerOutput, err error) {
	taskCtx, err := r.buildTaskContext(ctx, taskInfo)
	if err != nil {
		return nil, fmt.Errorf("error building task context: %w", err)
	}
	defer taskCtx.Progress.Stop()
	taskType, taskID := taskCtx.Task.Type, taskCtx.Task.ID

	handler, ok := r.TaskHandlers[strings.ToLower(taskType)]
	if !ok {
		return nil, UnretriableError{fmt.Errorf("unknown task type=%q", taskType)}
	}

	isFirstStep := taskCtx.Step == ""
	if !isFirstStep {
		glog.Infof(`Continuing task type=%q id=%s step=%s inputAssetId=%s outputAssetId=%s params="%+v" stepInput=%q`, taskType, taskID, taskCtx.Step, taskCtx.InputAssetID, taskCtx.OutputAssetID, taskCtx.Params, taskCtx.StepInput)
	} else {
		if taskCtx.Status.Phase == api.TaskPhaseRunning {
			return nil, errors.New("task has already been started before")
		}

		err = r.lapi.UpdateTaskStatus(taskID, api.TaskPhaseRunning, 0)
		if err == api.ErrRateLimited {
			glog.Warningf("Task execution rate limited type=%q id=%s userID=%s", taskType, taskID, taskCtx.UserID)
			return nil, r.delayTaskStep(ctx, taskID, taskCtx.Step, taskCtx.StepInput)
		} else if err != nil {
			glog.Errorf("Error updating task progress type=%q id=%s err=%q unretriable=%v", taskType, taskID, err, IsUnretriable(err))
			// execute the task anyway
		}

		glog.Infof(`Starting task type=%q id=%s inputAssetId=%s outputAssetId=%s params="%+v"`, taskType, taskID, taskCtx.InputAssetID, taskCtx.OutputAssetID, taskCtx.Params)
	}

	return handler(taskCtx)
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
	task, err := r.lapi.GetTask(info.ID, true)
	if err != nil {
		return nil, err
	}
	inputAsset, inputOSObj, inputOS, err := r.getAssetAndOS(task.InputAssetID)
	if err != nil {
		return nil, err
	}
	outputAsset, outputOSObj, outputOS, err := r.getAssetAndOS(task.OutputAssetID)
	if err != nil {
		return nil, err
	}
	progress := NewProgressReporter(ctx, r.lapi, task.ID)
	return &TaskContext{ctx, r, info, task, progress, inputAsset, outputAsset, inputOSObj, outputOSObj, inputOS, outputOS}, nil
}

func (r *runner) getAssetAndOS(assetID string) (*api.Asset, *api.ObjectStore, drivers.OSSession, error) {
	if assetID == "" {
		return nil, nil, nil, nil
	}
	asset, err := r.lapi.GetAsset(assetID, true)
	if err != nil {
		return nil, nil, nil, err
	}
	objectStore, err := r.lapi.GetObjectStore(asset.ObjectStoreID)
	if err != nil {
		return nil, nil, nil, err
	}
	osDriver, err := drivers.ParseOSURL(objectStore.URL, true)
	if err != nil {
		return nil, nil, nil, UnretriableError{fmt.Errorf("error parsing object store url=%s: %w", objectStore.URL, err)}
	}
	osSession := osDriver.NewSession("")
	return asset, objectStore, osSession, nil
}

func (r *runner) HandleCatalysis(ctx context.Context, taskId, nextStep, attemptID string, callback *clients.CatalystCallback) error {
	taskInfo, task, err := r.getTaskInfo(taskId, "catalysis", nil)
	if err != nil {
		return fmt.Errorf("failed to get task %s: %w", taskId, err)
	}

	glog.Infof("Received catalyst callback taskType=%q id=%s taskPhase=%s status=%q completionRatio=%v error=%q rawCallback=%+v",
		task.Type, task.ID, task.Status.Phase, callback.Status, callback.CompletionRatio, callback.Error, *callback)

	if task.Status.Phase != api.TaskPhaseRunning &&
		task.Status.Phase != api.TaskPhaseWaiting {
		return fmt.Errorf("task %s is not running", taskId)
	} else if curr := catalystTaskAttemptID(task); attemptID != "" && attemptID != curr {
		return fmt.Errorf("outdated catalyst job callback, "+
			"task has already been retried (callback: %s current: %s)", attemptID, curr)
	}

	progress := 0.9 * callback.CompletionRatio
	progress = math.Round(progress*1000) / 1000
	currProgress, taskUpdatedAt := task.Status.Progress, data.NewUnixMillisTime(task.Status.UpdatedAt)
	if shouldReportProgress(progress, currProgress, task.ID, taskUpdatedAt.Time) {
		err = r.lapi.UpdateTaskStatus(task.ID, api.TaskPhaseRunning, progress)
		if err != nil {
			glog.Warningf("Failed to update task progress. taskID=%s err=%v", task.ID, err)
		}
	}

	if callback.Status == clients.CatalystStatusError {
		glog.Infof("Catalyst job failed for task type=%q id=%s error=%q unretriable=%v", task.Type, task.ID, callback.Error, callback.Unretriable)
		err := NewCatalystError(callback.Error, callback.Unretriable)
		return r.publishTaskResult(taskInfo, nil, err)
	} else if callback.Status == clients.CatalystStatusSuccess {
		return r.scheduleTaskStep(task.ID, nextStep, callback)
	}

	return nil
}

func (r *runner) delayTaskStep(ctx context.Context, taskID, step string, input interface{}) error {
	if step == "" {
		return errors.New("can only schedule sub-steps of tasks")
	}
	task, _, err := r.getTaskInfo(taskID, step, input)
	if err != nil {
		return err
	}
	return r.publishLogged(task, r.DelayedExchange,
		fmt.Sprintf("task.trigger.%s", task.Type),
		data.NewTaskTriggerEvent(task))
}

func (r *runner) scheduleTaskStep(taskID, step string, input interface{}) error {
	if step == "" {
		return errors.New("can only schedule sub-steps of tasks")
	}
	task, _, err := r.getTaskInfo(taskID, step, input)
	if err != nil {
		return err
	}
	key, body := fmt.Sprintf("task.trigger.%s", task.Type), data.NewTaskTriggerEvent(task)
	if err := r.publishLogged(task, r.ExchangeName, key, body); err != nil {
		return fmt.Errorf("error publishing task result event: %w", err)
	}
	return nil
}

func (r *runner) publishTaskResult(task data.TaskInfo, output *TaskHandlerOutput, resultErr error) error {
	if r.HumanizeErrors {
		resultErr = humanizeError(resultErr)
	}
	var body *data.TaskResultEvent
	if resultErr != nil {
		body = data.NewTaskResultEvent(task, errorInfo(resultErr), nil)
	} else if output != nil {
		body = data.NewTaskResultEvent(task, errorInfo(resultErr), output.TaskOutput)
	} else {
		return errors.New("output or resultErr must be non-nil")
	}
	key := fmt.Sprintf("task.result.%s.%s", task.Type, task.ID)
	if err := r.publishLogged(task, r.ExchangeName, key, body); err != nil {
		return fmt.Errorf("error publishing task result event: %w", err)
	}
	return nil
}

func (r *runner) getTaskInfo(id, step string, input interface{}) (data.TaskInfo, *api.Task, error) {
	task, err := r.lapi.GetTask(id, true)
	if err != nil {
		return data.TaskInfo{}, nil, fmt.Errorf("error getting task %q: %w", id, err)
	}
	snapshot, err := json.Marshal(task)
	if err != nil {
		return data.TaskInfo{}, task, fmt.Errorf("error marshalling task %q: %w", id, err)
	}
	var stepInput json.RawMessage
	if input != nil {
		stepInput, err = json.Marshal(input)
		if err != nil {
			return data.TaskInfo{}, task, fmt.Errorf("error marshalling step input %q: %w", id, err)
		}
	}
	return data.TaskInfo{
		ID:        id,
		Type:      task.Type,
		Snapshot:  snapshot,
		Step:      step,
		StepInput: stepInput,
	}, task, nil
}

func (r *runner) publishLogged(task data.TaskInfo, exchange, key string, body interface{}) error {
	return publishLoggedRaw(r.amqp, task, exchange, key, body)
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

// Caller should check if err is a CatalystError first
func humanizeCatalystError(err error) error {
	errMsg := strings.ToLower(err.Error())

	fileNotAccessibleErrs := []string{
		"504 Gateway Timeout",
		"giving up after",
	}
	// General errors
	if strings.Contains(errMsg, "import request") {
		for _, e := range fileNotAccessibleErrs {
			if strings.Contains(errMsg, e) {
				return errors.New("file could not be imported from URL because it was not accessible")
			}
		}
	}

	// MediaConvert pipeline errors
	if strings.Contains(errMsg, "doesn't have video that the transcoder can consume") {
		// TODO(yondonfu): Add link in this error message to a page with the input codec/container support matrix
		return errors.New("invalid video file codec or container, check your input file against the input codec and container support matrix")
	} else if strings.Contains(errMsg, "Failed probe/open") {
		// TODO(yondonfu): Add link in this error message to a page with the input codec/container support matrix
		return errors.New("failed to probe or open file, check your input file against the input codec and container support matrix")
	}

	// Livepeer pipeline errors
	if strings.Contains(errMsg, "unsupported input pixel format") {
		return errors.New("unsupported input pixel format, must be 'yuv420p' or 'yuvj420p'")
	} else if strings.Contains(errMsg, "Unsupported video input") {
		return errors.New("unsupported file format")
	} else if strings.Contains(errMsg, "ReadPacketData File read failed - end of file hit") {
		return errors.New("invalid video file, possibly truncated")
	}

	return errInternalProcessingError
}

func humanizeError(err error) error {
	if err == nil {
		return nil
	}
	errMsg := strings.ToLower(err.Error())

	var catErr CatalystError
	if errors.As(err, &catErr) {
		return humanizeCatalystError(err)
	}

	if strings.Contains(errMsg, "unexpected eof") {
		return errors.New("file download failed")
	} else if strings.Contains(errMsg, "multipartupload: upload multipart failed") {
		return errors.New("error saving file to storage")
	} else if strings.Contains(errMsg, "mp4io: parse error") {
		return UnretriableError{errors.New("file format unsupported, must be a valid MP4")}
	}

	isProcessing := strings.Contains(errMsg, "error running ffprobe [] exit status 1") ||
		strings.Contains(errMsg, "could not create stream id") ||
		strings.Contains(errMsg, "502 bad gateway") ||
		strings.Contains(errMsg, "task has already been started before") ||
		strings.Contains(errMsg, "catalyst task lost") ||
		(strings.Contains(errMsg, "eof") && strings.Contains(errMsg, "error processing file"))

	if isProcessing {
		return errInternalProcessingError
	}

	isTimeout := strings.Contains(errMsg, "context deadline exceeded") ||
		strings.Contains(errMsg, "context canceled") ||
		strings.Contains(errMsg, "context deadline exceeded")

	if isTimeout {
		return errors.New("execution timeout")
	}

	return err
}

func blockUntil(t <-chan time.Time) { <-t }

// This is a code path to send a task failure event the simplest way possible,
// to be used when handling panics in the task processing code path. It is
// purposedly meant to be a separate flow as much as possible to avoid any
// chance of hitting the same panic again.
func simplePublishTaskFatalError(producer event.AMQPProducer, exchange string, task data.TaskInfo) error {
	body := data.NewTaskResultEvent(task, taskFatalErrorInfo, nil)
	key := taskResultMessageKey(task.Type, task.ID)
	return publishLoggedRaw(producer, task, exchange, key, body)
}

func publishLoggedRaw(producer event.AMQPProducer, task data.TaskInfo, exchange, key string, body interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), taskPublishTimeout)
	defer cancel()
	msg := event.AMQPMessage{
		Exchange:   exchange,
		Key:        key,
		Body:       body,
		Persistent: true,
		// TODO: Actually handle returns from the AMQP server so we can toggle mandatory here. Needs further support in pkg/event.
		// Mandatory:  true,
		WaitResult: true,
	}
	glog.Infof("Publishing AMQP message. taskType=%q id=%s step=%q exchange=%q key=%q body=%+v", task.Type, task.ID, task.Step, exchange, key, body)
	if err := producer.Publish(ctx, msg); err != nil {
		glog.Errorf("Error publishing AMQP message. taskType=%q id=%s step=%q exchange=%q key=%q err=%q body=%+v", task.Type, task.ID, task.Step, exchange, key, err, body)
		return err
	}
	return nil
}

func taskResultMessageKey(ttype, id string) string {
	return fmt.Sprintf("task.result.%s.%s", ttype, id)
}
