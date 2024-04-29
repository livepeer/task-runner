package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	catalystClients "github.com/livepeer/catalyst-api/clients"
	api "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-tools/drivers"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/livepeer/task-runner/clients"
	"github.com/livepeer/task-runner/metrics"
	"github.com/prometheus/client_golang/prometheus"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	DefaultMaxTaskProcessingTime = 10 * time.Minute
	DefaultMinTaskProcessingTime = 5 * time.Second
	DefaultMaxConcurrentTasks    = 3
	taskPublishTimeout           = 1 * time.Minute
)

// Humanized errors
var (
	errFileInaccessible = errors.New("file could not be imported from URL because it was not accessible")
	// TODO(yondonfu): Add link in this error message to a page with the input codec/container support matrix
	// TODO(yondonfu): See if we can passthrough the MediaConvert error message with the exact problematic input codec
	// without including extraneous error information from Catalyst
	errInvalidVideo = UnretriableError{errors.New("invalid video file codec or container, check your input file against the input codec and container support matrix")}
	// TODO(yondonfu): Add link in this error message to a page with the input codec/container support matrix
	errProbe = UnretriableError{errors.New("failed to probe or open file, check your input file against the input codec and container support matrix")}
)

var (
	defaultTasks = map[string]TaskHandler{
		"upload":         TaskUpload,
		"export":         TaskExport,
		"export-data":    TaskExportData,
		"transcode-file": TaskTranscodeFile,
		"clip":           TaskClip,
	}

	errInternalProcessingError = errors.New("internal error processing file")
	taskFatalErrorInfo         = &data.ErrorInfo{Message: errInternalProcessingError.Error(), Unretriable: true}

	taskResultCount = metrics.Factory.NewCounterVec(
		prometheus.CounterOpts{
			Name: metrics.FQName("task_result_count"),
			Help: "Breakdown of task execution results by task type and status (success, error, unretriable_error)",
		},
		[]string{"task_type", "status"},
	)
	taskStepDurationSec = metrics.Factory.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    metrics.FQName("task_step_duration_sec"),
			Help:    "Time spent executing a task by task type, step and result flags",
			Buckets: []float64{5, 15, 60, 300, 900, 3600},
		},
		[]string{"task_type", "step", "finished", "errored"},
	)
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
	CronJobForAssetDeletion(ctx context.Context) error
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
	UploadTaskConfig

	VodDecryptPrivateKey string
	VodDecryptPublicKey  string

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
		err := c.QueueUnbind(oldQueue, "task.trigger.#", exchange, nil)
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

	startTime := time.Now()
	output, err := r.handleTask(ctx, task)

	willContinue := err == nil && output != nil && output.Continue
	taskStepDurationSec.
		WithLabelValues(task.Type, task.Step, strconv.FormatBool(!willContinue), strconv.FormatBool(err != nil)).
		Observe(time.Since(startTime).Seconds())

	// send partial message to studio here
	if willContinue && output.TaskOutput != nil && output.TaskOutput.Upload != nil {
		body := data.NewTaskResultPartialEvent(task, &data.TaskPartialOutput{
			Upload: output.TaskOutput.Upload,
		})
		key := fmt.Sprintf("task.resultPartial.%s.%s", task.Type, task.ID)
		if err = r.publishLogged(task, r.ExchangeName, key, body); err != nil {
			return fmt.Errorf("error publishing task partial event: %w", err)
		}
	}

	if willContinue {
		glog.Infof("Task handler will continue task async type=%q id=%s output=%+v", task.Type, task.ID, output)
		return nil
	}

	// return the error directly so that if publishing the result fails we nack the message to try again
	return r.publishTaskResult(task, output, err)
}

func (r *runner) handleTask(ctx context.Context, taskInfo data.TaskInfo) (out *TaskHandlerOutput, err error) {
	taskCtx, err := r.buildTaskContext(ctx, taskInfo)
	if err != nil {
		if errors.Is(err, assetNotFound) {
			glog.Infof("task cancelled, asset has been deleted. taskID=%s", taskInfo.ID)
			return nil, UnretriableError{errors.New("task cancelled, asset has been deleted")}
		}
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
		glog.Infof("Continuing task type=%q id=%s step=%s inputAssetId=%s outputAssetId=%s", taskType, taskID, taskCtx.Step, taskCtx.InputAssetID, taskCtx.OutputAssetID)
	} else {
		if taskCtx.Status.Phase == api.TaskPhaseRunning {
			return nil, errors.New("task has already been started before")
		}

		err = r.lapi.UpdateTaskStatus(taskID, api.TaskPhaseRunning, 0, "")
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
		if errors.Is(err, api.ErrNotExists) {
			return nil, assetNotFound
		}
		return nil, err
	}
	progress := NewProgressReporter(ctx, r.lapi, task.ID, info.Step)
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

	glog.Infof("Received catalyst callback taskType=%q id=%s taskPhase=%s status=%q completionRatio=%v error=%q",
		task.Type, task.ID, task.Status.Phase, callback.Status, callback.CompletionRatio, callback.Error)

	if task.Status.Phase != api.TaskPhaseRunning &&
		task.Status.Phase != api.TaskPhaseWaiting {
		return fmt.Errorf("task %s is not running", taskId)
	} else if curr := catalystTaskAttemptID(task); attemptID != "" && attemptID != curr {
		return fmt.Errorf("outdated catalyst job callback, "+
			"task has already been retried (callback: %s current: %s)", attemptID, curr)
	}

	if callback.SourcePlayback != nil {
		err = r.scheduleTaskStep(task.ID, "resultPartial", callback.SourcePlayback)
		if err != nil {
			glog.Warningf("Failed to schedule resultPartial step. taskID=%s err=%v", task.ID, err)
		}
	}

	progress := 0.9 * callback.CompletionRatio
	progress = math.Round(progress*1000) / 1000
	step := "catalyst_" + strings.ToLower(callback.Status.String())
	if callback.Status == catalystClients.TranscodeStatusCompleted {
		step = nextStep
	}

	if shouldReportProgressTask(progress, step, task) {
		err = r.lapi.UpdateTaskStatus(task.ID, api.TaskPhaseRunning, progress, step)
		if err != nil {
			glog.Warningf("Failed to update task progress. taskID=%s err=%v", task.ID, err)
		}
	}

	if callback.Status == catalystClients.TranscodeStatusError {
		glog.Infof("Catalyst job failed for task type=%q id=%s error=%q unretriable=%v", task.Type, task.ID, callback.Error, callback.Unretriable)
		err := NewCatalystError(callback.Error, callback.Unretriable)
		return r.publishTaskResult(taskInfo, nil, err)
	} else if callback.Status == catalystClients.TranscodeStatusCompleted {
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

func (r *runner) publishTaskResult(task data.TaskInfo, output *TaskHandlerOutput, rawErr error) error {
	errInfo := makeErrorInfo(rawErr)

	taskResultCount.WithLabelValues(task.Type, taskResultStatusLabel(errInfo)).Inc()
	glog.Infof("Task handler processed task type=%q id=%s output=%+v error=%q humanError=%q unretriable=%v",
		task.Type, task.ID, output, errInfo.RawError, errInfo.HumanError, errInfo.Unretriable)

	var body *data.TaskResultEvent
	if errInfo.RawError != nil {
		evtErrInfo := &data.ErrorInfo{
			Message:     errInfo.RawError.Error(),
			Unretriable: errInfo.Unretriable,
		}
		if r.HumanizeErrors {
			evtErrInfo.Message = errInfo.HumanError.Error()
		}
		body = data.NewTaskResultEvent(task, evtErrInfo, nil)
	} else if output != nil {
		body = data.NewTaskResultEvent(task, nil, output.TaskOutput)
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

func (r *runner) UnpinFromIpfs(ctx context.Context, cid string, filter string) error {
	assets, _, err := r.lapi.ListAssets(api.ListOptions{
		Filters: map[string]interface{}{
			filter: cid,
		},
		AllUsers: true,
	})

	if err != nil {
		return err
	}

	if len(assets) == 1 {
		return r.ipfs.Unpin(ctx, cid)
	}

	return nil
}

func (r *runner) CronJobForAssetDeletion(ctx context.Context) error {
	// Loop every hour to delete assets
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	glog.Infof("Starting asset deletion cron job")

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			assets, err := r.lapi.GetDeletingAssets()
			if err != nil {
				glog.Errorf("Error retrieving assets for deletion: %v", err)
				continue
			}
			for _, asset := range assets {
				err := deleteAsset(asset, r, ctx)
				if err != nil {
					glog.Errorf("Error deleting asset %v: %v", asset.ID, err)
				}
			}
		}
	}
}

func deleteAsset(asset *api.Asset, r *runner, ctx context.Context) error {
	_, _, assetOS, err := r.getAssetAndOS(asset.ID)
	if err != nil {
		glog.Errorf("Error getting asset object store session: %v", err)
		return err
	}

	directory := asset.PlaybackID
	var totalDeleted int

	// Initially list files
	pi, err := assetOS.ListFiles(ctx, directory, "/")
	glog.Infof("Found %v files for asset %v", len(pi.Files()), asset.ID)
	if err != nil {
		glog.Errorf("Error listing files for asset %v: %v", asset.ID, err)
		return err
	}

	isErr := false

	for pi != nil {
		for _, file := range pi.Files() {
			glog.Infof("Found file %v", file.Name)
			err := assetOS.DeleteFile(ctx, file.Name)
			if err != nil {
				glog.Errorf("Error deleting file %v: %v", file.Name, err)
				isErr = true
				continue
			}
			glog.Infof("Deleted file %v", file.Name)
			totalDeleted++
		}

		if pi.HasNextPage() {
			pi, err = pi.NextPage()
			if err != nil {
				glog.Errorf("Failed to load next page of files for asset %v: %v", asset.ID, err)
				isErr = true
				break
			}
		} else {
			break // No more pages
		}
	}

	glog.Infof("Deleted %v files from asset=%v", totalDeleted, asset.ID)

	if ipfs := asset.AssetSpec.Storage.IPFS; ipfs != nil {
		err = r.UnpinFromIpfs(ctx, ipfs.CID, "cid")
		if err != nil {
			glog.Errorf("Error unpinning from IPFS %v", ipfs.CID)
			return err
		}
		err = r.UnpinFromIpfs(ctx, ipfs.NFTMetadata.CID, "nftMetadataCid")
		if err != nil {
			glog.Errorf("Error unpinning metadata from IPFS %v", ipfs.NFTMetadata.CID)
			return err
		}

		glog.Infof("Unpinned asset=%v from IPFS", asset.ID)
	}

	if isErr {
		return errors.New("error deleting files")
	}

	err = r.lapi.FlagAssetAsDeleted(asset.ID)
	if err != nil {
		glog.Errorf("Error flagging asset as deleted: %v", err)
		return err
	}

	return nil
}

type taskErrInfo struct {
	RawError, HumanError error
	Unretriable          bool
}

func makeErrorInfo(err error) taskErrInfo {
	if err == nil {
		return taskErrInfo{}
	}
	humanErr := humanizeError(err)

	if IsUnretriable(err) && !IsUnretriable(humanErr) {
		// catch if we ever create a human error that loses the (opt-in) unretriable
		// flag of the original error. we still consider the original error below.
		glog.Warningf("Error is unretriable but humanized error is retriable. originalErr=%q humanErr=%q", err, humanErr)
	}
	unretriable := IsUnretriable(err) || IsUnretriable(humanErr)

	return taskErrInfo{
		RawError:    err,
		HumanError:  humanErr,
		Unretriable: unretriable,
	}
}

// Caller should check if err is a CatalystError first
func humanizeCatalystError(err error) error {
	errMsg := strings.ToLower(err.Error())

	if strings.Contains(errMsg, "probe failed for segment") {
		return errInternalProcessingError
	}

	fileNotAccessibleErrs := []string{
		// This should trigger retry logic so probably can be removed
		"504 gateway timeout",
		// This will not trigger retry logic so needs to be handled on its own
		"404 not found",
		// Checks for hitting max retries for a HTTP client
		"giving up after",
	}
	// General errors
	// TODO(yondonfu): This string matching is ugly and we should come up with a better less error-prone way to
	// do this
	if strings.Contains(errMsg, "download error") && strings.Contains(errMsg, "import request") {
		for _, e := range fileNotAccessibleErrs {
			if strings.Contains(errMsg, e) {
				return errFileInaccessible
			}
		}
	}

	switch {
	case strings.Contains(errMsg, "upload error") && strings.Contains(errMsg, "failed to write file") && strings.Contains(errMsg, "unexpected eof"):
		return errFileInaccessible
	// MediaConvert inaccessible error
	case strings.Contains(errMsg, "3450") && strings.Contains(errMsg, "error encountered when accessing"):
		return errFileInaccessible
	case strings.Contains(errMsg, "error copying input file to s3") && (strings.Contains(errMsg, "download error") || strings.Contains(errMsg, "unexpected eof")):
		return errFileInaccessible
	case strings.Contains(errMsg, "failed to write to os url") && strings.Contains(errMsg, "accessdenied"):
		return errFileInaccessible
	}

	invalidVideoErrs := []string{
		"doesn't have video that the transcoder can consume",
		"is not a supported input video codec",
		"is not a supported input audio codec",
		// Keeping it simple for now and returning errInvalidVideo for this
		// But, should there be a separate humanized error for this?
		"readpacketdata file read failed - end of file hit",
		"no video track found in file",
		"no pictures decoded",
		"zero bytes found for source",
		"invalid framerate",
		"maximum resolution is",
		"unsupported video input",
		"scaler position rectangle is outside output frame",
		"received non-media manifest",
		"no audio frames decoded on",
		"there is no frame rate information in the input stream info",
		"minimum field value of",
	}

	// MediaConvert pipeline errors
	for _, e := range invalidVideoErrs {
		if strings.Contains(errMsg, e) {
			return errInvalidVideo
		}
	}
	if strings.Contains(errMsg, "failed probe/open") {
		return errProbe
	}
	if strings.Contains(errMsg, "error probing") {
		return errInvalidVideo
	}

	// Livepeer pipeline errors
	if strings.Contains(errMsg, "unsupported input pixel format") {
		return UnretriableError{errors.New("unsupported input pixel format, must be 'yuv420p' or 'yuvj420p'")}
	}

	if IsUnretriable(err) {
		return UnretriableError{errInternalProcessingError}
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

func taskResultStatusLabel(errInfo taskErrInfo) string {
	if errInfo.RawError == nil {
		return "success"
	} else if errInfo.Unretriable {
		return "unretriable_error"
	} else {
		return "error"
	}
}

func taskResultMessageKey(ttype, id string) string {
	return fmt.Sprintf("task.result.%s.%s", ttype, id)
}
