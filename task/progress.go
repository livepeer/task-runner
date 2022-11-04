package task

import (
	"context"
	"io"
	"math"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
)

var progressReportBuckets = []float64{0, 0.25, 0.5, 0.75, 1}

const minProgressReportInterval = 10 * time.Second
const progressCheckInterval = 1 * time.Second

type ProgressReporter struct {
	ctx    context.Context
	cancel context.CancelFunc
	lapi   *api.Client
	taskID string

	mu                   sync.Mutex
	getProgress          func() float64
	scaleStart, scaleEnd float64

	lastReport   time.Time
	lastProgress float64
}

func NewProgressReporter(ctx context.Context, lapi *api.Client, taskID string) *ProgressReporter {
	ctx, cancel := context.WithCancel(ctx)
	p := &ProgressReporter{
		ctx:    ctx,
		cancel: cancel,
		lapi:   lapi,
		taskID: taskID,
	}
	go p.mainLoop()
	return p
}

func (p *ProgressReporter) Stop() {
	p.cancel()
}

func (p *ProgressReporter) Track(getProgress func() float64, end float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if end < p.scaleStart || end > 1 {
		glog.Errorf("Invalid end progress set taskID=%s lastProgress=%f endProgress=%f", p.taskID, p.lastProgress, end)
		if end > 1 {
			end = 1
		} else {
			end = p.scaleStart
		}
	}
	p.getProgress, p.scaleStart, p.scaleEnd = getProgress, p.scaleEnd, end
}

func (p *ProgressReporter) Set(val float64) {
	p.Track(func() float64 { return 1 }, val)
}

func (p *ProgressReporter) TrackCount(getCount func() uint64, size uint64, endProgress float64) {
	p.Track(func() float64 {
		return float64(getCount()) / float64(size)
	}, endProgress)
}

func (p *ProgressReporter) TrackReader(r io.Reader, size uint64, endProgress float64) *ReadCounter {
	counter := NewReadCounter(r)
	p.TrackCount(counter.Count, size, endProgress)
	return counter
}

func (p *ProgressReporter) mainLoop() {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Panic reporting task progress: value=%q stack:\n%s", r, string(debug.Stack()))
		}
	}()
	timer := time.NewTicker(progressCheckInterval)
	defer timer.Stop()
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-timer.C:
			p.reportOnce()
		}
	}
}

func (p *ProgressReporter) reportOnce() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.getProgress == nil {
		return
	}

	progress := p.calcProgress()
	if progress <= p.lastProgress {
		glog.Errorf("Non monotonic progress received taskID=%s lastProgress=%v progress=%v", p.taskID, p.lastProgress, progress)
		return
	}
	if time.Since(p.lastReport) < minProgressReportInterval &&
		progressBucket(progress) == progressBucket(p.lastProgress) {
		return
	}
	if err := p.lapi.UpdateTaskStatus(p.taskID, "running", progress); err != nil {
		glog.Errorf("Error updating task progress taskID=%s progress=%v err=%q", p.taskID, progress, err)
		return
	}
	p.lastReport, p.lastProgress = time.Now(), progress
}

func (p *ProgressReporter) calcProgress() float64 {
	val := p.getProgress()
	val = math.Max(val, 0)
	val = math.Min(val, 0.99)
	val = p.scaleStart + val*(p.scaleEnd-p.scaleStart)
	val = math.Round(val*1000) / 1000
	return val
}

func progressBucket(progress float64) int {
	return sort.SearchFloat64s(progressReportBuckets, progress)
}
