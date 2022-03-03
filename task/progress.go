package task

import (
	"context"
	"math"
	"runtime/debug"
	"sort"
	"time"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
)

var progressReportBuckets = []float64{0, 0.25, 0.5, 0.75, 1}

const minProgressReportInterval = 10 * time.Second
const progressCheckInterval = 1 * time.Second

func ReportProgress(ctx context.Context, lapi *livepeerAPI.Client, taskID string, size uint64, getCount func() uint64) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Panic reporting task progress: value=%q stack:\n%s", r, string(debug.Stack()))
		}
	}()
	if size <= 0 {
		return
	}
	var (
		timer        = time.NewTicker(progressCheckInterval)
		lastProgress = float64(0)
		lastReport   time.Time
	)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			progress := calcProgress(getCount(), size)
			if time.Since(lastReport) < minProgressReportInterval &&
				progressBucket(progress) == progressBucket(lastProgress) {
				continue
			}
			if err := lapi.UpdateTaskStatus(taskID, "running", progress); err != nil {
				glog.Errorf("Error updating task progress taskID=%s progress=%v err=%q", taskID, progress, err)
				continue
			}
			lastReport, lastProgress = time.Now(), progress
		}
	}
}

func calcProgress(count, size uint64) (val float64) {
	val = float64(count) / float64(size)
	val = math.Round(val*1000) / 1000
	val = math.Min(val, 0.99)
	return
}

func progressBucket(progress float64) int {
	return sort.SearchFloat64s(progressReportBuckets, progress)
}