package task

import (
	"context"
	"math"
	"runtime/debug"
	"sort"
	"time"

	"github.com/golang/glog"
)

var progressReportBuckets = []float64{0, 0.25, 0.5, 0.75, 1}

const minProgressReportInterval = 10 * time.Second
const progressCheckInterval = 1 * time.Second

func ReportProgress(ctx context.Context, lapi *livepeerAPI.Client, taskID string, size uint64, getCount func() uint64, startPercentage int, endPercentage int) {
	startPercentage = int(math.Max(float64(startPercentage), 0)) // >= 0, <= 100, < endPercentage
	endPercentage = int(math.Min(float64(endPercentage), 100))   // >= 0, <= 100, > startPercentage
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
			progress := calcProgress(getCount(), size, startPercentage, endPercentage)
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

func calcProgress(count, size uint64, startPercentage int, endPercentage int) (val float64) {
	val = float64(count) / float64(size)
	val = val * (float64(startPercentage) - float64(endPercentage))
	val = (val + float64(startPercentage)) / 100
	val = math.Round(val*1000) / 1000
	val = math.Min(val, 0.99)
	return
}

func progressBucket(progress float64) int {
	return sort.SearchFloat64s(progressReportBuckets, progress)
}
