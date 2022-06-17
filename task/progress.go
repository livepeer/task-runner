package task

import (
	"context"
	"math"
	"runtime/debug"
	"sort"
	"time"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
)

var progressReportBuckets = []float64{0, 0.25, 0.5, 0.75, 1}

const minProgressReportInterval = 10 * time.Second
const progressCheckInterval = 1 * time.Second

func ReportProgress(ctx context.Context, lapi *api.Client, taskID string, size uint64, getCount func() uint64, startFraction, endFraction float64) {
	glog.Infof(`Report Progress rev3 taskID=%s size=%d count=%d startFraction=%f endFraction=%f"`, taskID, size, getCount(), startFraction, endFraction)
	if startFraction > endFraction || startFraction < 0 || endFraction < 0 || startFraction > 1 || endFraction > 1 {
		glog.Errorf("Error reporting task progress taskID=%s startFraction=%f endFraction=%f", taskID, startFraction, endFraction)
		return
	}
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
			glog.Infof(`Report Progress COUNT taskID=%s count=%d size=%d progress=%f"`, taskID, getCount(), size, progress)
			if time.Since(lastReport) < minProgressReportInterval &&
				progressBucket(progress) == progressBucket(lastProgress) {
				continue
			}
			scaledProgress := scaleProgress(progress, startFraction, endFraction)
			glog.Infof(`Report Progress SCALE taskID=%s progress=%f scaledProgress=%f start=%f end=%f"`, taskID, progress, scaledProgress, startFraction, endFraction)
			if err := lapi.UpdateTaskStatus(taskID, "running", scaledProgress); err != nil {
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

func scaleProgress(progress, startFraction, endFraction float64) (val float64) {
	val = startFraction + val*(endFraction-startFraction)
	return
}

func progressBucket(progress float64) int {
	return sort.SearchFloat64s(progressReportBuckets, progress)
}
