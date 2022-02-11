package task

import (
	"context"
	"math"
	"runtime/debug"
	"time"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
)

const progressReportInterval = 10 * time.Second

func ReportProgress(ctx context.Context, lapi *livepeerAPI.Client, taskID string, size uint64, getCount func() uint64) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Panic reporting task progress: value=%q stack:\n%s", r, string(debug.Stack()))
		}
	}()
	if size <= 0 {
		return
	}
	timer := time.NewTicker(progressReportInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			progress := float64(getCount()) / float64(size)
			progress = math.Round(progress*1000) / 1000
			progress = math.Min(progress, 0.99)
			if err := lapi.UpdateTaskStatus(taskID, "running", progress); err != nil {
				glog.Errorf("Error updating task progress taskID=%s progress=%v err=%q", taskID, progress, err)
			}
		}
	}
}
