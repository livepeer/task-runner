package task

import (
	"io/ioutil"
	"net/http"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
)

func TaskImport(info data.TaskInfo, lapi *livepeerAPI.Client) error {
	task, err := lapi.GetTask(info.ID)
	if err != nil {
		// TODO: Avoid returning error in case its unretriable (4xx?)
		return err
	}

	// Here be fantasy
	inputUrl := mockGetTaskInputParams(task)
	resp, err := http.Get(inputUrl)
	if err != nil {
		glog.Errorf("Error downloading task input err=%v", err)
		// TODO: Try identifying transient errors that could make sense to retry.
		return nil
	} else if resp.StatusCode >= 400 {
		glog.Errorf("Status error downloading task input. status=%d", resp.StatusCode)
		return nil
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	glog.Info("Import task success. response=%q", string(body))
	return nil
}

func mockGetTaskInputParams(task *livepeerAPI.Task) (inputUrl string) {
	return "https://httpbin.org/anything?userId=" + task.UserId
}
