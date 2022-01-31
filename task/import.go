package task

import (
	"io/ioutil"
	"net/http"

	"github.com/golang/glog"
)

func TaskImport(tctx *TaskContext) error {
	var (
		ctx    = tctx.Context
		srcURL = tctx.Params.Import.URL
	)
	req, err := http.NewRequestWithContext(ctx, "GET", srcURL, nil)
	if err != nil {
		glog.Errorf("Error creating http request err=%q", err)
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		glog.Errorf("Error downloading task input err=%v", err)
		return nil
	} else if resp.StatusCode >= 300 {
		glog.Errorf("Status error downloading task input. status=%d", resp.StatusCode)
		return nil
	}

	// Here be fantasy
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	glog.Info("Import task success. response=%q", string(body))
	return nil
}
