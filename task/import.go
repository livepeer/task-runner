package task

import (
	"io/ioutil"
	"net/http"
	"path"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/drivers"
)

const fileUploadTimeout = 5 * time.Minute

func TaskImport(tctx *TaskContext) error {
	var (
		ctx    = tctx.Context
		srcURL = tctx.Params.Import.URL
		osURL  = tctx.ObjectStore.URL
		// TODO: Re-evaluate this file path. Maybe omit the user ID and/or use a playbackID instead of asset ID (primary key)?
		filePath = path.Join(tctx.Asset.UserID, tctx.Asset.ID, "video.mp4")
	)

	osDriver, err := drivers.ParseOSURL(osURL, true)
	if err != nil {
		glog.Errorf("Error parsing object store url=%s err=%q", osURL, err)
		return nil
	}
	osSess := osDriver.NewSession("")

	req, err := http.NewRequestWithContext(ctx, "GET", srcURL, nil)
	if err != nil {
		glog.Errorf("Error creating http request url=%s err=%q", srcURL, err)
		return nil
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		glog.Errorf("Error downloading task input err=%v", err)
		return nil
	} else if resp.StatusCode >= 300 {
		glog.Errorf("Status error downloading task input status=%d", resp.StatusCode)
		return nil
	}

	// TODO: ObjectStore driver must receive a Reader instead of the entire file buffered in memory.
	contents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		glog.Errorf("Error reading file from remote URL err=%q", err)
		return err
	}

	_, err = osSess.SaveData(ctx, filePath, contents, nil, fileUploadTimeout)
	if err != nil {
		glog.Errorf("Error uploading file to object store filePath=%q err=%q", filePath, err)
		return err
	}

	// TODO: Register success on the API
	glog.Info("Import task success! filePath=%q", filePath)
	return nil
}
