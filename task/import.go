package task

import (
	"encoding/json"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/drivers"
	"golang.org/x/sync/errgroup"
	ffprobe "gopkg.in/vansante/go-ffprobe.v2"
)

const fileUploadTimeout = 5 * time.Minute

func TaskImport(tctx *TaskContext) (bool, interface{}, error) {
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
		return true, nil, err
	}
	osSess := osDriver.NewSession("")

	req, err := http.NewRequestWithContext(ctx, "GET", srcURL, nil)
	if err != nil {
		glog.Errorf("Error creating http request url=%s err=%q", srcURL, err)
		return true, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		glog.Errorf("Error downloading task input err=%v", err)
		return true, nil, err
	} else if resp.StatusCode >= 300 {
		glog.Errorf("Status error downloading task input status=%d", resp.StatusCode)
		return true, nil, err
	}

	secondaryReader, pipe := io.Pipe()
	mainReader := io.TeeReader(resp.Body, pipe)

	eg, egCtx := errgroup.WithContext(ctx)
	var probeData *ffprobe.ProbeData
	eg.Go(func() error {
		defer pipe.Close()
		var err error
		probeData, err = ffprobe.ProbeReader(egCtx, mainReader)
		if err != nil {
			glog.Errorf("Error probing file err=%q", err)
			return err
		}
		return nil
	})
	eg.Go(func() error {
		_, err = osSess.SaveData(egCtx, filePath, secondaryReader, nil, fileUploadTimeout)
		if err != nil {
			glog.Errorf("Error uploading file to object store filePath=%q err=%q", filePath, err)
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return false, nil, err
	}

	probeJson, _ := json.Marshal(probeData)
	glog.Info("Import task success! filePath=%q probeData=%+v probeJson=%q", filePath, probeData, probeJson)
	return true, probeData, nil
}
