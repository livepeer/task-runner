package task

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-livepeer/drivers"
	"golang.org/x/sync/errgroup"
	ffprobe "gopkg.in/vansante/go-ffprobe.v2"
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
		// TODO: ObjectStore driver MUST receive a Reader instead of the entire file buffered in memory.
		contents, err := ioutil.ReadAll(secondaryReader)
		if err != nil {
			glog.Errorf("Error reading file from remote URL err=%q", err)
			return err
		}

		_, err = osSess.SaveData(egCtx, filePath, contents, nil, fileUploadTimeout)
		if err != nil {
			glog.Errorf("Error uploading file to object store filePath=%q err=%q", filePath, err)
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	// TODO: Register success on the API
	probeJson, _ := json.Marshal(probeData)
	glog.Info("Import task success! filePath=%q probeData=%+v probeJson=%q", filePath, probeData, probeJson)
	return nil
}
