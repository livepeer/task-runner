package task

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/golang/glog"
	"golang.org/x/sync/errgroup"
	ffprobe "gopkg.in/vansante/go-ffprobe.v2"
)

const fileUploadTimeout = 5 * time.Minute

func TaskImport(tctx *TaskContext) (interface{}, error) {
	var (
		ctx    = tctx.Context
		srcURL = tctx.Params.Import.URL
		osSess = tctx.osSession
		// TODO: Re-evaluate this file path. Maybe omit the user ID and/or use a playbackID instead of asset ID (primary key)?
		filePath = path.Join(tctx.Asset.UserID, tctx.Asset.ID, "video.mp4")
	)

	req, err := http.NewRequestWithContext(ctx, "GET", srcURL, nil)
	if err != nil {
		glog.Errorf("Error creating http request url=%s err=%q", srcURL, err)
		return nil, UnretriableError{err}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, UnretriableError{fmt.Errorf("error on import request: %w", err)}
	} else if resp.StatusCode >= 300 {
		return nil, UnretriableError{fmt.Errorf("bad status code from import request: %d %s", resp.StatusCode, resp.Status)}
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
		return nil, err
	}

	probeJson, _ := json.Marshal(probeData)
	glog.Info("Import task success! filePath=%q probeData=%+v probeJson=%q", filePath, probeData, probeJson)
	return probeData, nil
}
