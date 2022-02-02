package task

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/golang/glog"
	"golang.org/x/sync/errgroup"
	ffprobe "gopkg.in/vansante/go-ffprobe.v2"
)

const (
	fileUploadTimeout = 5 * time.Minute
	// TODO: decide file name
	videoFileName    = "video.mp4"
	metadataFileName = "video.json"
)

func TaskImport(tctx *TaskContext) (interface{}, error) {
	var (
		ctx    = tctx.Context
		srcURL = tctx.Params.Import.URL
		osSess = tctx.osSession
	)

	req, err := http.NewRequestWithContext(ctx, "GET", srcURL, nil)
	if err != nil {
		glog.Errorf("Error creating http request url=%s err=%q", srcURL, err)
		return nil, UnretriableError{err}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error on import request: %w", err)
	} else if resp.StatusCode >= 300 {
		err := fmt.Errorf("bad status code from import request: %d %s", resp.StatusCode, resp.Status)
		if resp.StatusCode < 500 {
			err = UnretriableError{err}
		}
		return nil, err
	}

	secondaryReader, pipe := io.Pipe()
	mainReader := io.TeeReader(resp.Body, pipe)

	eg, egCtx := errgroup.WithContext(ctx)
	var (
		probeData                       *ffprobe.ProbeData
		metadata                        []byte
		metadataFilePath, videoFilePath string
	)
	eg.Go(func() error {
		defer pipe.Close()
		var err error
		probeData, err = ffprobe.ProbeReader(egCtx, mainReader)
		if err != nil {
			return fmt.Errorf("error probing file: %w", err)
		}
		metadata, err = json.Marshal(map[string]interface{}{"ffprobe": probeData})
		if err != nil {
			return fmt.Errorf("error marshaling ffprobe data: %w", err)
		}
		metadataFilePath, err = osSess.SaveData(egCtx, metadataFileName, bytes.NewReader(metadata), nil, fileUploadTimeout)
		if err != nil {
			return fmt.Errorf("error saving metadata file: %w", err)
		}
		return nil
	})
	eg.Go(func() error {
		videoFilePath, err = osSess.SaveData(egCtx, videoFileName, secondaryReader, nil, fileUploadTimeout)
		if err != nil {
			return fmt.Errorf("error uploading file=%q to object store: %w", videoFileName, err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	// TODO: validate if video is really mp4 / supported format

	glog.V(5).Info("Import task success! id=%s videoFilePath=%q metadataFilePath=%q metadata=%q",
		tctx.Task.ID, videoFilePath, metadataFilePath, probeData, metadata)
	// TODO: Return a typed/processed response here instead of raw probe output
	return probeData, nil
}
