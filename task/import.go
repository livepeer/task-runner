package task

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/livepeer-data/pkg/data"
	"golang.org/x/sync/errgroup"
	ffprobe "gopkg.in/vansante/go-ffprobe.v2"
)

const (
	fileUploadTimeout = 5 * time.Minute
	// TODO: decide file name
	videoFileName    = "video.mp4"
	metadataFileName = "video.json"
)

type fileMetadata struct {
	Ffprobe *ffprobe.ProbeData `json:"ffprobe"`
}

func TaskImport(tctx *TaskContext) (*data.ImportTaskOutput, error) {
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
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		err := fmt.Errorf("bad status code from import request: %d %s", resp.StatusCode, resp.Status)
		if resp.StatusCode < 500 {
			err = UnretriableError{err}
		}
		return nil, err
	}

	secondaryReader, pipe := io.Pipe()
	mainReader := io.TeeReader(resp.Body, pipe)

	var (
		eg, egCtx     = errgroup.WithContext(ctx)
		videoFilePath string
		metadata      *fileMetadata
	)
	eg.Go(func() error {
		defer pipe.Close()
		probeData, err := ffprobe.ProbeReader(egCtx, mainReader)
		if err != nil {
			return fmt.Errorf("error probing file: %w", err)
		}
		metadata = &fileMetadata{probeData}
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
	rawMetadata, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("error marshaling ffprobe data: %w", err)
	}
	metadataFilePath, err := osSess.SaveData(egCtx, metadataFileName, bytes.NewReader(rawMetadata), nil, fileUploadTimeout)
	if err != nil {
		return nil, fmt.Errorf("error saving metadata file: %w", err)
	}

	// TODO: hash file
	assetSpec, err := toAssetSpec(filename(req, resp), metadata.Ffprobe, nil)
	if err != nil {
		// TODO: Delete uploaded file
		return nil, err
	}

	return &data.ImportTaskOutput{
		VideoFilePath:    videoFilePath,
		MetadataFilePath: metadataFilePath,
		Metadata:         metadata,
		AssetSpec:        assetSpec,
	}, nil
}

func filename(req *http.Request, resp *http.Response) string {
	contentDisposition := resp.Header.Get("Content-Disposition")
	_, params, _ := mime.ParseMediaType(contentDisposition)
	if filename, ok := params["filename"]; ok {
		return filename
	}
	if base := path.Base(req.URL.Path); len(base) > 1 {
		return base
	}
	return ""
}
