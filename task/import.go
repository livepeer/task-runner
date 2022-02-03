package task

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"time"

	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-livepeer/drivers"
	"github.com/livepeer/livepeer-data/pkg/data"
	"golang.org/x/sync/errgroup"
)

const (
	fileUploadTimeout = 5 * time.Minute
	// TODO: decide file name
	videoFileName    = "video.mp4"
	metadataFileName = "video.json"
)

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

	eg, egCtx := errgroup.WithContext(ctx)
	var (
		videoFilePath, metadataFilePath string
		assetSpec                       *livepeerAPI.AssetSpec
		metadata                        *FileMetadata
	)
	eg.Go(func() (err error) {
		assetSpec, metadata, err = Probe(egCtx, filename(req, resp), mainReader)
		pipe.CloseWithError(err)
		if err != nil {
			return err
		}
		metadataFilePath, err = saveMetadataFile(egCtx, osSess, metadata)
		return err
	})
	eg.Go(func() (err error) {
		videoFilePath, err = osSess.SaveData(egCtx, videoFileName, secondaryReader, nil, fileUploadTimeout)
		if err != nil {
			return fmt.Errorf("error uploading file=%q to object store: %w", videoFileName, err)
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		// TODO: Delete the uploaded file
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

func saveMetadataFile(ctx context.Context, osSess drivers.OSSession, metadata *FileMetadata) (string, error) {
	raw, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("error marshaling file metadat: %w", err)
	}
	path, err := osSess.SaveData(ctx, metadataFileName, bytes.NewReader(raw), nil, fileUploadTimeout)
	if err != nil {
		return "", fmt.Errorf("error saving metadata file: %w", err)
	}
	return path, nil
}
