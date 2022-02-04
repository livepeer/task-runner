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
	videoFileName     = "video"
	metadataFileName  = "video.json"
)

func TaskImportOrDirectUpload(tctx *TaskContext) (*data.ImportTaskOutput, error) {
	var (
		ctx        = tctx.Context
		playbackID = tctx.Asset.PlaybackID
		params     = tctx.Task.Params
		osSess     = tctx.osSession
	)
	filename, contents, err := getFile(ctx, osSess, params)
	if err != nil {
		return nil, err
	}
	defer contents.Close()

	secondaryReader, pipe := io.Pipe()
	mainReader := io.TeeReader(contents, pipe)

	eg, egCtx := errgroup.WithContext(ctx)
	var (
		videoFilePath, metadataFilePath string
		metadata                        *FileMetadata
	)
	eg.Go(func() (err error) {
		metadata, err = Probe(egCtx, filename, mainReader)
		pipe.CloseWithError(err)
		if err != nil {
			return err
		}
		metadataFilePath, err = saveMetadataFile(egCtx, osSess, metadata)
		return err
	})
	eg.Go(func() (err error) {
		fullPath := path.Join(playbackID, videoFileName)
		videoFilePath, err = osSess.SaveData(egCtx, fullPath, secondaryReader, nil, fileUploadTimeout)
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
		AssetSpec:        metadata.AssetSpec,
	}, nil
}

func getFile(ctx context.Context, osSess drivers.OSSession, params livepeerAPI.TaskParams) (string, io.ReadCloser, error) {
	if params.DirectUpload != nil {
		// TODO: We should simply "move" the file in case of direct import since we
		// know the file is already in the object store. Independently, we also have
		// to delete the uploaded file after copying to the new location.
		fileInfo, err := osSess.ReadData(ctx, params.DirectUpload.ObjectKey)
		if err != nil {
			return "", nil, UnretriableError{fmt.Errorf("error reading direct uploaded file: %w", err)}
		}
		return fileInfo.FileInfo.Name, fileInfo.Body, nil
	} else if params.Import == nil {
		return "", nil, fmt.Errorf("no import or direct upload params: %+v", params)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", params.Import.URL, nil)
	if err != nil {
		return "", nil, UnretriableError{fmt.Errorf("error creating http request: %w", err)}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("error on import request: %w", err)
	}
	if resp.StatusCode >= 300 {
		resp.Body.Close()
		err := fmt.Errorf("bad status code from import request: %d %s", resp.StatusCode, resp.Status)
		if resp.StatusCode < 500 {
			err = UnretriableError{err}
		}
		return "", nil, err
	}
	return filename(req, resp), resp.Body, nil
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
