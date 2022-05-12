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
	"strings"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-livepeer/drivers"
	"github.com/livepeer/livepeer-data/pkg/data"
	"golang.org/x/sync/errgroup"
)

func TaskImport(tctx *TaskContext) (*data.TaskOutput, error) {
	var (
		ctx        = tctx.Context
		playbackID = tctx.OutputAsset.PlaybackID
		params     = *tctx.Task.Params.Import
		osSess     = tctx.outputOS // Import deals with outputOS only (URL -> ObjectStorage)
	)
	filename, size, contents, err := getFile(ctx, osSess, params)
	if err != nil {
		return nil, err
	}
	defer contents.Close()

	secondaryReader, pipe := io.Pipe()
	mainReader := NewReadCounter(io.TeeReader(contents, pipe))

	eg, egCtx := errgroup.WithContext(ctx)
	var (
		videoFilePath, metadataFilePath, fullPath string
		metadata                                  *FileMetadata
	)
	go ReportProgress(egCtx, tctx.lapi, tctx.Task.ID, size, mainReader.Count)
	// Probe the source file to retrieve metadata
	eg.Go(func() (err error) {
		metadata, err = Probe(egCtx, filename, mainReader)
		pipe.CloseWithError(err)
		if err != nil {
			return err
		}
		metadataFilePath, err = saveMetadataFile(egCtx, osSess, playbackID, metadata)
		return err
	})
	// Save source file to our storage
	eg.Go(func() (err error) {
		fullPath = videoFileName(playbackID)
		videoFilePath, err = osSess.SaveData(egCtx, fullPath, secondaryReader, nil, fileUploadTimeout)
		if err != nil {
			return fmt.Errorf("error uploading file=%q to object store: %w", fullPath, err)
		}
		glog.Infof("Saved file=%s to url=%s", fullPath, videoFilePath)
		return nil
	})
	// Wait for async goroutines finish to run prepare
	if err := eg.Wait(); err != nil {
		// TODO: Delete the source file
		return nil, err
	}
	// Download our imported output file
	fileInfoReader, err := osSess.ReadData(ctx, fullPath)
	if err != nil {
		return nil, fmt.Errorf("error reading imported file from output OS path=%s err=%w", fullPath, err)
	}
	defer fileInfoReader.Body.Close()
	importedFile, err := readFile(fileInfoReader)
	if err != nil {
		return nil, err
	}
	defer importedFile.Close()
	playbackRecordingId := ""
	if strings.Contains(params.URL, "/recordings/") {
		// RecordStream on output file for HLS playback
		playbackRecordingId, err = Prepare(tctx, metadata.AssetSpec, importedFile)
		if err != nil {
			glog.Errorf("error preparing imported file assetId=%s err=%q", tctx.OutputAsset.ID, err)
		}
	}
	assetSpec := *metadata.AssetSpec
	assetSpec.PlaybackRecordingID = playbackRecordingId
	return &data.TaskOutput{Import: &data.ImportTaskOutput{
		VideoFilePath:    videoFilePath,
		MetadataFilePath: metadataFilePath,
		AssetSpec:        assetSpec,
	}}, nil
}

func getFile(ctx context.Context, osSess drivers.OSSession, params livepeerAPI.ImportTaskParams) (name string, size uint64, content io.ReadCloser, err error) {
	if upedObjKey := params.UploadedObjectKey; upedObjKey != "" {
		// TODO: We should simply "move" the file in case of direct import since we
		// know the file is already in the object store. Independently, we also have
		// to delete the uploaded file after copying to the new location.
		fileInfo, err := osSess.ReadData(ctx, upedObjKey)
		if err != nil {
			return "", 0, nil, UnretriableError{fmt.Errorf("error reading direct uploaded file: %w", err)}
		}
		if fileInfo.Size != nil && *fileInfo.Size > 0 {
			size = uint64(*fileInfo.Size)
		}
		return fileInfo.FileInfo.Name, size, fileInfo.Body, nil
	} else if params.URL == "" {
		return "", 0, nil, fmt.Errorf("no import URL or direct upload object key: %+v", params)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", params.URL, nil)
	if err != nil {
		return "", 0, nil, UnretriableError{fmt.Errorf("error creating http request: %w", err)}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, nil, fmt.Errorf("error on import request: %w", err)
	}
	if resp.StatusCode >= 300 {
		resp.Body.Close()
		err := fmt.Errorf("bad status code from import request: %d %s", resp.StatusCode, resp.Status)
		if resp.StatusCode < 500 {
			err = UnretriableError{err}
		}
		return "", 0, nil, err
	}
	if resp.ContentLength > 0 {
		size = uint64(resp.ContentLength)
	}
	return filename(req, resp), size, resp.Body, nil
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

func saveMetadataFile(ctx context.Context, osSess drivers.OSSession, playbackID string, metadata *FileMetadata) (string, error) {
	fullPath := metadataFileName(playbackID)
	raw, err := json.Marshal(metadata)
	if err != nil {
		return "", fmt.Errorf("error marshaling file metadat: %w", err)
	}
	path, err := osSess.SaveData(ctx, fullPath, bytes.NewReader(raw), nil, fileUploadTimeout)
	if err != nil {
		return "", fmt.Errorf("error saving metadata file: %w", err)
	}
	return path, nil
}
