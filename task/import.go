package task

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-api-client/logs"
	"github.com/livepeer/go-tools/drivers"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/webcrypto"
)

const IPFS_PREFIX = "ipfs://"
const ARWEAVE_PREFIX = "ar://"

type ImportTaskConfig struct {
	// Ordered list of IPFS gateways (includes /ipfs/ suffix) to import assets from
	ImportIPFSGatewayURLs []*url.URL
}

func TaskImport(tctx *TaskContext) (*TaskHandlerOutput, error) {
	var (
		ctx        = tctx.Context
		playbackID = tctx.OutputAsset.PlaybackID
		params     = *tctx.Task.Params.Import
		osSess     = tctx.outputOS // Import deals with outputOS only (URL -> ObjectStorage)
	)
	filename, size, contents, err := getFile(ctx, osSess, tctx.ImportTaskConfig, params)
	if err != nil {
		return nil, err
	}
	defer contents.Close()

	// Download the file to local disk (or memory).
	input := tctx.Progress.TrackReader(contents, size, 0.09)
	sizeInt := int64(size)
	sourceFile, err := readFile(filename, &sizeInt, input)
	if err != nil {
		return nil, err
	}
	defer sourceFile.Close()

	// Probe metadata from the source file and save it to object store.
	input = tctx.Progress.TrackReader(sourceFile, size, 0.11)
	isRecording := tctx.Params.Import.RecordedSessionID != ""
	metadata, err := Probe(ctx, tctx.OutputAsset.ID, filename, input, !isRecording)
	if err != nil {
		return nil, err
	}
	metadataFilePath, _, err := saveMetadataFile(ctx, osSess, playbackID, metadata)
	if err != nil {
		return nil, err
	}

	// Save source file to object store.
	_, err = sourceFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("error seeking to start of source file: %w", err)
	}
	input = tctx.Progress.TrackReader(sourceFile, size, 0.2)
	fullPath := videoFileName(playbackID)
	videoFilePath, err := osSess.SaveData(ctx, fullPath, input, nil, fileUploadTimeout)
	if err != nil {
		return nil, fmt.Errorf("error uploading file=%q to object store: %w", fullPath, err)
	}
	glog.Infof("Saved file=%s to url=%s", fullPath, videoFilePath)

	_, err = sourceFile.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("error seeking to start of source file: %w", err)
	}
	playbackRecordingID, err := prepareImportedAsset(tctx, metadata, sourceFile)
	if err != nil {
		return nil, fmt.Errorf("error preparing asset: %w", err)
	}
	assetSpec := *metadata.AssetSpec
	assetSpec.PlaybackRecordingID = playbackRecordingID
	return &TaskHandlerOutput{
		TaskOutput: &data.TaskOutput{Import: &data.UploadTaskOutput{
			VideoFilePath:    videoFilePath,
			MetadataFilePath: metadataFilePath,
			AssetSpec:        assetSpec,
		}}}, nil
}

func getFile(ctx context.Context, osSess drivers.OSSession, cfg ImportTaskConfig, params api.UploadTaskParams) (name string, size uint64, content io.ReadCloser, err error) {
	name, size, content, err = getFileRaw(ctx, osSess, cfg, params)
	if err != nil || params.Encryption.Key == "" {
		return
	}

	switch strings.ToLower(params.Encryption.Algorithm) {
	case "", "aes-cbc":
		glog.V(logs.VVERBOSE).Infof("Decrypting file with key file=%s keyHash=%x", params.URL, sha256.Sum256([]byte(params.Encryption.Key)))
		decrypted, err := webcrypto.DecryptAESCBC(content, params.Encryption.Key)
		if err != nil {
			content.Close()
			return "", 0, nil, fmt.Errorf("failed to decrypt input file: %w", err)
		}

		glog.V(logs.VVERBOSE).Infof("Returning decrypted stream for file=%s", params.URL)
		return name, size, decrypted, nil
	default:
		return "", 0, nil, fmt.Errorf("unknown encryption algorithm: %s", params.Encryption.Algorithm)
	}
}

func getFileRaw(ctx context.Context, osSess drivers.OSSession, cfg ImportTaskConfig, params api.UploadTaskParams) (name string, size uint64, content io.ReadCloser, err error) {
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

	if strings.HasPrefix(params.URL, IPFS_PREFIX) {
		cid := strings.TrimPrefix(params.URL, IPFS_PREFIX)
		return getFileIPFS(ctx, cfg.ImportIPFSGatewayURLs, cid)
	}

	if strings.HasPrefix(params.URL, ARWEAVE_PREFIX) {
		txID := strings.TrimPrefix(params.URL, ARWEAVE_PREFIX)
		// arweave.net is the main gateway for Arweave right now
		// In the future, given more gateways, we can pass a list of gateway URLs similar to what we do for IPFS
		gatewayUrl := "https://arweave.net/" + txID
		return getFileWithUrl(ctx, gatewayUrl)
	}

	return getFileWithUrl(ctx, params.URL)
}

func getFileIPFS(ctx context.Context, gateways []*url.URL, cid string) (name string, size uint64, content io.ReadCloser, err error) {
	for _, gateway := range gateways {
		url := gateway.JoinPath(cid).String()
		name, size, content, err = getFileWithUrl(ctx, url)
		if err == nil {
			return name, size, content, nil
		}
		glog.Infof("Failed to get file from IPFS cid=%v url=%v err=%v", cid, gateway, err)
	}

	return "", 0, nil, err
}

func getFileWithUrl(ctx context.Context, url string) (name string, size uint64, content io.ReadCloser, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
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

func prepareImportedAsset(tctx *TaskContext, metadata *FileMetadata, sourceFile io.ReadSeekCloser) (string, error) {
	if sessID := tctx.Params.Import.RecordedSessionID; sessID != "" {
		return sessID, nil
	}

	playbackRecordingID, err := Prepare(tctx, metadata.AssetSpec, sourceFile)
	if err != nil {
		glog.Errorf("Error preparing file assetId=%s taskType=import err=%q", tctx.OutputAsset.ID, err)
		return "", err
	}
	return playbackRecordingID, nil
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

func saveMetadataFile(ctx context.Context, osSess drivers.OSSession, playbackID string, metadata interface{}) (string, string, error) {
	path := metadataFileName(playbackID)
	raw, err := json.Marshal(metadata)
	if err != nil {
		return "", "", fmt.Errorf("error marshaling file metadat: %w", err)
	}
	fullPath, err := osSess.SaveData(ctx, path, bytes.NewReader(raw), nil, fileUploadTimeout)
	if err != nil {
		return "", "", fmt.Errorf("error saving metadata file: %w", err)
	}
	return fullPath, path, nil
}
