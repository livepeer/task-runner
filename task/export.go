package task

import (
	"context"
	"fmt"
	"io"
	"strings"

	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
)

var DefaultClient = clients.BaseClient{}

func TaskExport(tctx *TaskContext) (*data.TaskOutput, error) {
	var (
		ctx    = tctx.Context
		asset  = tctx.InputAsset
		osSess = tctx.inputOS
		params = *tctx.Task.Params.Export
	)
	file, err := osSess.ReadData(ctx, videoFileName(asset.PlaybackID))
	if err != nil {
		return nil, err
	}
	defer file.Body.Close()

	ctx, cancel := context.WithTimeout(ctx, fileUploadTimeout)
	defer cancel()
	output, err := uploadFile(ctx, tctx.ipfs, params, asset, file.Body)
	if err != nil {
		return nil, err
	}
	return &data.TaskOutput{Export: output}, nil
}

func uploadFile(ctx context.Context, ipfs clients.IPFS, params livepeerAPI.ExportTaskParams, asset *livepeerAPI.Asset, content io.Reader) (*data.ExportTaskOutput, error) {
	contentType := "video/" + asset.VideoSpec.Format
	if c := params.Custom; c != nil {
		req := clients.Request{
			Method:      strings.ToUpper(c.Method),
			URL:         c.URL,
			Headers:     c.Headers,
			Body:        content,
			ContentType: contentType,
		}
		if req.Method == "" {
			req.Method = "PUT"
		}
		if err := DefaultClient.DoRequest(ctx, req, nil); err != nil {
			if httpErr, ok := err.(*clients.HTTPStatusError); ok && httpErr.Status < 500 {
				err = UnretriableError{err}
			}
			return nil, fmt.Errorf("error on export request: %w", err)
		}
		return &data.ExportTaskOutput{}, nil
	}

	if p := params.IPFS.Pinata; p != nil {
		if p.JWT != "" {
			ipfs = clients.NewPinataClientJWT(p.JWT)
		} else {
			ipfs = clients.NewPinataClientAPIKey(p.APIKey, p.APISecret)
		}
	}
	cid, metadata, err := ipfs.PinContent(ctx, asset.PlaybackID, contentType, content)
	if err != nil {
		return nil, err
	}
	return &data.ExportTaskOutput{
		IPFS: &data.IPFSExportInfo{
			VideoFileCID: cid,
			// TODO: Pin some default metadata as well
			ERC1155MetadataCID: "",
			Internal: map[string]interface{}{
				"pinata": metadata,
			},
		},
	}, nil
}
