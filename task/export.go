package task

import (
	"context"
	"errors"

	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
)

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

	if params.IPFS == nil {
		// TODO: Add support for raw URL export
		return nil, errors.New("only ipfs supported yet")
	}

	ctx, cancel := context.WithTimeout(ctx, fileUploadTimeout)
	defer cancel()

	ipfs := tctx.ipfs
	if p := params.IPFS.Pinata; p != nil {
		if p.JWT != "" {
			ipfs = clients.NewPinataClientJWT(p.JWT)
		} else {
			ipfs = clients.NewPinataClientAPIKey(p.APIKey, p.APISecret)
		}
	}
	cid, metadata, err := ipfs.PinContent(ctx, asset.PlaybackID, "video/"+asset.VideoSpec.Format, file.Body)
	if err != nil {
		return nil, err
	}
	return &data.TaskOutput{Export: &data.ExportTaskOutput{
		IPFS: &data.IPFSExportInfo{
			VideoFileCID: cid,
			// TODO: Pin some default metadata as well
			ERC1155MetadataCID: "",
			Internal: map[string]interface{}{
				"pinata": metadata,
			},
		},
	}}, nil
}
