package task

import (
	"context"
	"errors"

	"github.com/livepeer/livepeer-data/pkg/data"
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
	cid, metadata, err := tctx.ipfs.PinContent(ctx, asset.PlaybackID, "video/"+asset.VideoSpec.Format, file.Body)
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
