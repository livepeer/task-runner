package task

import (
	"bytes"
	"fmt"
	"github.com/livepeer/livepeer-data/pkg/data"
)

func TaskExportData(tctx *TaskContext) (*TaskHandlerOutput, error) {
	var (
		params       = tctx.Params.ExportData
		customParams = params.Custom
		ipfsParams   = params.IPFS
	)
	content := bytes.NewReader(params.Content)
	name := fmt.Sprintf("%s-%s", params.Type, params.ID)
	contentType := "application/json"
	cid, _, err := uploadFile(tctx, customParams, ipfsParams, name, content, contentType)
	if err != nil {
		return nil, err
	}

	return &TaskHandlerOutput{
		TaskOutput: &data.TaskOutput{
			ExportData: &data.ExportDataTaskOutput{
				IPFS: &data.IPFSExportDataInfo{
					CID: cid,
				},
			},
		},
	}, nil
}
