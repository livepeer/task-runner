package task

import (
	"errors"
	"fmt"
	"net/url"
	"path"

	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
)

func TaskUpload(tctx *TaskContext) (*data.TaskOutput, error) {
	var (
		ctx    = tctx.Context
		params = *tctx.Task.Params.Import
		os     = tctx.OutputOSObj
	)
	url, err := getFileUrl(tctx.InputOSObj, params)
	if err != nil {
		return nil, fmt.Errorf("error building file URL: %v", err)
	}
	uploadReq := clients.UploadVODRequest{
		Url:         url,
		CallbackUrl: fmt.Sprintf("%s/v1/catalyst/%s/%s"), // TODO: Set the right path here
		Mp4Output:   true,
		OutputLocations: []clients.OutputLocation{
			{
				Type: "object_store",
				URL:  os.URL,
			},
			{
				Type:            "ipfs_pinata", // TODO: Add this based on asset.storage
				PinataAccessKey: tctx.PinataAccessToken,
			},
		},
	}
	if err := tctx.catalyst.UploadVOD(ctx, uploadReq); err != nil {
		return nil, fmt.Errorf("failed to call catalyst: %v", err)
	}
	return nil, errors.New("catalyst callback not implemented")
}

func getFileUrl(os *api.ObjectStore, params api.ImportTaskParams) (string, error) {
	if params.UploadedObjectKey != "" {
		u, err := url.Parse(os.PublicURL)
		if err != nil {
			return "", err
		}
		u.Path = path.Join(u.Path, params.UploadedObjectKey)
		return u.String(), nil
	}
	if params.URL != "" {
		return params.URL, nil
	}
	return "", fmt.Errorf("no URL or uploaded object key specified")
}
