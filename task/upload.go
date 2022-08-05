package task

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path"

	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
)

func TaskUpload(tctx *TaskContext) (*data.TaskOutput, error) {
	var (
		ctx        = tctx.Context
		playbackID = tctx.OutputAsset.PlaybackID
		step       = tctx.Step
		params     = *tctx.Task.Params.Import
		os         = tctx.OutputOSObj
	)
	url, err := getFileUrl(tctx.InputOSObj, params)
	if err != nil {
		return nil, fmt.Errorf("error building file URL: %v", err)
	}
	switch step {
	case "":
		uploadReq := clients.UploadVODRequest{
			Url:         url,
			CallbackUrl: fmt.Sprintf("%s/v1/catalyst/%s/%s?nextStep=finalize"), // TODO: Set the right path here
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
		return nil, nil
	case "finalize":
		var callback *clients.CatalystCallback
		if err := json.Unmarshal(tctx.StepInput, &callback); err != nil {
			return nil, fmt.Errorf("error parsing step input: %v", err)
		}
		if callback.Status != "success" {
			return nil, fmt.Errorf("unsucessful callback received. status=%v", callback.Status)
		}
		metadataFilePath, err := saveMetadataFile(tctx, tctx.outputOS, playbackID, callback)
		if err != nil {
			return nil, fmt.Errorf("error saving metadata file: %v", err)
		}
		var (
			assetSpec     = callback.Spec
			videoFilePath string
		)
		for _, output := range callback.Outputs {
			if output.Type == "object_store" {
				videoFilePath = output.Manifest
				// TODO: Save the output path in the asset somehow (internal field)?
			} else if output.Type == "ipfs_pinata" {
				asset := tctx.OutputAsset
				asset.AssetSpec = *assetSpec
				_, err := saveNFTMetadata(tctx, nil, asset, output.Manifest)
				if err != nil {
					return nil, fmt.Errorf("error saving NFT metadata: %v", err)
				}
				// TODO: Set IPFS hashes in the asset storage spec
			}
		}

		return &data.TaskOutput{
			Import: &data.ImportTaskOutput{
				VideoFilePath:    videoFilePath,
				MetadataFilePath: metadataFilePath,
				AssetSpec:        assetSpec,
			},
		}, nil
	}
	return nil, fmt.Errorf("unknown task step: %s", step)
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
