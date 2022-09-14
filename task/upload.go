package task

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
)

func TaskUpload(tctx *TaskContext) (*data.TaskOutput, error) {
	var (
		ctx        = tctx.Context
		playbackID = tctx.OutputAsset.PlaybackID
		step       = tctx.Step
		params     = *tctx.Task.Params.Upload
	)
	inUrl, err := getFileUrl(tctx.InputOSObj, params)
	if err != nil {
		return nil, fmt.Errorf("error building file URL: %v", err)
	}
	switch step {
	case "":
		outURL, err := url.Parse(tctx.OutputOSObj.URL)
		if err != nil {
			return nil, fmt.Errorf("error parsing object store URL: %v", err)
		}
		uploadReq := clients.UploadVODRequest{
			Url:         inUrl,
			CallbackUrl: tctx.catalyst.CatalystHookURL(tctx.Task.ID, "finalize"),
			Mp4Output:   true,
			OutputLocations: []clients.OutputLocation{
				{
					Type: "object_store",
					URL:  outURL.JoinPath(videoFileName(playbackID)).String(),
					Outputs: &clients.OutputsRequest{
						SourceMp4: true,
					},
				},
				{
					Type: "object_store",
					URL:  outURL.JoinPath(hlsRootPlaylistFileName(playbackID)).String(),
					Outputs: &clients.OutputsRequest{
						SourceSegments:     true,
						TranscodedSegments: true,
					},
				},
			},
		}
		if tctx.OutputAsset.Storage.IPFS != nil {
			uploadReq.OutputLocations = append(uploadReq.OutputLocations, clients.OutputLocation{
				Type:            "ipfs_pinata",
				PinataAccessKey: tctx.PinataAccessToken,
				Outputs: &clients.OutputsRequest{
					SourceMp4: true,
				},
			})
		}
		if err := tctx.catalyst.UploadVOD(ctx, uploadReq); err != nil {
			return nil, fmt.Errorf("failed to call catalyst: %v", err)
		}
		err = tctx.delayTaskStep(ctx, tctx.Task.ID, "checkCatalyst", nil)
		if err != nil {
			return nil, fmt.Errorf("failed scheduling catalyst healthcheck: %v", err)
		}
		return nil, ErrYieldExecution
	case "checkCatalyst":
		task := tctx.Task
		if task.Status.Phase != "running" {
			return nil, ErrYieldExecution
		}
		updatedAt := data.NewUnixMillisTime(task.Status.UpdatedAt)
		if updateAge := time.Since(updatedAt.Time); updateAge > time.Minute {
			return nil, fmt.Errorf("catalyst task lost (last update %s ago)", updateAge)
		}
		err := tctx.delayTaskStep(ctx, task.ID, "checkCatalyst", nil)
		if err != nil {
			return nil, fmt.Errorf("failed to schedule next check: %v", err)
		}
		return nil, ErrYieldExecution
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

		assetSpec, videoFilePath, err := assetSpecFromCatalystCallback(tctx, callback)
		if err != nil {
			return nil, fmt.Errorf("error processing catalyst callback: %v", err)
		}

		return &data.TaskOutput{
			Upload: &data.UploadTaskOutput{
				VideoFilePath:    videoFilePath,
				MetadataFilePath: metadataFilePath,
				AssetSpec:        assetSpec,
			},
		}, nil
	}
	return nil, fmt.Errorf("unknown task step: %s", step)
}

func getFileUrl(os *api.ObjectStore, params api.UploadTaskParams) (string, error) {
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

func assetSpecFromCatalystCallback(tctx *TaskContext, callback *clients.CatalystCallback) (*api.AssetSpec, string, error) {
	assetSpec := &api.AssetSpec{
		Name: tctx.OutputAsset.Name,
		Type: "video",
		Size: uint64(callback.InputVideo.SizeBytes),
		VideoSpec: &api.AssetVideoSpec{
			Format:      callback.InputVideo.Format,
			DurationSec: callback.InputVideo.Duration,
			Bitrate:     0,
			Tracks:      make([]*api.AssetTrack, len(callback.InputVideo.Tracks)),
		},
		Storage: tctx.OutputAsset.Storage,
	}
	for i, track := range callback.InputVideo.Tracks {
		assetSpec.VideoSpec.Bitrate += float64(track.Bitrate)
		assetSpec.VideoSpec.Tracks[i] = &api.AssetTrack{
			Type:        track.Type,
			Codec:       track.Codec,
			StartTime:   track.StartTimeSec,
			DurationSec: track.DurationSec,
			Bitrate:     float64(track.Bitrate),

			Width:       track.VideoTrack.Width,
			Height:      track.VideoTrack.Height,
			PixelFormat: track.VideoTrack.PixelFormat,
			FPS:         float64(track.VideoTrack.FPS) / 1000,

			Channels:   track.AudioTrack.Channels,
			SampleRate: track.AudioTrack.SampleRate,
		}
	}

	videoFilePath := ""
	for _, output := range callback.Outputs {
		if output.Type == "object_store" {
			videoFilePath = output.Manifest
		} else if output.Type == "ipfs_pinata" {
			ipfs := *tctx.OutputAsset.Storage.IPFS
			ipfs.CID = output.Manifest
			metadataCID, err := saveNFTMetadata(tctx, tctx.ipfs, tctx.OutputAsset, ipfs.CID,
				ipfs.Spec.NFTMetadataTemplate, ipfs.Spec.NFTMetadata, tctx.ExportTaskConfig)
			if err != nil {
				return nil, "", fmt.Errorf("error saving NFT metadata: %v", err)
			}
			ipfs.NFTMetadata = &api.IPFSFileInfo{CID: metadataCID}
			assetSpec.Storage.IPFS = &ipfs
		}
	}
	return assetSpec, videoFilePath, nil
}
