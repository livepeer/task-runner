package task

import (
	"encoding/json"
	"fmt"
	"net/url"
	"path"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
)

// Feature flag whether to use Catalyst's IPFS support or not. Should be tunable
// via a CLI flag for easy configuration on deployment.
var FlagCatalystSupportsIPFS = false

type OutputName string

var (
	OutputNameOSSourceMP4   = OutputName("source_mp4")
	OutputNameOSPlaylistHLS = OutputName("playlist_hls")
	OutputNameIPFSSourceMP4 = OutputName("ipfs_source_mp4")
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
		return nil, fmt.Errorf("error building file URL: %w", err)
	}
	switch step {
	case "":
		_, outputLocations, err := assetOutputLocations(tctx)
		if err != nil {
			return nil, err
		}
		uploadReq := clients.UploadVODRequest{
			Url:             inUrl,
			CallbackUrl:     tctx.catalyst.CatalystHookURL(tctx.Task.ID, "finalize"),
			OutputLocations: outputLocations,
		}
		if err := tctx.catalyst.UploadVOD(ctx, uploadReq); err != nil {
			return nil, fmt.Errorf("failed to call catalyst: %w", err)
		}
		err = tctx.delayTaskStep(ctx, tctx.Task.ID, "checkCatalyst", nil)
		if err != nil {
			return nil, fmt.Errorf("failed scheduling catalyst healthcheck: %w", err)
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
			return nil, fmt.Errorf("failed to schedule next check: %w", err)
		}
		return nil, ErrYieldExecution
	case "finalize":
		var callback *clients.CatalystCallback
		if err := json.Unmarshal(tctx.StepInput, &callback); err != nil {
			return nil, fmt.Errorf("error parsing step input: %w", err)
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

	outputNames, outputReqs, err := assetOutputLocations(tctx)
	if err != nil {
		return nil, "", fmt.Errorf("error getting asset output requests: %w", err)
	}

	var videoFilePath, catalystCid string
	for idx, output := range callback.Outputs {
		outName := outputNames[idx]
		outReq := outputReqs[idx]
		if output.Type != outReq.Type {
			return nil, "", fmt.Errorf("output type mismatch: %s != %s", output.Type, outReq.Type)
		}
		switch outName {
		case OutputNameOSSourceMP4:
			if len(output.Videos) != 1 {
				return nil, "", fmt.Errorf("unexpected number of videos in source MP4 output: %d", len(output.Videos))
			}
			video := output.Videos[0]
			if video.Type != "mp4" {
				return nil, "", fmt.Errorf("unexpected video type in source MP4 output: %s", output.Videos[0].Type)
			}
			videoFilePath = video.Location
		case OutputNameOSPlaylistHLS:
			// TODO: We don't really know how to handle this yet. Just log.
			glog.Infof("Received OS HLS playlist output! manifest=%q output=%+v", output.Manifest, output)
		case OutputNameIPFSSourceMP4:
			catalystCid = output.Manifest
		default:
			return nil, "", fmt.Errorf("unknown output name=%q for output=%+v", outName, output)
		}
	}

	if tctx.OutputAsset.Storage.IPFS != nil {
		var (
			ipfs = *tctx.OutputAsset.Storage.IPFS
			cid  string
		)
		if FlagCatalystSupportsIPFS {
			cid = catalystCid
		} else {
			// TODO: Remove this branch once we have reliable catalyst IPFS support
			var (
				ipfs        = *tctx.OutputAsset.Storage.IPFS
				playbackID  = tctx.OutputAsset.PlaybackID
				contentType = "video/" + tctx.OutputAsset.VideoSpec.Format
			)
			file, err := tctx.outputOS.ReadData(tctx, videoFilePath)
			if err != nil {
				return nil, "", fmt.Errorf("error reading exported video file: %w", err)
			}
			defer file.Body.Close()
			cid, _, err = tctx.ipfs.PinContent(tctx, "asset-"+playbackID, contentType, file.Body)
			if err != nil {
				return nil, "", fmt.Errorf("error pinning file to IPFS: %w", err)
			}
			ipfs.CID = cid
		}
		metadataCID, err := saveNFTMetadata(tctx, tctx.ipfs, tctx.OutputAsset, cid,
			ipfs.Spec.NFTMetadataTemplate, ipfs.Spec.NFTMetadata, tctx.ExportTaskConfig)
		if err != nil {
			return nil, "", fmt.Errorf("error pining NFT metadata to IPFS: %w", err)
		}
		ipfs.NFTMetadata = &api.IPFSFileInfo{CID: metadataCID}
		assetSpec.Storage.IPFS = &ipfs
	}
	return assetSpec, videoFilePath, nil
}

func assetOutputLocations(tctx *TaskContext) ([]OutputName, []clients.OutputLocation, error) {
	var (
		asset             = tctx.OutputAsset
		outOS             = tctx.OutputOSObj
		pinataAccessToken = tctx.PinataAccessToken
	)
	outURL, err := url.Parse(outOS.URL)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing object store URL: %w", err)
	}
	names, locations :=
		[]OutputName{OutputNameIPFSSourceMP4, OutputNameOSPlaylistHLS},
		[]clients.OutputLocation{
			{
				Type: "object_store",
				URL:  outURL.JoinPath(videoFileName(asset.PlaybackID)).String(),
				Outputs: &clients.OutputsRequest{
					SourceMp4: true,
				},
			},
			{
				Type: "object_store",
				URL:  outURL.JoinPath(hlsRootPlaylistFileName(asset.PlaybackID)).String(),
				Outputs: &clients.OutputsRequest{
					SourceSegments:     true,
					TranscodedSegments: true,
				},
			},
		}
	if FlagCatalystSupportsIPFS && asset.Storage.IPFS != nil {
		// TODO: This interface is likely going to change so that pinata is just a
		// `object_store` output
		names, locations =
			append(names, OutputNameIPFSSourceMP4),
			append(locations, clients.OutputLocation{
				Type:            "ipfs_pinata",
				PinataAccessKey: pinataAccessToken,
				Outputs: &clients.OutputsRequest{
					SourceMp4: true,
				},
			})
	}
	return names, locations, nil
}
