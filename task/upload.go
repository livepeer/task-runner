package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
)

var (
	// Feature flag whether to use Catalyst's IPFS support or not.
	FlagCatalystSupportsIPFS = false
	// Feature flag whether to use Catalyst for copying source file to object store.
	FlagCatalystCopiesSourceFile = false
	// Feature flag whether Catalyst is able to generate all required probe info.
	FlagCatalystProbesFile = false
)

type OutputName string

var (
	OutputNameOSSourceMP4   = OutputName("source_mp4")
	OutputNameOSPlaylistHLS = OutputName("playlist_hls")
	OutputNameIPFSSourceMP4 = OutputName("ipfs_source_mp4")
)

func TaskUpload(tctx *TaskContext) (*data.TaskOutput, error) {
	var (
		ctx    = tctx.Context
		step   = tctx.Step
		params = *tctx.Task.Params.Upload
	)
	inUrl, err := getFileUrl(tctx.OutputOSObj, params)
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
		if task.Status.Phase != api.TaskPhaseRunning {
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
		glog.Infof("Processing upload task catalyst callback. taskId=%s status=%q rawCallback=%+v",
			tctx.Task.ID, callback.Status, *callback)
		if callback.Status != "success" {
			return nil, fmt.Errorf("unsucessful callback received. status=%v", callback.Status)
		}

		tctx.Progress.Set(0.9)
		taskOutput, err := processCatalystCallback(tctx, callback)
		if err != nil {
			return nil, fmt.Errorf("error processing catalyst callback: %w", err)
		}
		return &data.TaskOutput{Upload: taskOutput}, nil
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

func processCatalystCallback(tctx *TaskContext, callback *clients.CatalystCallback) (*data.UploadTaskOutput, error) {
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
		return nil, fmt.Errorf("error getting asset output requests: %w", err)
	}

	var videoFilePath string
	var isMockResult bool
	for idx, output := range callback.Outputs {
		// TODO: Remove this once catalyst returns real data
		if output.Type == "google-s3" || output.Type == "google-s4" {
			isMockResult = true
			output.Type = "object_store"
		}
		outName := outputNames[idx]
		outReq := outputReqs[idx]
		if output.Type != outReq.Type {
			return nil, fmt.Errorf("output type mismatch: %s != %s", output.Type, outReq.Type)
		}
		switch outName {
		case OutputNameOSSourceMP4:
			if len(output.Videos) != 1 {
				return nil, fmt.Errorf("unexpected number of videos in source MP4 output: %d", len(output.Videos))
			}
			video := output.Videos[0]
			if video.Type != "mp4" {
				return nil, fmt.Errorf("unexpected video type in source MP4 output: %s", output.Videos[0].Type)
			}
			videoFilePath = video.Location
		case OutputNameOSPlaylistHLS:
			// TODO: We don't really know how to handle this yet. Just log
			glog.Infof("Received OS HLS playlist output! taskId=%s manifest=%q output=%+v", tctx.Task.ID, output.Manifest, output)
		case OutputNameIPFSSourceMP4:
			assetSpec.Storage.IPFS.CID = output.Manifest
		default:
			return nil, fmt.Errorf("unknown output name=%q for output=%+v", outName, output)
		}
	}
	if FlagCatalystCopiesSourceFile && videoFilePath == "" {
		return nil, fmt.Errorf("no video file path found in catalyst output")
	}

	output, err := complementCatalystPipeline(tctx, *assetSpec, callback)
	if err != nil {
		return nil, err
	}
	if videoFilePath != "" {
		output.VideoFilePath = videoFilePath
	}

	assetSpecJson, _ := json.Marshal(assetSpec)
	glog.Infof("Parsed asset spec from Catalyst: taskId=%s assetSpec=%+v, assetSpecJson=%q", tctx.Task.ID, assetSpec, assetSpecJson)
	if isMockResult {
		return nil, UnretriableError{errors.New("catalyst api only has mock results for now, check back later... :(")}
	}
	return output, nil
}

func complementCatalystPipeline(tctx *TaskContext, assetSpec api.AssetSpec, callback *clients.CatalystCallback) (*data.UploadTaskOutput, error) {
	var (
		playbackID = tctx.OutputAsset.PlaybackID
		params     = *tctx.Task.Params.Upload
		osSess     = tctx.outputOS // Upload deals with outputOS only (URL -> ObjectStorage)
	)
	filename, size, contents, err := getFile(tctx, osSess, tctx.ImportTaskConfig, params)
	if err != nil {
		return nil, fmt.Errorf("error getting source file: %w", err)
	}
	defer contents.Close()
	input := tctx.Progress.TrackReader(contents, size, 0.94)
	sizeInt := int64(size)
	rawSourceFile, err := readFile(filename, &sizeInt, input)
	if err != nil {
		return nil, fmt.Errorf("error downloading source file to disk: %w", err)
	}
	defer rawSourceFile.Close()
	readLocalFile := func(endProgress float64) (*ReadCounter, error) {
		_, err = rawSourceFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil, fmt.Errorf("error seeking to start of source file: %w", err)
		}
		return tctx.Progress.TrackReader(rawSourceFile, size, endProgress), nil
	}

	var videoFilePath string
	if !FlagCatalystCopiesSourceFile {
		input, err := readLocalFile(0.95)
		if err != nil {
			return nil, err
		}
		fullPath := videoFileName(playbackID)
		videoFilePath, err = osSess.SaveData(tctx, fullPath, input, nil, fileUploadTimeout)
		if err != nil {
			return nil, fmt.Errorf("error uploading file=%q to object store: %w", fullPath, err)
		}
		glog.Infof("Saved file=%s to url=%s", fullPath, videoFilePath)
	}

	if tctx.OutputAsset.Storage.IPFS != nil {
		ipfs := *tctx.OutputAsset.Storage.IPFS
		if !FlagCatalystSupportsIPFS {
			// TODO: Remove this branch once we have reliable catalyst IPFS support
			var (
				playbackID  = tctx.OutputAsset.PlaybackID
				contentType = "video/" + tctx.OutputAsset.VideoSpec.Format
			)
			input, err = readLocalFile(0.99)
			if err != nil {
				return nil, err
			}
			cid, _, err := tctx.ipfs.PinContent(tctx, "asset-"+playbackID, contentType, input)
			if err != nil {
				return nil, fmt.Errorf("error pinning file to IPFS: %w", err)
			}
			ipfs.CID = cid
		}
		if ipfs.CID == "" {
			return nil, fmt.Errorf("missing IPFS CID from Catalyst response")
		}
		metadataCID, err := saveNFTMetadata(tctx, tctx.ipfs, tctx.OutputAsset, ipfs.CID,
			ipfs.Spec.NFTMetadataTemplate, ipfs.Spec.NFTMetadata, tctx.ExportTaskConfig)
		if err != nil {
			return nil, fmt.Errorf("error pining NFT metadata to IPFS: %w", err)
		}
		ipfs.NFTMetadata = &api.IPFSFileInfo{CID: metadataCID}
		assetSpec.Storage.IPFS = &ipfs
	}

	metadata := &FileMetadata{}
	if !FlagCatalystProbesFile {
		input, err = readLocalFile(1)
		if err != nil {
			return nil, err
		}
		metadata, err = Probe(tctx, tctx.OutputAsset.ID, filename, input)
		if err != nil {
			return nil, err
		}
		probed := metadata.AssetSpec
		assetSpec.Hash, assetSpec.Size, assetSpec.VideoSpec = probed.Hash, probed.Size, probed.VideoSpec
	}
	metadata.AssetSpec, metadata.CatalystResult = &assetSpec, callback
	metadataFilePath, err := saveMetadataFile(tctx, tctx.outputOS, tctx.OutputAsset.PlaybackID, metadata)
	if err != nil {
		return nil, fmt.Errorf("error saving metadata file: %w", err)
	}

	return &data.UploadTaskOutput{
		VideoFilePath:    videoFilePath,
		MetadataFilePath: metadataFilePath,
		AssetSpec:        assetSpec,
	}, nil
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
		[]OutputName{OutputNameOSPlaylistHLS},
		[]clients.OutputLocation{
			{
				Type: "object_store",
				URL:  outURL.JoinPath(hlsRootPlaylistFileName(asset.PlaybackID)).String(),
				Outputs: &clients.OutputsRequest{
					SourceSegments:     true,
					TranscodedSegments: true,
				},
			},
		}
	if FlagCatalystCopiesSourceFile {
		names, locations =
			append(names, OutputNameOSSourceMP4),
			append(locations, clients.OutputLocation{
				Type: "object_store",
				URL:  outURL.JoinPath(videoFileName(asset.PlaybackID)).String(),
				Outputs: &clients.OutputsRequest{
					SourceMp4: true,
				},
			})
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
