package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	catalystClients "github.com/livepeer/catalyst-api/clients"
	"github.com/livepeer/catalyst-api/pipeline"
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
)

const (
	OUTPUT_ENABLED       = "enabled"
	OUTPUT_DISABLED      = "disabled"
	OUTPUT_ONLY_SHORT    = "only_short"
	IPFS_PREFIX          = "ipfs://"
	ARWEAVE_PREFIX       = "ar://"
	segLen               = 2 * time.Second
	maxFileSizeForMemory = 50_000_000
)

var (
	// Feature flag whether to use Catalyst's IPFS support or not.
	FlagCatalystSupportsIPFS = false
)

type UploadTaskConfig struct {
	// Ordered list of IPFS gateways (includes /ipfs/ suffix) to import assets from
	ImportIPFSGatewayURLs []*url.URL
}

type OutputName string

var (
	OutputNameEmpty         = OutputName("empty_output")
	OutputNameOSSourceMP4   = OutputName("source_mp4")
	OutputNameOSPlaylistHLS = OutputName("playlist_hls")
	OutputNameIPFSSourceMP4 = OutputName("ipfs_source_mp4")
	OutputNameClipSource    = OutputName("clip_source")
)

type handleUploadVODParams struct {
	tctx                     *TaskContext
	inUrl                    string
	getOutputLocations       func() ([]clients.OutputLocation, error)
	finalize                 func(callback *clients.CatalystCallback) (*TaskHandlerOutput, error)
	profiles                 []api.Profile
	targetSegmentSizeSecs    int64
	catalystPipelineStrategy pipeline.Strategy
	clipStrategy             clients.ClipStrategy
	c2pa                     bool
}

func handleUploadVOD(p handleUploadVODParams) (*TaskHandlerOutput, error) {
	var (
		tctx  = p.tctx
		ctx   = tctx.Context
		step  = tctx.Step
		inUrl = p.inUrl
	)
	switch step {
	case "", "rateLimitBackoff":
		outputLocations, err := p.getOutputLocations()
		if err != nil {
			return nil, err
		}

		var encryption *clients.EncryptionPayload
		var clipStrategy *clients.ClipStrategy

		if clipParams := tctx.Task.Params.Clip; clipParams != nil {
			clipStrategy = &clients.ClipStrategy{
				StartTime:  clipParams.ClipStrategy.StartTime,
				EndTime:    clipParams.ClipStrategy.EndTime,
				PlaybackID: clipParams.ClipStrategy.PlaybackId,
			}
		} else {
			uploadParams := tctx.Task.Params.Upload
			if uploadParams != nil && uploadParams.Encryption.EncryptedKey != "" {
				encryption = &clients.EncryptionPayload{
					EncryptedKey: uploadParams.Encryption.EncryptedKey,
				}
			}
		}

		req := clients.UploadVODRequest{
			ExternalID:            tctx.Task.ID,
			Url:                   inUrl,
			CallbackUrl:           tctx.catalyst.CatalystHookURL(tctx.Task.ID, "finalize", catalystTaskAttemptID(tctx.Task)),
			OutputLocations:       outputLocations,
			PipelineStrategy:      p.catalystPipelineStrategy,
			TargetSegmentSizeSecs: p.targetSegmentSizeSecs,
			Encryption:            encryption,
			C2PA:                  p.c2pa,
		}

		if p.profiles != nil {
			req.Profiles = &p.profiles
		}

		if clipStrategy != nil {
			req.ClipStrategy = *clipStrategy
		}

		var nextStep = "checkCatalyst"

		err = tctx.catalyst.UploadVOD(ctx, req)
		if errors.Is(err, clients.ErrRateLimited) {
			nextStep = "rateLimitBackoff"
		} else if clients.IsInputError(err) {
			return nil, UnretriableError{fmt.Errorf("input error on catalyst request: %w", err)}
		} else if err != nil {
			return nil, fmt.Errorf("failed to call catalyst: %w", err)
		}
		err = tctx.delayTaskStep(ctx, tctx.Task.ID, nextStep, nil)
		if err != nil {
			return nil, fmt.Errorf("failed scheduling catalyst healthcheck: %w", err)
		}
		return ContinueAsync, nil
	case "checkCatalyst":
		task := tctx.Task
		if task.Status.Phase != api.TaskPhaseRunning || task.Status.Step == "finalize" {
			// Task has already progressed to another phase or step. To stop the loop
			// of `checkCatalyst` we "continue" without having scheduled another msg.
			return ContinueAsync, nil
		}
		updatedAt := data.NewUnixMillisTime(task.Status.UpdatedAt)
		if updateAge := time.Since(updatedAt.Time); updateAge > time.Minute {
			return nil, fmt.Errorf("catalyst task lost (last update %s ago)", updateAge)
		}
		err := tctx.delayTaskStep(ctx, task.ID, "checkCatalyst", nil)
		if err != nil {
			return nil, fmt.Errorf("failed to schedule next check: %w", err)
		}
		return ContinueAsync, nil
	case "finalize":
		var callback *clients.CatalystCallback
		if err := json.Unmarshal(tctx.StepInput, &callback); err != nil {
			return nil, fmt.Errorf("error parsing step input: %w", err)
		}
		glog.Infof("Processing upload vod catalyst callback. taskId=%s status=%q",
			tctx.Task.ID, callback.Status)
		if callback.Status != catalystClients.TranscodeStatusCompleted {
			return nil, fmt.Errorf("unsucessful callback received. status=%v", callback.Status)
		}

		return p.finalize(callback)
	case "resultPartial":
		var sourcePlayback *video.OutputVideo
		if err := json.Unmarshal(tctx.StepInput, &sourcePlayback); err != nil {
			return nil, fmt.Errorf("error parsing step input: %w", err)
		}
		if (tctx.Task != nil && tctx.Task.Type == "transcode-file") || tctx.OutputAsset == nil {
			glog.Infof("not sending partial result. taskId=%s type=%s outputAssetNil=%t", tctx.Task.ID, tctx.Task.Type, tctx.OutputAsset == nil)
			return ContinueAsync, nil
		}
		manifestPath, err := extractOSUriFilePath(sourcePlayback.Manifest, tctx.OutputAsset.PlaybackID)
		if err != nil {
			return nil, fmt.Errorf("error extracting file path from output manifest: %w", err)
		}

		return &TaskHandlerOutput{
			TaskOutput: &data.TaskOutput{
				Upload: &data.UploadTaskOutput{
					AssetSpec: api.AssetSpec{Files: []api.AssetFile{
						{
							Type: "catalyst_hls_manifest",
							Path: manifestPath,
						},
					}},
				},
			},
			Continue: true,
		}, nil
	}
	return nil, fmt.Errorf("unknown task step: %s", step)
}

func TaskUpload(tctx *TaskContext) (*TaskHandlerOutput, error) {
	params := *tctx.Task.Params.Upload
	inUrl, err := getFileUrlForUploadTask(tctx.OutputOSObj, params)
	if err != nil {
		return nil, fmt.Errorf("error building file URL: %w", err)
	}

	strategy := pipeline.Strategy(params.CatalystPipelineStrategy)
	if isRecording(tctx) || strings.HasSuffix(inUrl, ".m3u8") { // disable external transcode fallback for recordings
		strategy = pipeline.StrategyCatalystFfmpegDominance
	}

	return handleUploadVOD(handleUploadVODParams{
		tctx:  tctx,
		inUrl: inUrl,
		getOutputLocations: func() ([]clients.OutputLocation, error) {
			_, outputLocations, err := uploadTaskOutputLocations(tctx)
			return outputLocations, err
		},
		finalize: func(callback *clients.CatalystCallback) (*TaskHandlerOutput, error) {
			tctx.Progress.Set(0.9)
			taskOutput, err := processCatalystCallback(tctx, callback)
			if err != nil {
				return nil, fmt.Errorf("error processing catalyst callback: %w", err)
			}
			return &TaskHandlerOutput{
				TaskOutput: &data.TaskOutput{Upload: taskOutput},
			}, nil
		},
		catalystPipelineStrategy: strategy,
		c2pa:                     params.C2PA,
		profiles:                 params.Profiles,
		targetSegmentSizeSecs:    params.TargetSegmentSizeSecs,
	})
}

func isRecording(tctx *TaskContext) bool {
	if tctx == nil || tctx.OutputAsset == nil {
		return false
	}
	return tctx.OutputAsset.Source.Type == "recording"
}

func TaskTranscodeFile(tctx *TaskContext) (*TaskHandlerOutput, error) {
	params := *tctx.Task.Params.TranscodeFile

	return handleUploadVOD(handleUploadVODParams{
		tctx:  tctx,
		inUrl: params.Input.URL,
		getOutputLocations: func() ([]clients.OutputLocation, error) {
			_, outputLocation, err := outputLocations(
				params.Storage.URL,
				outputs{
					hls:  out(isEnabled(params.Outputs.HLS.Path), params.Outputs.HLS.Path),
					mp4:  out(isEnabled(params.Outputs.MP4.Path), params.Outputs.MP4.Path),
					fmp4: out(isEnabled(params.Outputs.FMP4.Path), params.Outputs.FMP4.Path),
				},
				false,
			)
			return outputLocation, err
		},
		finalize: func(callback *clients.CatalystCallback) (*TaskHandlerOutput, error) {
			tctx.Progress.Set(1)
			tfo, err := toTranscodeFileTaskOutput(callback)
			if err != nil {
				return nil, err
			}
			return &TaskHandlerOutput{TaskOutput: &data.TaskOutput{TranscodeFile: &tfo}}, nil
		},
		catalystPipelineStrategy: pipeline.Strategy(params.CatalystPipelineStrategy),
		targetSegmentSizeSecs:    params.TargetSegmentSizeSecs,
		profiles:                 params.Profiles,
		c2pa:                     params.C2PA,
	})
}

func TaskClip(tctx *TaskContext) (*TaskHandlerOutput, error) {
	params := *tctx.Task.Params.Clip
	return handleUploadVOD(handleUploadVODParams{
		tctx:  tctx,
		inUrl: params.URL,
		getOutputLocations: func() ([]clients.OutputLocation, error) {
			_, outputLocations, err := clipTaskOutputLocations(tctx)
			return outputLocations, err
		},
		finalize: func(callback *clients.CatalystCallback) (*TaskHandlerOutput, error) {
			tctx.Progress.Set(0.9)
			taskOutput, err := processCatalystCallback(tctx, callback)
			if err != nil {
				return nil, fmt.Errorf("error processing catalyst callback: %w", err)
			}
			return &TaskHandlerOutput{
				TaskOutput: &data.TaskOutput{Clip: taskOutput},
			}, nil
		},
		catalystPipelineStrategy: pipeline.StrategyCatalystFfmpegDominance,
		clipStrategy: clients.ClipStrategy{
			StartTime:  params.ClipStrategy.StartTime,
			EndTime:    params.ClipStrategy.EndTime,
			PlaybackID: params.ClipStrategy.PlaybackId,
		},
	})
}

func isEnabled(output string) string {
	if output != "" {
		return OUTPUT_ENABLED
	}
	return OUTPUT_DISABLED
}

func toTranscodeFileTaskOutput(callback *clients.CatalystCallback) (data.TranscodeFileTaskOutput, error) {
	var res data.TranscodeFileTaskOutput

	res.RequestID = callback.RequestID
	res.InputVideo = &data.InputVideo{
		Duration:  callback.InputVideo.Duration,
		SizeBytes: callback.InputVideo.SizeBytes,
	}

	if len(callback.Outputs) < 1 {
		return res, fmt.Errorf("invalid video outputs: %v", callback.Outputs)
	}
	// we expect only one output
	o := callback.Outputs[0]

	bu, p, err := parseUrlToBaseAndPath(o.Manifest)
	if err != nil {
		return res, err
	}
	res.BaseUrl = bu
	if len(o.Videos) > 0 {
		res.Hls = &data.TranscodeFileTaskOutputPath{Path: p}
	}

	for _, m := range o.MP4Outputs {
		_, p, err := parseUrlToBaseAndPath(m.Location)
		if err != nil {
			return res, err
		}
		res.Mp4 = append(res.Mp4, data.TranscodeFileTaskOutputPath{Path: p})
	}

	return res, nil
}

func parseUrlToBaseAndPath(URL string) (string, string, error) {
	u, err := url.Parse(URL)
	if err != nil {
		return "", "", err
	}

	p := u.Path
	if strings.HasPrefix(u.Scheme, "s3+http") {
		// first part of the Object Store path is a bucket name, skip it
		ps := strings.Split(strings.TrimLeft(p, "/"), "/")
		p = "/" + strings.Join(ps[1:], "/")
	}

	var baseUrl string
	if u.Scheme == "ipfs" {
		// add baseUrl only for IPFS
		u.Path = ""
		baseUrl = u.String()
	}

	return baseUrl, p, nil
}

func getFileUrlForUploadTask(os *api.ObjectStore, params api.UploadTaskParams) (string, error) {
	if key := params.UploadedObjectKey; key != "" {
		u, err := url.Parse(os.PublicURL)
		if err != nil {
			return "", err
		}
		return u.JoinPath(key).String(), nil
	}
	return params.URL, nil
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

			Width:       int(track.Width),
			Height:      int(track.Height),
			PixelFormat: track.PixelFormat,
			FPS:         float64(track.FPS) / 1000,

			Channels:   track.Channels,
			SampleRate: track.SampleRate,
		}
	}

	var outputNames []OutputName
	var outputReqs []clients.OutputLocation
	var err error

	if tctx.Task.Params.Upload != nil {
		outputNames, outputReqs, err = uploadTaskOutputLocations(tctx)
	} else {
		glog.Infof("Processing clip task outputs")
		outputNames, outputReqs, err = clipTaskOutputLocations(tctx)
	}

	if err != nil {
		return nil, fmt.Errorf("error getting asset output requests: %w", err)
	}

	var (
		playbackID    = tctx.OutputAsset.PlaybackID
		videoFilePath string
	)
	for idx, output := range callback.Outputs {
		if idx >= len(outputNames) {
			extraOuts := callback.Outputs[idx:]
			extraOutsStr, _ := json.Marshal(extraOuts)
			glog.Warningf("Catalyst returned more outputs than requested, ignoring unexpected outputs. extraOutputs=%q", extraOutsStr)
			break
		}
		outName := outputNames[idx]
		outReq := outputReqs[idx]
		if output.Type != outReq.Type {
			return nil, fmt.Errorf("output type mismatch: %s != %s", output.Type, outReq.Type)
		}
		manifestPath, err := extractOSUriFilePath(output.Manifest, playbackID)
		if err != nil {
			return nil, fmt.Errorf("error extracting file path from output manifest: %w", err)
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
			assetSpec.Files = append(assetSpec.Files, api.AssetFile{
				Type: "source_file",
				Path: manifestPath,
			})
		case OutputNameOSPlaylistHLS:
			glog.Infof("Received OS HLS playlist output! taskId=%s manifest=%q output=%+v", tctx.Task.ID, output.Manifest, output)
			assetSpec.Files = append(assetSpec.Files, api.AssetFile{
				Type: "catalyst_hls_manifest",
				Path: manifestPath,
			})
			for v, video := range output.MP4Outputs {
				if video.Type != "mp4" {
					return nil, fmt.Errorf("unexpected video type in rendition MP4 output: %s", output.Videos[v].Type)
				}
				videoFilePath = video.Location
				videoFilePath, err = extractOSUriFilePath(videoFilePath, playbackID)
				if err != nil {
					return nil, fmt.Errorf("error extracting file path from mp4 rendition video location: %w", err)
				}
				glog.Infof("Adding mp4 asset file %+v= path=%q", video, videoFilePath)
				assetSpec.Files = append(assetSpec.Files, api.AssetFile{
					Type: "static_transcoded_mp4",
					Path: videoFilePath,
					Spec: api.AssetFileSpec{
						Size:    video.SizeBytes,
						Width:   video.Width,
						Height:  video.Height,
						Bitrate: video.Bitrate,
					},
				})
			}
		case OutputNameIPFSSourceMP4:
			if assetSpec.Storage == nil {
				assetSpec.Storage = &api.AssetStorage{IPFS: &api.AssetIPFS{}}
			} else if assetSpec.Storage.IPFS == nil {
				assetSpec.Storage.IPFS = &api.AssetIPFS{}
			}
			assetSpec.Storage.IPFS.CID = output.Manifest
		default:
			return nil, fmt.Errorf("unknown output name=%q for output=%+v", outName, output)
		}
	}
	assetSpecJson, _ := json.Marshal(assetSpec)
	glog.Infof("Parsed asset spec from Catalyst: taskId=%s assetSpec=%+v, assetSpecJson=%q", tctx.Task.ID, assetSpec, assetSpecJson)

	fullPath := videoFileName(playbackID)
	assetSpec.Files = append(assetSpec.Files, api.AssetFile{
		Type: "source_file",
		Path: toAssetRelativePath(playbackID, fullPath),
	})

	// add thumbs vtt output file
	thumbsLocation := "thumbnails/thumbnails.vtt"
	_, err = tctx.outputOS.ReadData(tctx, path.Join(playbackID, thumbsLocation))
	if err != nil {
		glog.Warningf("Thumbnails VTT not found: taskId=%s err=%q", tctx.Task.ID, err)
	} else {
		assetSpec.Files = append(assetSpec.Files, api.AssetFile{
			Type: "thumbnails_vtt",
			Path: thumbsLocation,
		})
	}

	var output *data.UploadTaskOutput

	if tctx.Task.Params.Upload != nil {
		output, err = complementCatalystPipeline(tctx, *assetSpec)
	} else {
		output, err = complementClipCatalystPipeline(tctx, *assetSpec)
	}

	if err != nil {
		return nil, err
	}

	assetSpecJson, _ = json.Marshal(output.AssetSpec)
	glog.Infof("Complemented spec from Catalyst: taskId=%s assetSpec=%+v, assetSpecJson=%q", tctx.Task.ID, output.AssetSpec, assetSpecJson)
	return output, nil
}

func complementCatalystPipeline(tctx *TaskContext, assetSpec api.AssetSpec) (*data.UploadTaskOutput, error) {
	var (
		playbackID           = tctx.OutputAsset.PlaybackID
		params               = *tctx.Task.Params.Upload
		osSess               = tctx.outputOS // Upload deals with outputOS only (URL -> ObjectStorage)
		inFile               = params.URL
		vodDecryptPrivateKey = tctx.VodDecryptPrivateKey
		contents             io.ReadCloser
		size                 uint64
		filename             string
		catalystCopiedSource = false
	)
	if isHLSFile(inFile) {
		return &data.UploadTaskOutput{AssetSpec: assetSpec}, nil
	}

	catalystSource, err := osSess.ReadData(tctx, videoFileName(playbackID))
	if err == nil {
		glog.Infof("Found source copy from catalyst taskId=%s filename=%s", tctx.Task.ID, catalystSource.Name)
		contents = catalystSource.Body
		if catalystSource.Size != nil && *catalystSource.Size > 0 {
			size = uint64(*catalystSource.Size)
		}
		filename = catalystSource.FileInfo.Name
		catalystCopiedSource = true
	} else {
		glog.Infof("Source copy from catalyst not found taskId=%s err=%v", tctx.Task.ID, err)
		filename, size, contents, err = getFile(tctx, osSess, tctx.UploadTaskConfig, params, vodDecryptPrivateKey)
		if err != nil {
			return nil, fmt.Errorf("error getting source file: %w", err)
		}
	}
	defer contents.Close()

	storage := tctx.OutputAsset.Storage
	ipfsRequired := storage != nil && storage.IPFS != nil && storage.IPFS.Spec != nil
	if !ipfsRequired && catalystCopiedSource {
		glog.Infof("Skipping file download")
		return &data.UploadTaskOutput{AssetSpec: assetSpec}, nil
	}

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

	if !catalystCopiedSource {
		// in case of encrypted input, file will have been copied in the beginning
		if !isEncryptionEnabled(params) {
			input, err := readLocalFile(0.95)
			if err != nil {
				return nil, err
			}
			fullPath := videoFileName(playbackID)
			fileUrl, err := osSess.SaveData(tctx, fullPath, input, nil, fileUploadTimeout)
			if err != nil {
				return nil, fmt.Errorf("error uploading file=%q to object store: %w", fullPath, err)
			}
			glog.Infof("Saved file=%s to url=%s", fullPath, fileUrl)
		}
	}

	if ipfsRequired {
		ipfs := *storage.IPFS
		if !FlagCatalystSupportsIPFS {
			// TODO: Remove this branch once we have reliable catalyst IPFS support
			var (
				playbackID  = tctx.OutputAsset.PlaybackID
				contentType = "video/" + assetSpec.VideoSpec.Format
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

	return &data.UploadTaskOutput{AssetSpec: assetSpec}, nil
}

func complementClipCatalystPipeline(tctx *TaskContext, assetSpec api.AssetSpec) (*data.UploadTaskOutput, error) {
	var (
		playbackID           = tctx.OutputAsset.PlaybackID
		params               = *tctx.Task.Params.Clip
		osSess               = tctx.outputOS // Upload deals with outputOS only (URL -> ObjectStorage)
		inFile               = params.URL
		contents             io.ReadCloser
		size                 uint64
		filename             string
		catalystCopiedSource = false
	)
	if isHLSFile(inFile) {
		return &data.UploadTaskOutput{AssetSpec: assetSpec}, nil
	}

	catalystSource, err := osSess.ReadData(tctx, videoFileName(playbackID))
	if err == nil {
		glog.Infof("Found source copy from catalyst taskId=%s filename=%s", tctx.Task.ID, catalystSource.Name)
		contents = catalystSource.Body
		if catalystSource.Size != nil && *catalystSource.Size > 0 {
			size = uint64(*catalystSource.Size)
		}
		filename = catalystSource.FileInfo.Name
		catalystCopiedSource = true
	} else {
		return nil, fmt.Errorf("source copy from catalyst not found taskId=%s err=%v", tctx.Task.ID, err)
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

	if !catalystCopiedSource {
		input, err := readLocalFile(0.95)
		if err != nil {
			return nil, err
		}
		fullPath := videoFileName(playbackID)
		fileUrl, err := osSess.SaveData(tctx, fullPath, input, nil, fileUploadTimeout)
		if err != nil {
			return nil, fmt.Errorf("error uploading file=%q to object store: %w", fullPath, err)
		}
		glog.Infof("Saved file=%s to url=%s", fullPath, fileUrl)
	}

	return &data.UploadTaskOutput{AssetSpec: assetSpec}, nil
}

func isHLSFile(fname string) bool {
	if filepath.Ext(fname) == ".m3u8" {
		return true
	}
	u, err := url.Parse(fname)
	if err != nil {
		return false
	}
	return filepath.Ext(u.Path) == ".m3u8"
}

func isEncryptionEnabled(params api.UploadTaskParams) bool {
	return params.Encryption.EncryptedKey != ""
}

func uploadTaskOutputLocations(tctx *TaskContext) ([]OutputName, []clients.OutputLocation, error) {
	playbackId := tctx.OutputAsset.PlaybackID
	outURL := tctx.OutputOSObj.URL
	var mp4, thumbsEnabled string
	if isRecording(tctx) {
		mp4 = OUTPUT_ENABLED
	} else {
		mp4 = OUTPUT_ONLY_SHORT
	}
	if tctx.Task.Params.Upload.Thumbnails {
		thumbsEnabled = OUTPUT_ENABLED
	}

	outputNames, outputLocations, err := outputLocations(
		outURL,
		outputs{
			hls:        out(OUTPUT_ENABLED, playbackId),
			mp4:        out(mp4, playbackId),
			thumbnails: out(thumbsEnabled, playbackId),
		},
		!isEncryptionEnabled(*tctx.Task.Params.Upload),
	)
	if err != nil {
		return nil, nil, err
	}
	// Add Pinata output location
	if FlagCatalystSupportsIPFS && tctx.OutputAsset.Storage != nil && tctx.OutputAsset.Storage.IPFS != nil {
		// TODO: This interface is likely going to change so that pinata is just a
		// `object_store` output
		outputNames, outputLocations =
			append(outputNames, OutputNameIPFSSourceMP4),
			append(outputLocations, clients.OutputLocation{
				Type:            "ipfs_pinata",
				PinataAccessKey: tctx.PinataAccessToken,
				Outputs: &clients.OutputsRequest{
					SourceMp4: true,
				},
			})
	}

	return outputNames, outputLocations, nil
}

func clipTaskOutputLocations(tctx *TaskContext) ([]OutputName, []clients.OutputLocation, error) {
	playbackId := tctx.OutputAsset.PlaybackID
	outURL := tctx.OutputOSObj.URL
	sourceObjectStoreID := tctx.Task.Params.Clip.SourceObjectStoreID

	sourceOSObj, err := tctx.lapi.GetObjectStore(sourceObjectStoreID)

	if err != nil {
		return nil, nil, fmt.Errorf("error fetching object store %w", err)
	}

	inUrl := sourceOSObj.URL
	sourceUrl, err := url.Parse(inUrl)
	clipSourceUrl := sourceUrl.JoinPath(tctx.Task.Params.Clip.ClipStrategy.PlaybackId, tctx.Task.Params.Clip.InputSessionID)

	if err != nil {
		return nil, nil, fmt.Errorf("error parsing object store URL %w", err)
	}

	outputNames, outputLocations, err := outputLocations(
		outURL,
		outputs{
			hls:        out(OUTPUT_ENABLED, playbackId),
			mp4:        out(OUTPUT_ENABLED, playbackId),
			thumbnails: out(OUTPUT_ENABLED, playbackId),
		},
		false,
	)

	if err != nil {
		return nil, nil, err
	}
	clipOutputLocationUrl := clipSourceUrl.JoinPath("/clip_" + playbackId).String()

	outputNames, outputLocations =
		append(outputNames, OutputNameClipSource),
		append(outputLocations, clients.OutputLocation{
			Type: "object_store",
			URL:  clipOutputLocationUrl,
			Outputs: &clients.OutputsRequest{
				Clip: OUTPUT_ENABLED,
			},
		})

	return outputNames, outputLocations, nil
}

// out is a helper to create an output, to avoid repeated struct attribute names in the code
func out(enabled, path string) output {
	return output{
		enabled: enabled,
		path:    path,
	}
}

// output represents each requested media output for outputLocations()
type output struct {
	enabled string
	path    string
}

type outputs struct {
	hls        output
	mp4        output
	fmp4       output
	thumbnails output
}

func outputLocations(
	outURL string,
	outs outputs,
	sourceCopy bool,
) ([]OutputName, []clients.OutputLocation, error) {
	url, err := url.Parse(outURL)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing object store URL: %w", err)
	}
	names, locations :=
		[]OutputName{OutputNameOSPlaylistHLS, OutputNameEmpty},
		[]clients.OutputLocation{
			outputLocation(url, outs.hls.path, &clients.OutputsRequest{HLS: outs.hls.enabled}),
			outputLocation(url, outs.mp4.path, &clients.OutputsRequest{MP4: outs.mp4.enabled}),
		}
	if outs.fmp4.enabled == OUTPUT_ENABLED {
		names, locations =
			append(names, OutputNameEmpty),
			append(locations, outputLocation(url, outs.fmp4.path, &clients.OutputsRequest{FMP4: outs.fmp4.enabled}))
	}
	if sourceCopy {
		names, locations =
			append(names, OutputNameOSSourceMP4),
			append(locations, outputLocation(url, videoFileName(outs.hls.path), &clients.OutputsRequest{SourceMp4: true}))
	}
	if outs.thumbnails.enabled == OUTPUT_ENABLED {
		names, locations =
			append(names, OutputNameEmpty),
			append(locations, outputLocation(url, outs.thumbnails.path, &clients.OutputsRequest{Thumbnails: outs.thumbnails.enabled}))
	}
	return names, locations, nil
}

func outputLocation(url *url.URL, path string, request *clients.OutputsRequest) clients.OutputLocation {
	return clients.OutputLocation{
		Type:    "object_store",
		URL:     url.JoinPath(path).String(),
		Outputs: request,
	}
}

func catalystTaskAttemptID(task *api.Task) string {
	// Simplest way to identify unique runs of a given task. We should think of
	// something more sophisticated in the future.
	return strconv.Itoa(task.Status.Retries)
}
