package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
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
	OUTPUT_ENABLED    = "enabled"
	OUTPUT_DISABLED   = "disabled"
	OUTPUT_ONLY_SHORT = "only_short"
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
	OutputNameEmpty         = OutputName("empty_output")
	OutputNameOSSourceMP4   = OutputName("source_mp4")
	OutputNameOSPlaylistHLS = OutputName("playlist_hls")
	OutputNameIPFSSourceMP4 = OutputName("ipfs_source_mp4")
)

type handleUploadVODParams struct {
	tctx                     *TaskContext
	inUrl                    string
	getOutputLocations       func() ([]clients.OutputLocation, error)
	finalize                 func(callback *clients.CatalystCallback) (*TaskHandlerOutput, error)
	profiles                 []api.Profile
	targetSegmentSizeSecs    int64
	catalystPipelineStrategy pipeline.Strategy
}

func handleUploadVOD(p handleUploadVODParams) (*TaskHandlerOutput, error) {
	var (
		tctx  = p.tctx
		ctx   = tctx.Context
		step  = tctx.Step
		inUrl = p.inUrl
	)
	switch step {
	case "":
		// TODO: Move this input decryption logic to catalyst
		if uploadParams := tctx.Params.Upload; uploadParams != nil {
			var err error
			inUrl, err = decryptInputFile(tctx, inUrl, *uploadParams)
			if err != nil {
				return nil, fmt.Errorf("error decrypting input file: %w", err)
			}
		}
		fallthrough
	case "rateLimitBackoff":
		outputLocations, err := p.getOutputLocations()
		if err != nil {
			return nil, err
		}
		var (
			req = clients.UploadVODRequest{
				ExternalID:            tctx.Task.ID,
				Url:                   inUrl,
				CallbackUrl:           tctx.catalyst.CatalystHookURL(tctx.Task.ID, "finalize", catalystTaskAttemptID(tctx.Task)),
				OutputLocations:       outputLocations,
				PipelineStrategy:      p.catalystPipelineStrategy,
				Profiles:              p.profiles,
				TargetSegmentSizeSecs: p.targetSegmentSizeSecs,
			}
			nextStep = "checkCatalyst"
		)
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
	}
	return nil, fmt.Errorf("unknown task step: %s", step)
}

func TaskUpload(tctx *TaskContext) (*TaskHandlerOutput, error) {
	params := *tctx.Task.Params.Upload
	inUrl, err := getFileUrlForUploadTask(tctx.OutputOSObj, params)
	if err != nil {
		return nil, fmt.Errorf("error building file URL: %w", err)
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
		catalystPipelineStrategy: pipeline.Strategy(params.CatalystPipelineStrategy),
	})
}

func TaskTranscodeFile(tctx *TaskContext) (*TaskHandlerOutput, error) {
	params := *tctx.Task.Params.TranscodeFile

	return handleUploadVOD(handleUploadVODParams{
		tctx:  tctx,
		inUrl: params.Input.URL,
		getOutputLocations: func() ([]clients.OutputLocation, error) {
			_, outputLocation, err := outputLocations(params.Storage.URL, isEnabled(params.Outputs.HLS.Path),
				params.Outputs.HLS.Path, isEnabled(params.Outputs.MP4.Path), params.Outputs.MP4.Path)
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
		profiles:                 params.Profiles,
		catalystPipelineStrategy: pipeline.Strategy(params.CatalystPipelineStrategy),
		targetSegmentSizeSecs:    params.TargetSegmentSizeSecs,
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

func decryptInputFile(tctx *TaskContext, fileUrl string, params api.UploadTaskParams) (string, error) {
	var (
		osSess     = tctx.outputOS
		os         = tctx.OutputOSObj
		cfg        = tctx.ImportTaskConfig
		playbackID = tctx.OutputAsset.PlaybackID
	)
	if params.Encryption.Key == "" {
		return fileUrl, nil
	}

	glog.Infof("Downloading file=%s from object store", params.URL)
	_, _, content, err := getFile(tctx, osSess, cfg, params)
	if err != nil {
		return "", fmt.Errorf("failed to get input file: %w", err)
	}
	defer content.Close()

	glog.Infof("Uploading decrypted file=%s to object store", params.URL)
	fullPath := videoFileName(playbackID)
	fileUrl, err = osSess.SaveData(tctx, fullPath, content, nil, fileUploadTimeout)
	if err != nil {
		return "", fmt.Errorf("error uploading file=%q to object store: %w", fullPath, err)
	}
	glog.Infof("Saved file=%s to url=%s", fullPath, fileUrl)

	osPublicURL, err := url.Parse(os.PublicURL)
	if err != nil {
		return "", fmt.Errorf("error parsing object store public URL: %w", err)
	}
	return osPublicURL.JoinPath(fullPath).String(), nil
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

			Width:       int(track.VideoTrack.Width),
			Height:      int(track.VideoTrack.Height),
			PixelFormat: track.VideoTrack.PixelFormat,
			FPS:         float64(track.VideoTrack.FPS) / 1000,

			Channels:   track.AudioTrack.Channels,
			SampleRate: track.AudioTrack.SampleRate,
		}
	}

	outputNames, outputReqs, err := uploadTaskOutputLocations(tctx)
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
			assetSpec.Storage.IPFS.CID = output.Manifest
		default:
			return nil, fmt.Errorf("unknown output name=%q for output=%+v", outName, output)
		}
	}
	if FlagCatalystCopiesSourceFile && videoFilePath == "" {
		return nil, fmt.Errorf("no video file path found in catalyst output")
	}
	assetSpecJson, _ := json.Marshal(assetSpec)
	glog.Infof("Parsed asset spec from Catalyst: taskId=%s assetSpec=%+v, assetSpecJson=%q", tctx.Task.ID, assetSpec, assetSpecJson)

	output, err := complementCatalystPipeline(tctx, *assetSpec, callback)
	if err != nil {
		return nil, err
	}

	assetSpecJson, _ = json.Marshal(output.AssetSpec)
	glog.Infof("Complemented spec from Catalyst: taskId=%s assetSpec=%+v, assetSpecJson=%q", tctx.Task.ID, output.AssetSpec, assetSpecJson)
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

	if !FlagCatalystCopiesSourceFile {
		fullPath := videoFileName(playbackID)
		assetSpec.Files = append(assetSpec.Files, api.AssetFile{
			Type: "source_file",
			Path: toAssetRelativePath(playbackID, fullPath),
		})

		// in case of encrypted input, file will have been copied in the beginning
		if params.Encryption.Key == "" {
			input, err := readLocalFile(0.95)
			if err != nil {
				return nil, err
			}
			fileUrl, err := osSess.SaveData(tctx, fullPath, input, nil, fileUploadTimeout)
			if err != nil {
				return nil, fmt.Errorf("error uploading file=%q to object store: %w", fullPath, err)
			}
			glog.Infof("Saved file=%s to url=%s", fullPath, fileUrl)
		}
	}

	metadata := &FileMetadata{}
	if !FlagCatalystProbesFile {
		input, err = readLocalFile(1)
		if err != nil {
			return nil, err
		}
		metadata, err = Probe(tctx, tctx.OutputAsset.ID, filename, input, false)
		if err != nil {
			return nil, err
		}
		probed := metadata.AssetSpec
		assetSpec.Hash, assetSpec.Size, assetSpec.VideoSpec = probed.Hash, probed.Size, probed.VideoSpec
	}

	metadata.AssetSpec, metadata.CatalystResult = &assetSpec, removeCredentials(callback)

	if ipfsSpec := tctx.OutputAsset.Storage.IPFS; ipfsSpec != nil && ipfsSpec.Spec != nil {
		ipfs := *ipfsSpec
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

	_, metadataPath, err := saveMetadataFile(tctx, tctx.outputOS, tctx.OutputAsset.PlaybackID, metadata)
	if err != nil {
		return nil, fmt.Errorf("error saving metadata file: %w", err)
	}
	assetSpec.Files = append(assetSpec.Files, api.AssetFile{
		Type: "metadata",
		Path: toAssetRelativePath(playbackID, metadataPath),
	})

	return &data.UploadTaskOutput{AssetSpec: assetSpec}, nil
}

func removeCredentials(metadata *clients.CatalystCallback) *clients.CatalystCallback {
	res := *metadata
	res.Outputs = make([]video.OutputVideo, len(metadata.Outputs))

	for o, output := range metadata.Outputs {
		res.Outputs[o] = output
		res.Outputs[o].Manifest = clients.RedactURL(output.Manifest)
		res.Outputs[o].Videos = make([]video.OutputVideoFile, len(output.Videos))
		for v, video := range output.Videos {
			res.Outputs[o].Videos[v] = video
			res.Outputs[o].Videos[v].Location = clients.RedactURL(video.Location)
		}
	}

	return &res
}

func uploadTaskOutputLocations(tctx *TaskContext) ([]OutputName, []clients.OutputLocation, error) {
	playbackId := tctx.OutputAsset.PlaybackID
	outURL := tctx.OutputOSObj.URL
	outputNames, outputLocations, err := outputLocations(outURL, OUTPUT_ENABLED, playbackId, OUTPUT_ONLY_SHORT, playbackId)
	if err != nil {
		return nil, nil, err
	}
	// Add Pinata output location
	if FlagCatalystSupportsIPFS && tctx.OutputAsset.Storage.IPFS != nil {
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

func outputLocations(outURL, hls, hlsRelPath, mp4, mp4RelPath string) ([]OutputName, []clients.OutputLocation, error) {
	url, err := url.Parse(outURL)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing object store URL: %w", err)
	}
	names, locations :=
		[]OutputName{OutputNameOSPlaylistHLS, OutputNameEmpty},
		[]clients.OutputLocation{
			{
				Type: "object_store",
				URL:  url.JoinPath(hlsRelPath).String(),
				Outputs: &clients.OutputsRequest{
					HLS: hls,
				},
			},
			{
				Type: "object_store",
				URL:  url.JoinPath(mp4RelPath).String(),
				Outputs: &clients.OutputsRequest{
					MP4: mp4,
				},
			},
		}
	if FlagCatalystCopiesSourceFile {
		names, locations =
			append(names, OutputNameOSSourceMP4),
			append(locations, clients.OutputLocation{
				Type: "object_store",
				URL:  url.JoinPath(videoFileName(hlsRelPath)).String(),
				Outputs: &clients.OutputsRequest{
					SourceMp4: true,
				},
			})
	}
	return names, locations, nil
}

func catalystTaskAttemptID(task *api.Task) string {
	// Simplest way to identify unique runs of a given task. We should think of
	// something more sophisticated in the future.
	return strconv.Itoa(task.Status.Retries)
}
