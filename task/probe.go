package task

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"path"
	"strconv"
	"strings"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
	"github.com/livepeer/stream-tester/model"
	"github.com/livepeer/task-runner/clients"
	ffprobe "gopkg.in/vansante/go-ffprobe.v2"
)

var (
	supportedFormats      = []string{"mp4", "mov"}
	supportedVideoCodecs  = map[string]bool{"h264": true}
	supportedPixelFormats = map[string]bool{"yuv420p": true, "yuvj420p": true}
	supportedAudioCodecs  = map[string]bool{"aac": true}
)

type FileMetadata struct {
	AssetSpec      *api.AssetSpec            `json:"assetSpec"`
	MD5            string                    `json:"md5,omitempty"`
	SHA256         string                    `json:"sha256,omitempty"`
	Ffprobe        *ffprobe.ProbeData        `json:"ffprobe,omitempty"`
	CatalystResult *clients.CatalystCallback `json:"catalystResult,omitempty"`
}

func Probe(ctx context.Context, assetId, filename string, data *ReadCounter, strict bool) (*FileMetadata, error) {
	hasher := NewReadHasher(data)
	probeData, err := ffprobe.ProbeReader(ctx, hasher)
	if err != nil {
		return nil, fmt.Errorf("error probing file: %w", err)
	}
	logProbeData(assetId, filename, probeData)
	if _, err := hasher.FinishReader(); err != nil {
		return nil, fmt.Errorf("error reading input: %w", err)
	}
	size, md5, sha256 := data.Count(), hasher.MD5(), hasher.SHA256()
	assetSpec, err := toAssetSpec(filename, strict, probeData, size, []api.AssetHash{
		{Hash: md5, Algorithm: "md5"},
		{Hash: sha256, Algorithm: "sha256"}})
	if err != nil {
		return nil, fmt.Errorf("error processing ffprobe output: %w", err)
	}
	return &FileMetadata{
		MD5:       md5,
		SHA256:    sha256,
		Ffprobe:   probeData,
		AssetSpec: assetSpec,
	}, nil
}

func toAssetSpec(filename string, strict bool, probeData *ffprobe.ProbeData, size uint64, hash []api.AssetHash) (*api.AssetSpec, error) {
	if filename == "" && probeData.Format.Filename != "pipe:" {
		filename = probeData.Format.Filename
	}
	format, err := findFormat(supportedFormats, probeData.Format.FormatName, filename, strict)
	if err != nil {
		return nil, err
	}
	bitrate, err := strconv.ParseFloat(probeData.Format.BitRate, 64)
	if probeData.Format.BitRate != "" && err != nil {
		return nil, fmt.Errorf("error parsing file bitrate: %w", err)
	}
	spec := &api.AssetSpec{
		Name: filename,
		Type: "video",
		Hash: hash,
		Size: size,
		VideoSpec: &api.AssetVideoSpec{
			Format:      format,
			DurationSec: probeData.Format.DurationSeconds,
			Bitrate:     bitrate,
			Tracks:      make([]*api.AssetTrack, 0, len(probeData.Streams)),
		},
	}
	var hasVideo, hasAudio bool
	for _, stream := range probeData.Streams {
		track, err := toAssetTrack(stream, strict)
		if err != nil {
			return nil, err
		} else if track == nil {
			continue
		}
		if track.Type == "video" {
			if hasVideo && strict {
				return nil, fmt.Errorf("multiple video tracks in file")
			}
			hasVideo = true
		}
		hasAudio = hasAudio || track.Type == "audio"
		spec.VideoSpec.Tracks = append(spec.VideoSpec.Tracks, track)
	}
	if !hasVideo {
		return nil, fmt.Errorf("no video track found in file")
	}
	if !hasAudio && strict {
		return nil, fmt.Errorf("no audio track found in file")
	}
	return spec, nil
}

func findFormat(supportedFormats []string, format, filename string, strict bool) (string, error) {
	actualFormats := strings.Split(format, ",")
	extension := path.Ext(filename)
	if containsStr(supportedFormats, extension) && containsStr(actualFormats, extension) {
		return extension, nil
	}
	for _, f := range supportedFormats {
		if containsStr(actualFormats, f) {
			return f, nil
		}
	}
	if !strict {
		return actualFormats[0], nil
	}
	return "", fmt.Errorf("unsupported format: %s", format)
}

func containsStr(slc []string, val string) bool {
	for _, v := range slc {
		if v == val {
			return true
		}
	}
	return false
}

func toAssetTrack(stream *ffprobe.Stream, strict bool) (*api.AssetTrack, error) {
	if stream.CodecType == "data" {
		return nil, nil
	} else if stream.CodecType == "video" {
		if !supportedVideoCodecs[stream.CodecName] {
			return nil, fmt.Errorf("unsupported video codec: %s", stream.CodecName)
		}
		if stream.PixFmt != "" && !supportedPixelFormats[stream.PixFmt] {
			return nil, fmt.Errorf("unsupported video pixel format: %s", stream.PixFmt)
		}
	} else if strict {
		if stream.CodecType != "video" && stream.CodecType != "audio" {
			return nil, fmt.Errorf("unsupported codec type: %s", stream.CodecType)
		} else if stream.CodecType == "audio" && !supportedAudioCodecs[stream.CodecName] {
			return nil, fmt.Errorf("unsupported audio codec: %s", stream.CodecName)
		}
	}

	startTime, err := strconv.ParseFloat(stream.StartTime, 64)
	if stream.StartTime != "" && err != nil {
		return nil, fmt.Errorf("error parsing start time from track %d: %w", stream.Index, err)
	}
	duration, err := strconv.ParseFloat(stream.Duration, 64)
	if stream.Duration != "" && err != nil {
		return nil, fmt.Errorf("error parsing duration from track %d: %w", stream.Index, err)
	}
	bitrate, err := strconv.ParseFloat(stream.BitRate, 64)
	if stream.BitRate != "" && err != nil {
		return nil, fmt.Errorf("error parsing bitrate from track %d: %w", stream.Index, err)
	}
	track := &api.AssetTrack{
		Type:        stream.CodecType,
		Codec:       stream.CodecName,
		StartTime:   startTime,
		DurationSec: duration,
		Bitrate:     bitrate,
	}
	if stream.CodecType == "video" {
		fps, err := parseFps(stream.AvgFrameRate)
		if err != nil {
			return nil, err
		}
		track.Width = stream.Width
		track.Height = stream.Height
		track.FPS = fps
		track.PixelFormat = stream.PixFmt
	} else if stream.CodecType == "audio" {
		sampleRate, err := strconv.Atoi(stream.SampleRate)
		if stream.SampleRate != "" && err != nil {
			return nil, fmt.Errorf("error parsing sample rate from track %d: %w", stream.Index, err)
		}
		bitDepth, err := strconv.Atoi(stream.BitsPerRawSample)
		if stream.BitsPerRawSample != "" && err != nil {
			return nil, fmt.Errorf("error parsing bit depth (bits_per_raw_sample) from track %d: %w", stream.Index, err)
		}
		track.Channels = stream.Channels
		track.SampleRate = sampleRate
		track.BitDepth = bitDepth
	}
	return track, nil
}

func parseFps(framerate string) (float64, error) {
	if framerate == "" {
		return 0, nil
	}
	parts := strings.SplitN(framerate, "/", 2)
	if len(parts) < 2 {
		fps, err := strconv.ParseFloat(framerate, 64)
		if err != nil {
			return 0, fmt.Errorf("error parsing framerate: %w", err)
		}
		return fps, nil
	}
	num, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("error parsing framerate numerator: %w", err)
	}
	den, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, fmt.Errorf("error parsing framerate denominator: %w", err)
	}
	return float64(num) / float64(den), nil
}

func logProbeData(assetId, filename string, probeData *ffprobe.ProbeData) {
	streamFields := []string{}
	var width, height int
	var maxStartTime float64
	for _, stream := range probeData.Streams {
		add := func(field string, value string) {
			streamFields = append(streamFields, fmt.Sprintf("stream_%d_%s=%q", stream.Index, field, value))
		}
		add("type", stream.CodecType)
		add("codec", stream.CodecName)
		add("bitrate", stream.BitRate)
		add("startTime", stream.StartTime)
		add("duration", stream.Duration)
		if stream.CodecType == "video" {
			add("pixelFormat", stream.PixFmt)
			width, height = stream.Width, stream.Height
		}
		if startTime, err := strconv.ParseFloat(stream.StartTime, 64); err == nil {
			maxStartTime = math.Max(maxStartTime, startTime)
		}
	}
	glog.Infof("Probed video file assetId=%s filename=%q format=%q width=%d height=%d bitrate=%s startTime=%v maxStreamStartTime=%v %s",
		assetId, filename, probeData.Format.FormatName, width, height, probeData.Format.BitRate,
		probeData.Format.StartTimeSeconds, maxStartTime, strings.Join(streamFields, " "))

	if glog.V(model.VERBOSE) {
		rawData, err := json.Marshal(probeData)
		if err != nil {
			glog.Errorf("Error JSON marshalling probe data err=%q", err)
		} else {
			glog.Infof("Raw ffprobe output assetId=%s filename=%q ffprobeOut=%q", assetId, filename, rawData)
		}
	}
}
