package task

import (
	"context"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	livepeerAPI "github.com/livepeer/go-api-client"
	ffprobe "gopkg.in/vansante/go-ffprobe.v2"
)

var (
	supportedFormats     = []string{"mp4", "mov"}
	supportedVideoCodecs = map[string]bool{"h264": true}
	supportedAudioCodecs = map[string]bool{"aac": true}
)

type FileMetadata struct {
	MD5       string                 `json:"md5"`
	SHA256    string                 `json:"sha256"`
	Ffprobe   *ffprobe.ProbeData     `json:"ffprobe"`
	AssetSpec *livepeerAPI.AssetSpec `json:"assetSpec"`
}

func Probe(ctx context.Context, filename string, data io.Reader) (*FileMetadata, error) {
	hasher := NewReadHasher(data)
	probeData, err := ffprobe.ProbeReader(ctx, hasher)
	if err != nil {
		return nil, fmt.Errorf("error probing file: %w", err)
	}
	if _, err := hasher.FinishReader(); err != nil {
		return nil, fmt.Errorf("error reading input: %w", err)
	}
	size, md5, sha256 := hasher.Size(), hasher.MD5(), hasher.SHA256()
	assetSpec, err := toAssetSpec(filename, probeData, size, []livepeerAPI.AssetHash{
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

func toAssetSpec(filename string, probeData *ffprobe.ProbeData, size int64, hash []livepeerAPI.AssetHash) (*livepeerAPI.AssetSpec, error) {
	if filename == "" && probeData.Format.Filename != "pipe:" {
		filename = probeData.Format.Filename
	}
	format, err := findFormat(supportedFormats, probeData.Format.FormatName, filename)
	if err != nil {
		return nil, err
	}
	bitrate, err := strconv.ParseFloat(probeData.Format.BitRate, 64)
	if probeData.Format.BitRate != "" && err != nil {
		return nil, fmt.Errorf("error parsing file bitrate: %w", err)
	}
	spec := &livepeerAPI.AssetSpec{
		Name: filename,
		Type: "video",
		Hash: hash,
		Size: size,
		VideoSpec: &livepeerAPI.AssetVideoSpec{
			Format:      format,
			DurationSec: probeData.Format.DurationSeconds,
			Bitrate:     bitrate,
			Tracks:      make([]*livepeerAPI.AssetTrack, 0, len(probeData.Streams)),
		},
	}
	if ns := len(probeData.Streams); ns > 2 {
		return nil, fmt.Errorf("too many tracks in file: %d", ns)
	}
	var hasVideo bool
	for _, stream := range probeData.Streams {
		track, err := toAssetTrack(stream)
		if err != nil {
			return nil, err
		}
		hasVideo = hasVideo || track.Type == "video"
		spec.VideoSpec.Tracks = append(spec.VideoSpec.Tracks, track)
	}
	if !hasVideo {
		return nil, fmt.Errorf("no video track found in file")
	}
	return spec, nil
}

func findFormat(supportedFormats []string, format, filename string) (string, error) {
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

func toAssetTrack(stream *ffprobe.Stream) (*livepeerAPI.AssetTrack, error) {
	if stream.CodecType != "video" && stream.CodecType != "audio" {
		return nil, fmt.Errorf("unsupported codec type: %s", stream.CodecType)
	} else if stream.CodecType == "video" && !supportedVideoCodecs[stream.CodecName] {
		return nil, fmt.Errorf("unsupported video codec: %s", stream.CodecName)
	} else if stream.CodecType == "audio" && !supportedAudioCodecs[stream.CodecName] {
		return nil, fmt.Errorf("unsupported audio codec: %s", stream.CodecName)
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
	track := &livepeerAPI.AssetTrack{
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
