package task

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/stream-tester/model"
	"github.com/livepeer/stream-tester/segmenter"
)

const (
	minVideoBitrate         = 100_000
	absoluteMinVideoBitrate = 5_000
	minVideoDimensionPixels = 145
)

var allProfiles = []livepeerAPI.Profile{
	{
		Name:    "240p0",
		Fps:     0,
		Bitrate: 250_000,
		Width:   426,
		Height:  240,
		Gop:     "2.0",
	},
	{
		Name:    "360p0",
		Fps:     0,
		Bitrate: 800_000,
		Width:   640,
		Height:  360,
		Gop:     "2.0",
	},
	{
		Name:    "480p0",
		Fps:     0,
		Bitrate: 1_600_000,
		Width:   854,
		Height:  480,
		Gop:     "2.0",
	},
	{
		Name:    "720p0",
		Fps:     0,
		Bitrate: 3_000_000,
		Width:   1280,
		Height:  720,
		Gop:     "2.0",
	},
}

func Prepare(tctx *TaskContext, assetSpec *livepeerAPI.AssetSpec, file io.ReadSeekCloser) (string, error) {
	var (
		ctx     = tctx.Context
		lapi    = tctx.lapi
		assetId = tctx.OutputAsset.ID
	)

	streamName := fmt.Sprintf("vod_hls_recording_%s", assetId)
	profiles, err := getPlaybackProfiles(assetSpec.VideoSpec)
	if err != nil {
		return "", nil
	}
	stream, err := lapi.CreateStreamEx2(streamName, true, "", nil, profiles...)
	if err != nil {
		return "", nil
	}
	defer lapi.DeleteStream(stream.ID)

	gctx, gcancel := context.WithCancel(ctx)
	defer gcancel()
	segmentsIn := make(chan *model.HlsSegment)
	if err = segmenter.StartSegmentingR(gctx, file, true, 0, 0, segLen, false, segmentsIn); err != nil {
		return "", err
	}
	var transcoded [][]byte
	err = nil

	contentResolution := ""
	for _, track := range assetSpec.VideoSpec.Tracks {
		if track.Type == "video" {
			contentResolution = fmt.Sprintf("%dx%d", track.Width, track.Height)
			break
		}
	}

	for seg := range segmentsIn {
		if seg.Err == io.EOF {
			break
		}
		if seg.Err != nil {
			err = seg.Err
			glog.Errorf("Error while segmenting file for prepare err=%v", err)
			break
		}
		glog.V(model.VERBOSE).Infof("Got segment seqNo=%d pts=%s dur=%s data len bytes=%d\n", seg.SeqNo, seg.Pts, seg.Duration, len(seg.Data))
		started := time.Now()
		_, err = lapi.PushSegment(stream.ID, seg.SeqNo, seg.Duration, seg.Data, contentResolution)
		if err != nil {
			glog.Errorf("Error while segment push for prepare err=%v\n", err)
			break
		}
		glog.V(model.VERBOSE).Infof("Transcode %d took %s\n", len(transcoded), time.Since(started))
	}
	if err != nil && err != io.EOF {
		return "", err
	}
	return stream.ID, nil
}

func getPlaybackProfiles(assetVideoSpec *livepeerAPI.AssetVideoSpec) ([]livepeerAPI.Profile, error) {
	var video *livepeerAPI.AssetTrack
	for _, track := range assetVideoSpec.Tracks {
		if track.Type == "video" {
			video = track
		}
	}
	if video == nil {
		return nil, fmt.Errorf("no video track found in asset spec")
	}
	filtered := make([]livepeerAPI.Profile, 0, len(allProfiles))
	for _, baseProfile := range allProfiles {
		profile := effectiveProfile(baseProfile, video)
		if profile.Height <= video.Height && profile.Bitrate < int(video.Bitrate) &&
			profile.Height > minVideoDimensionPixels && profile.Width > minVideoDimensionPixels {
			filtered = append(filtered, profile)
		}
	}
	if len(filtered) == 0 {
		return []livepeerAPI.Profile{lowBitrateProfile(video)}, nil
	}
	return filtered, nil
}

func effectiveProfile(profile livepeerAPI.Profile, video *livepeerAPI.AssetTrack) livepeerAPI.Profile {
	if video.Width >= video.Height {
		profile.Height = profile.Width * video.Height / video.Width
	} else {
		profile.Width = profile.Height * video.Width / video.Height
	}
	return profile
}

func lowBitrateProfile(video *livepeerAPI.AssetTrack) livepeerAPI.Profile {
	bitrate := int(video.Bitrate / 3)
	if bitrate < minVideoBitrate && video.Bitrate > minVideoBitrate {
		bitrate = minVideoBitrate
	} else if bitrate < absoluteMinVideoBitrate {
		bitrate = absoluteMinVideoBitrate
	}
	return livepeerAPI.Profile{
		Name:    "low-bitrate",
		Fps:     0,
		Bitrate: bitrate,
		Width:   video.Width,
		Height:  video.Height,
	}
}
