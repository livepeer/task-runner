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

var profile240p = livepeerAPI.Profile{
	Name:    "240p0",
	Fps:     0,
	Bitrate: 250000,
	Width:   426,
	Height:  240,
	Gop:     "2.0",
}

var allProfiles = []livepeerAPI.Profile{
	profile240p,
	{
		Name:    "360p0",
		Fps:     0,
		Bitrate: 800000,
		Width:   640,
		Height:  360,
		Gop:     "2.0",
	},
	{
		Name:    "480p0",
		Fps:     0,
		Bitrate: 1600000,
		Width:   854,
		Height:  480,
		Gop:     "2.0",
	},
	{
		Name:    "720p0",
		Fps:     0,
		Bitrate: 3000000,
		Width:   1280,
		Height:  720,
		Gop:     "2.0",
	},
}

func RecordStream(ctx context.Context, lapi *livepeerAPI.Client, assetSpec *livepeerAPI.AssetSpec, file io.ReadSeekCloser) (string, error) {
	streamName := fmt.Sprintf("vod_hls_recording_%s", time.Now().Format("2006-01-02T15:04:05Z07:00"))
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
		_, err = lapi.PushSegment(stream.ID, seg.SeqNo, seg.Duration, seg.Data)
		if err != nil {
			glog.Errorf("Error while segment push for prepare err=%v\n", err)
			break
		}
		glog.V(model.VERBOSE).Infof("Transcode %d took %s\n", len(transcoded), time.Since(started))
	}
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		return "", err
	}

	return stream.ID, nil
}

func getPlaybackProfiles(assetVideoSpec *livepeerAPI.AssetVideoSpec) ([]livepeerAPI.Profile, error) {
	assetHeight := -1
	for _, track := range assetVideoSpec.Tracks {
		if track.Type == "video" {
			assetHeight = track.Height
		}
	}
	if assetHeight < 0 {
		return nil, fmt.Errorf("no video track found in asset spec")
	}
	filtered := make([]livepeerAPI.Profile, 0, len(allProfiles))
	for _, profile := range allProfiles {
		if profile.Height <= assetHeight {
			filtered = append(filtered, profile)
		}
	}
	if len(filtered) == 0 {
		return []livepeerAPI.Profile{profile240p}, nil
	}
	return filtered, nil
}
