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

func RecordStream(ctx context.Context, lapi *livepeerAPI.Client, file io.ReadSeekCloser) (string, error) {
	streamName := fmt.Sprintf("vod_hls_recording_%s", time.Now().Format("2006-01-02T15:04:05Z07:00"))
	stream, err := lapi.CreateStreamEx(streamName, true, nil)
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
			glog.Errorf("Error while segmenting file for prepare err=%v", file, err)
			break
		}
		glog.V(model.VERBOSE).Infof("Got segment seqNo=%d pts=%s dur=%s data len bytes=%d\n", seg.SeqNo, seg.Pts, seg.Duration, len(seg.Data))
		started := time.Now()
		_, err = lapi.PushSegment(stream.ID, seg.SeqNo, seg.Duration, seg.Data)
		if err != nil {
			glog.Errorf("Error while segment push for prepare err=%v\n", file, err)
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
