package task

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-livepeer/drivers"
	"github.com/livepeer/stream-tester/model"
	"github.com/livepeer/stream-tester/segmenter"
)

func RecordStream(ctx context.Context, lapi *livepeerAPI.Client, playbackID string, osSess drivers.OSSession) (string, error) {
	glog.Infof("made it into record function")
	fullPath := videoFileName(playbackID)
	glog.Infof("full path %s", fullPath)
	fir, err := osSess.ReadData(ctx, fullPath)
	glog.Infof("inputOS %b", osSess.IsExternal())
	if err != nil {
		return "", fmt.Errorf("error reading data from source OS url=%s err=%w", fullPath, err)
	}
	sourceFile, err := readFile(fir)
	fir.Body.Close()
	if err != nil {
		return "", err
	}
	defer sourceFile.Close()
	var sourceFileSize int64
	if fir.Size != nil {
		sourceFileSize = *fir.Size
	}

	streamName := fmt.Sprintf("vod_recording_%s", time.Now().Format("2006-01-02T15:04:05Z07:00"))
	stream, err := lapi.CreateStreamEx(streamName, true, nil)
	if err != nil {
		return "", nil
	}
	defer lapi.DeleteStream(stream.ID) // if we delete the recording stream for cleanup will the session/recording persist?

	gctx, gcancel := context.WithCancel(ctx)
	defer gcancel()
	segmentsIn := make(chan *model.HlsSegment)
	if err = segmenter.StartSegmentingR(gctx, sourceFile, true, 0, 0, segLen, false, segmentsIn); err != nil {
		return "", err
	}
	ws := fileWriter(sourceFileSize)
	defer ws.Close()
	var transcoded [][]byte
	err = nil

	for seg := range segmentsIn {
		if seg.Err == io.EOF {
			break
		}
		if seg.Err != nil {
			err = seg.Err
			glog.Errorf("Error while segmenting playbackID=%s err=%v", playbackID, err)
			break
		}
		glog.V(model.VERBOSE).Infof("Got segment seqNo=%d pts=%s dur=%s data len bytes=%d\n", seg.SeqNo, seg.Pts, seg.Duration, len(seg.Data))
		started := time.Now()
		_, err = lapi.PushSegment(stream.ID, seg.SeqNo, seg.Duration, seg.Data)
		if err != nil {
			glog.Errorf("Segment push playbackID=%s err=%v\n", playbackID, err)
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
