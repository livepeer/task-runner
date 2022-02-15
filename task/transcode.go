package task

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"time"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/joy4/av"
	"github.com/livepeer/joy4/av/avutil"
	"github.com/livepeer/joy4/format"
	"github.com/livepeer/joy4/format/mp4"
	"github.com/livepeer/joy4/format/ts"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/stream-tester/model"
	"github.com/livepeer/stream-tester/segmenter"
)

const (
	segLen = 5 * time.Second
)

var glapi *livepeerAPI.Client

func init() {
	format.RegisterAll()
	rand.Seed(time.Now().UnixNano())
	opts := livepeerAPI.ClientOptions{
		AccessToken: "f8750a7c-084e-4315-8f8b-900237d48a57",
	}
	glapi = livepeerAPI.NewAPIClient(opts)
}

func TaskTranscode(tctx *TaskContext) (*data.TaskOutput, error) {
	var (
		ctx             = tctx.Context
		inputPlaybackID = tctx.InputAsset.PlaybackID
		lapi            = tctx.lapi
	)
	lapi = glapi

	fullPath := videoFileName(inputPlaybackID)
	fir, err := tctx.inputOS.ReadData(ctx, fullPath)
	if err != nil {
		return nil, fmt.Errorf("error reading data from source OS url=%s err=%w", fullPath, err)
	}
	fileInMem, err := io.ReadAll(fir.Body)
	fir.Body.Close()
	if err != nil {
		return nil, err
	}

	streamName := fmt.Sprintf("vod_%s", time.Now().Format("2006-01-02T15:04:05Z07:00"))
	profile := tctx.Params.Transcode.Profile
	stream, err := lapi.CreateStreamEx(streamName, false, nil, profile)
	if err != nil {
		return nil, err
	}
	defer lapi.DeleteStream(stream.ID)

	glog.V(model.DEBUG).Infof("Created vod stream id=%s name=%s\n", stream.ID, stream.Name)
	gctx, gcancel := context.WithCancel(ctx)
	defer gcancel()
	segmentsIn := make(chan *model.HlsSegment)
	if err = segmenter.StartSegmentingR(gctx, &ReaderClose{*bytes.NewReader(fileInMem)}, true, 0, 0, segLen, false, segmentsIn); err != nil {
		return nil, err
	}
	var outFiles []av.Muxer
	var outBuffers []*WriterSeeker
	ws := &WriterSeeker{}
	mp4muxer := mp4.NewMuxer(ws)
	outFiles = append(outFiles, mp4muxer)
	outBuffers = append(outBuffers, ws)
	var transcoded [][]byte
	err = nil
out:
	for seg := range segmentsIn {
		if seg.Err == io.EOF {
			break
		}
		if seg.Err != nil {
			err = seg.Err
			glog.Errorf("Error while segmenting playbackID=%s err=%v", inputPlaybackID, err)
			break
		}
		glog.V(model.VERBOSE).Infof("Got segment seqNo=%d pts=%s dur=%s data len bytes=%d\n", seg.SeqNo, seg.Pts, seg.Duration, len(seg.Data))
		started := time.Now()
		transcoded, err = lapi.PushSegment(stream.ID, seg.SeqNo, seg.Duration, seg.Data)
		if err != nil {
			glog.Errorf("Segment push playbackID=%s err=%v\n", inputPlaybackID, err)
			break
		}
		glog.V(model.VERBOSE).Infof("Transcode %d took %s\n", len(transcoded), time.Since(started))

		for i, segData := range transcoded {
			demuxer := ts.NewDemuxer(bytes.NewReader(segData))
			if seg.SeqNo == 0 {
				streams, err := demuxer.Streams()
				if err != nil {
					glog.Errorf("error in demuxer err=%v", err)
					break out
				}
				if err = outFiles[i].WriteHeader(streams); err != nil {
					glog.Errorf("Write header err=%v\n", err)
					break out
				}
			}
			if err = avutil.CopyPackets(outFiles[i], demuxer); err != io.EOF {
				glog.Errorf("copy packets media %d err=%v\n", i, err)
				break out
			}
		}
	}
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	var videoFilePath string
	outFiles[0].WriteTrailer()
	asset := tctx.OutputAsset
	fullPath = videoFileName(asset.PlaybackID)
	ws = outBuffers[0]
	videoFilePath, err = tctx.outputOS.SaveData(gctx, fullPath, ws.Reader(), nil, fileUploadTimeout)
	if err != nil {
		return nil, fmt.Errorf("error uploading file=%q to object store: %w", fullPath, err)
	} else {
		glog.Infof("Saved file with playbackID=%s to url=%s", asset.PlaybackID, videoFilePath)
	}

	metadata, err := Probe(gctx, asset.Name+"_"+tctx.Params.Transcode.Profile.Name, NewReadCounter(ws.Reader()))
	if err != nil {
		return nil, err
	}
	metadataFilePath, err := saveMetadataFile(gctx, tctx.outputOS, asset.PlaybackID, metadata)
	if err != nil {
		return nil, err
	}
	return &data.TaskOutput{
		Transcode: &data.TranscodeTaskOutput{
			Asset: data.ImportTaskOutput{
				VideoFilePath:    videoFilePath,
				MetadataFilePath: metadataFilePath,
				AssetSpec:        metadata.AssetSpec,
			},
		},
	}, nil
}

// WriterSeeker is an in-memory io.WriteSeeker implementation
type WriterSeeker struct {
	buf bytes.Buffer
	pos int
}

// Write writes to the buffer of this WriterSeeker instance
func (ws *WriterSeeker) Write(p []byte) (n int, err error) {
	// If the offset is past the end of the buffer, grow the buffer with null bytes.
	if extra := ws.pos - ws.buf.Len(); extra > 0 {
		if _, err := ws.buf.Write(make([]byte, extra)); err != nil {
			return n, err
		}
	}

	// If the offset isn't at the end of the buffer, write as much as we can.
	if ws.pos < ws.buf.Len() {
		n = copy(ws.buf.Bytes()[ws.pos:], p)
		p = p[n:]
	}

	// If there are remaining bytes, append them to the buffer.
	if len(p) > 0 {
		var bn int
		bn, err = ws.buf.Write(p)
		n += bn
	}

	ws.pos += n
	return n, err
}

// Seek seeks in the buffer of this WriterSeeker instance
func (ws *WriterSeeker) Seek(offset int64, whence int) (int64, error) {
	newPos, offs := 0, int(offset)
	switch whence {
	case io.SeekStart:
		newPos = offs
	case io.SeekCurrent:
		newPos = ws.pos + offs
	case io.SeekEnd:
		newPos = ws.buf.Len() + offs
	}
	if newPos < 0 {
		return 0, errors.New("negative result pos")
	}
	ws.pos = newPos
	return int64(newPos), nil
}

// Reader returns an io.Reader. Use it, for example, with io.Copy, to copy the content of the WriterSeeker buffer to an io.Writer
func (ws *WriterSeeker) Reader() io.Reader {
	return bytes.NewReader(ws.buf.Bytes())
}

// Close :
func (ws *WriterSeeker) Close() error {
	return nil
}

// BytesReader returns a *bytes.Reader. Use it when you need a reader that implements the io.ReadSeeker interface
func (ws *WriterSeeker) BytesReader() *bytes.Reader {
	return bytes.NewReader(ws.buf.Bytes())
}

type ReaderClose struct {
	bytes.Reader
}

func (ReaderClose) Close() error { return nil }
