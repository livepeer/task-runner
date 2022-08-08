package task

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-tools/drivers"
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
	segLen               = 2 * time.Second
	maxFileSizeForMemory = 50_000_000
)

func init() {
	format.RegisterAll()
	rand.Seed(time.Now().UnixNano())
}

func readFileToMemory(fir *drivers.FileInfoReader) (io.ReadSeekCloser, error) {
	fileInMem, err := io.ReadAll(fir.Body)
	if err != nil {
		return nil, err
	}
	return nopCloser{bytes.NewReader(fileInMem)}, nil
}

type autodeletingFile struct {
	*os.File
}

func (adf *autodeletingFile) Reader() io.ReadSeekCloser {
	adf.Seek(0, io.SeekStart)
	return adf
}

func (adf *autodeletingFile) Close() error {
	err := adf.File.Close()
	os.Remove(adf.File.Name())
	return err
}

func getTempFile(size int64) (*os.File, error) {
	file, err := os.CreateTemp("", "transcode")
	if err != nil {
		glog.Errorf("Error creating temporary file err=%v", err)
		return nil, err
	}
	glog.Infof("Created temporary file name=%s", file.Name())
	if size > 0 {
		offset, err := file.Seek(size, io.SeekStart)
		if err != nil || offset != size {
			os.Remove(file.Name())
			glog.Errorf("Error creating temporary file name=%s with size=%d offset=%d err=%v", file.Name(), size, offset, err)
			return nil, err
		}
		file.Seek(0, io.SeekStart)
	}
	return file, nil
}

func readFile(fir *drivers.FileInfoReader) (io.ReadSeekCloser, error) {
	var fileSize int64
	if fir.Size != nil {
		fileSize = *fir.Size
	}
	glog.Infof("Source file name=%s size=%d", fir.Name, fileSize)
	if fir.Size != nil && *fir.Size < maxFileSizeForMemory {
		// use memory
		return readFileToMemory(fir)
	}
	if file, err := getTempFile(fileSize); err != nil {
		return readFileToMemory(fir)
	} else {
		if _, err = file.ReadFrom(fir.Body); err != nil {
			file.Close()
			os.Remove(file.Name())
			return nil, err
		}
		file.Seek(0, io.SeekStart)
		return &autodeletingFile{file}, nil
	}
}

type WriteSeekCloser interface {
	io.WriteSeeker
	io.Closer
	Reader() io.ReadSeekCloser
}

func fileWriter(size int64) WriteSeekCloser {
	if size > 0 && size < maxFileSizeForMemory {
		// use memory
		return &memWriteSeeker{}
	}
	if file, err := getTempFile(size); err != nil {
		return &memWriteSeeker{}
	} else {
		return &autodeletingFile{file}
	}
}

type Accumulator struct {
	size uint64
}

func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

func (a *Accumulator) Size() uint64 {
	return atomic.LoadUint64(&a.size)
}

func (a *Accumulator) Accumulate(size uint64) {
	atomic.AddUint64(&a.size, size)
}

func TaskTranscode(tctx *TaskContext) (*data.TaskOutput, error) {
	var (
		ctx             = tctx.Context
		outAsset        = tctx.OutputAsset
		inputPlaybackID = tctx.InputAsset.PlaybackID
		lapi            = tctx.lapi
	)

	fullPath := videoFileName(inputPlaybackID)
	fir, err := tctx.inputOS.ReadData(ctx, fullPath)
	if err != nil {
		return nil, fmt.Errorf("error reading data from source OS url=%s err=%w", fullPath, err)
	}
	sourceFile, err := readFile(fir)
	fir.Body.Close()
	if err != nil {
		return nil, err
	}
	defer sourceFile.Close()
	var sourceFileSize int64
	if fir.Size != nil {
		sourceFileSize = *fir.Size
	}

	streamName := fmt.Sprintf("vod_%s", time.Now().Format("2006-01-02T15:04:05Z07:00"))
	profiles := []api.Profile{tctx.Params.Transcode.Profile}
	stream, err := lapi.CreateStreamR(api.CreateStreamReq{Name: streamName, Profiles: profiles})
	if err != nil {
		return nil, err
	}
	defer lapi.DeleteStream(stream.ID)

	glog.V(model.DEBUG).Infof("Created vod stream id=%s name=%s\n", stream.ID, stream.Name)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	segmentsIn := make(chan *model.HlsSegment)
	if err = segmenter.StartSegmentingR(ctx, sourceFile, true, 0, 0, segLen, false, segmentsIn); err != nil {
		return nil, err
	}
	var outFiles []av.Muxer
	var outBuffers []WriteSeekCloser
	ws := fileWriter(sourceFileSize)
	defer ws.Close()
	mp4muxer := mp4.NewMuxer(ws)
	outFiles = append(outFiles, mp4muxer)
	outBuffers = append(outBuffers, ws)
	var transcoded [][]byte
	contentResolution := ""
	for _, track := range tctx.InputAsset.AssetSpec.VideoSpec.Tracks {
		if track.Type == "video" {
			contentResolution = fmt.Sprintf("%dx%d", track.Width, track.Height)
			break
		}
	}
	err = nil
	accumulator := NewAccumulator()
	progressCtx, cancelProgress := context.WithCancel(ctx)
	defer cancelProgress()
	go ReportProgress(progressCtx, lapi, tctx.Task.ID, uint64(sourceFileSize), accumulator.Size, 0, 0.5)
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
		transcoded, err = lapi.PushSegmentR(stream.ID, seg.SeqNo, seg.Duration, seg.Data, contentResolution)
		if err != nil {
			glog.Errorf("Segment push playbackID=%s err=%v\n", inputPlaybackID, err)
			break
		}
		accumulator.Accumulate(uint64(len(seg.Data)))
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
	if ctxErr := ctx.Err(); err == nil && ctxErr != nil {
		err = ctxErr
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	for i, f := range outFiles {
		if err := f.WriteTrailer(); err != nil {
			glog.Errorf("Error writing trailer file=%d err=%v\n", i, err)
			return nil, fmt.Errorf("error writing trailer of file %d: %w", i, err)
		}
	}
	fullPath = videoFileName(outAsset.PlaybackID)
	ws = outBuffers[0]
	videoFilePath, err := tctx.outputOS.SaveData(ctx, fullPath, ws.Reader(), nil, fileUploadTimeout)
	if err != nil {
		return nil, fmt.Errorf("error uploading file=%q to object store: %w", fullPath, err)
	}
	glog.Infof("Saved file with playbackID=%s to url=%s", outAsset.PlaybackID, videoFilePath)

	metadata, err := Probe(ctx, outAsset.ID, outAsset.Name+"_"+tctx.Params.Transcode.Profile.Name, NewReadCounter(ws.Reader()))
	if err != nil {
		return nil, err
	}
	metadataFilePath, err := saveMetadataFile(ctx, tctx.outputOS, outAsset.PlaybackID, metadata)
	if err != nil {
		return nil, err
	}
	cancelProgress()
	playbackRecordingId, err := Prepare(tctx.WithContext(ctx), metadata.AssetSpec, ws.Reader(), 0.5)
	if err != nil {
		glog.Errorf("Error preparing file assetId=%s taskType=transcode err=%q", tctx.OutputAsset.ID, err)
		return nil, fmt.Errorf("error preparing asset: %w", err)
	}
	assetSpec := *metadata.AssetSpec
	assetSpec.PlaybackRecordingID = playbackRecordingId
	return &data.TaskOutput{
		Transcode: &data.TranscodeTaskOutput{
			Asset: data.ImportTaskOutput{
				VideoFilePath:    videoFilePath,
				MetadataFilePath: metadataFilePath,
				AssetSpec:        assetSpec,
			},
		},
	}, nil
}

// memWriteSeeker is an in-memory io.WriteSeeker implementation
type memWriteSeeker struct {
	buf bytes.Buffer
	pos int
}

// Write writes to the buffer of this WriterSeeker instance
func (ws *memWriteSeeker) Write(p []byte) (n int, err error) {
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
func (ws *memWriteSeeker) Seek(offset int64, whence int) (int64, error) {
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
func (ws *memWriteSeeker) Reader() io.ReadSeekCloser {
	return nopCloser{bytes.NewReader(ws.buf.Bytes())}
}

// Close :
func (ws *memWriteSeeker) Close() error {
	return nil
}

type nopCloser struct {
	*bytes.Reader
}

func (nopCloser) Close() error { return nil }
