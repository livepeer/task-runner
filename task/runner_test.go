package task

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHumanizeError(t *testing.T) {
	assert := assert.New(t)

	// Catalyst errors
	err := NewCatalystError("download error import request 504 Gateway Timeout", false)
	assert.ErrorIs(humanizeError(err), errFileInaccessible)

	err = NewCatalystError("download error import request 404 Not Found", false)
	assert.ErrorIs(humanizeError(err), errFileInaccessible)

	err = NewCatalystError("download error import request giving up after", false)
	assert.ErrorIs(humanizeError(err), errFileInaccessible)

	err = NewCatalystError("upload error: failed to write file foobar to foobar: unexpected EOF", false)
	assert.ErrorIs(humanizeError(err), errFileInaccessible)

	err = NewCatalystError("external transcoder error: job failed: 3450: Error encountered when accessing: foo", false)
	assert.ErrorIs(humanizeError(err), errFileInaccessible)

	err = NewCatalystError("foobar doesn't have video that the transcoder can consume foobar", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("foobar is not a supported input video codec foobar", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("foobar is not a supported input audio codec foobar", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("Demuxer: [ReadPacketData File read failed - end of file hit at length [5242880]. Is file truncated?]", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("foobar Failed probe/open: foobar", false)
	assert.ErrorIs(humanizeError(err), errProbe)

	err = NewCatalystError("no video track found in file", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("job failed: Decoder closed. No pictures decoded", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("external transcoder error: error copying input file to S3: download error: failed to fetch https://cfile.madmen.app/ipfs/ from any of the gateways", false)
	assert.ErrorIs(humanizeError(err), errFileInaccessible)

	err = NewCatalystError("external transcoder error: error probing MP4 input file from S3: error running ffprobe [] exit status 1", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("external transcoder error: zero bytes found for source: file:///dev/null", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("external transcoder error: invalid framerate: 0.000000", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("external transcoder error: job failed: video_description [3]: Maximum resolution is [8192]x[4320] or [4320]x[8192] in portrait mode, resolution specified is [5000]x[7000]", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("Unsupported video input: [High 4:4:4 Predictive profile is unsupported]", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("external transcoder error: job failed: IP Stage runtime error on gpu [0] pipeline. [Scaler position rectangle is outside output frame ]", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("error copying input to storage: failed to copy file(s): error downloading HLS input manifest: received non-Media manifest, but currently only Media playlists are supported", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("external transcoder error: job failed: No audio frames decoded on [selector-(Audio Selector 1)-track-1-drc]", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("external transcoder error: job failed: Frame rate is set to follow, but there is no frame rate information in the input stream info", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("external transcoder error: error creating mediaconvert job: InvalidParameter: 2 validation error(s) found.\n- minimum field value of 32, CreateJobInput.Settings.OutputGroups[0].Outputs[0].VideoDescription.Height.\n- minimum field value of 32, CreateJobInput.Settings.OutputGroups[0].Outputs[1].VideoDescription.Height.\n", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("transcoding failed: no valid segments in stream to transcode", false)
	assert.ErrorIs(humanizeError(err), errInvalidVideo)

	err = NewCatalystError("error copying input to storage: failed to copy file(s): error copying input file to S3: failed to write to OS URL \"s3+https://storage.googleapis.com/lp-us-catalyst-vod-com/hls/source/bdfgehfe/transfer\": MultipartUpload: upload multipart failed\n\tupload id: ABPnzm6RnhkmNJDt42Vk-cVm7lmNqrPVScIXmjIImSYzxlZ6L0MRhBSn0yfWu6_GNUko9e4\ncaused by: ReadRequestBody: read multipart upload data failed\ncaused by: unexpected EOF", false)
	assert.ErrorIs(humanizeError(err), errFileInaccessible)

	err = NewCatalystError("failed to write to OS URL \"s3+https:/key:************************@s3.filebase.com/videos-dev/foo/hls/index540p0_00002.ts\": AccessDenied: Access Denied\n\tstatus code: 403, request id: 150640cd52de34f9ce9932a563aab747, host id: ZmlsZWJhc2UtN2ZiYjhiZDRjNy10NW1ycA==", false)
	assert.ErrorIs(humanizeError(err), errFileInaccessible)

	// catalyst throws "probe failed for segment" when there is an issue with segments from the segmenting stage
	// don't humanize, this is an internal error which should be retried and investigated
	err = NewCatalystError("probe failed for segment s3+https://jwanjs7ubkrhavis4bgvlnpcui3a:jzyzwnplxgj3vi6l7nz2qcwlrgj3hkcgsudjzox3ml6qfppfa33me@gateway.storjshare.io/catalyst-vod-monster/hls/source/55i1gxd3/source/index0.ts: error probing", false)
	assert.ErrorIs(humanizeError(err), errInternalProcessingError)
}

func TestSimplePublishErrorDoesNotPanic(t *testing.T) {
	require := require.New(t)

	var publishedMsg *event.AMQPMessage
	producer := func(ctx context.Context, msg event.AMQPMessage) error {
		require.Nil(publishedMsg)
		publishedMsg = &msg
		return nil
	}

	exchange, taskInfo := "uniswap", data.TaskInfo{
		ID:   "LPT",
		Type: "ICO",
		Step: "merkle_mine",
	}
	var err error
	require.NotPanics(func() {
		err = simplePublishTaskFatalError(producerFunc(producer), exchange, taskInfo)
	})
	require.NoError(err)
	require.NotNil(publishedMsg)

	require.IsType(&data.TaskResultEvent{}, publishedMsg.Body)
	base := publishedMsg.Body.(*data.TaskResultEvent).Base

	require.NotZero(base.ID())
	require.Equal(data.EventTypeTaskResult, base.Type())
	require.LessOrEqual(time.Since(base.Timestamp()), 1*time.Second)

	require.Equal(*publishedMsg, event.AMQPMessage{
		Exchange:   "uniswap",
		Key:        "task.result.ICO.LPT",
		Persistent: true,
		Mandatory:  false,
		WaitResult: true,
		Body: &data.TaskResultEvent{
			Base: base,
			Task: taskInfo,
			Error: &data.ErrorInfo{
				Message:     "internal error processing file",
				Unretriable: true,
			},
		},
	})
}

type producerFunc func(ctx context.Context, msg event.AMQPMessage) error

func (f producerFunc) Publish(ctx context.Context, msg event.AMQPMessage) error {
	return f(ctx, msg)
}

func (f producerFunc) Shutdown(ctx context.Context) error {
	return nil
}

func TestHandleTaskAssetNotFound(t *testing.T) {
	apiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case strings.Contains(path, "/task/"):
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"outputAssetId": "397f0b70-0028-44e3-ad9f-2686e43afabe"}`))
		case strings.Contains(path, "/asset/"):
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer apiServer.Close()

	runner := &runner{
		lapi: api.NewAPIClient(api.ClientOptions{
			Server: apiServer.URL,
		}),
	}
	_, err := runner.handleTask(context.Background(), data.TaskInfo{ID: "task"})
	require.EqualError(t, err, "task cancelled, asset has been deleted")
}
