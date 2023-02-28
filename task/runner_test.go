package task

import (
	"context"
	"testing"
	"time"

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
