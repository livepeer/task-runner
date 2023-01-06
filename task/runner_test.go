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
	assert.EqualError(humanizeError(err), errFileInaccessible.Error())

	err = NewCatalystError("download error import request 404 Not Found", false)
	assert.EqualError(humanizeError(err), errFileInaccessible.Error())

	err = NewCatalystError("download error import request giving up after", false)
	assert.EqualError(humanizeError(err), errFileInaccessible.Error())

	err = NewCatalystError("upload error: failed to write file foobar to foobar: unexpected EOF", false)
	assert.EqualError(humanizeError(err), errFileInaccessible.Error())

	err = NewCatalystError("external transcoder error: job failed: 3450: Error encountered when accessing: foo", false)
	assert.EqualError(humanizeError(err), errFileInaccessible.Error())

	err = NewCatalystError("foobar doesn't have video that the transcoder can consume foobar", false)
	assert.EqualError(humanizeError(err), errInvalidVideo.Error())

	err = NewCatalystError("foobar is not a supported input video codec foobar", false)
	assert.EqualError(humanizeError(err), errInvalidVideo.Error())

	err = NewCatalystError("foobar is not a supported input audio codec foobar", false)
	assert.EqualError(humanizeError(err), errInvalidVideo.Error())

	err = NewCatalystError("Demuxer: [ReadPacketData File read failed - end of file hit at length [5242880]. Is file truncated?]", false)
	assert.EqualError(humanizeError(err), errInvalidVideo.Error())

	err = NewCatalystError("foobar Failed probe/open: foobar", false)
	assert.EqualError(humanizeError(err), errProbe.Error())
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
