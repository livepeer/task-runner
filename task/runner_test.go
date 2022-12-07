package task

import (
	"context"
	"testing"
	"time"

	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/livepeer-data/pkg/event"
	"github.com/stretchr/testify/require"
)

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
		Mandatory:  true,
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
