package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/livepeer/task-runner/clients"
	"github.com/stretchr/testify/require"
)

func TestMetricsEndpoint(t *testing.T) {
	require := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := NewHandler(ctx, APIHandlerOptions{
		Catalyst:   &clients.CatalystOptions{Secret: "🤫"},
		Prometheus: true,
	}, nil)

	req, err := http.NewRequest("GET", "http://localhost/metrics", nil)
	require.NoError(err)

	rw := httptest.NewRecorder()
	handler.ServeHTTP(rw, req)

	res := rw.Result()
	require.Equal(http.StatusOK, res.StatusCode)
	require.Contains(res.Header.Get("Content-Type"), "text/plain")

	body := rw.Body.String()
	require.NotEmpty(body)

	// check that it contains some random metric
	require.Contains(body, "# TYPE livepeer_task_runner_http_requests_in_flight gauge\n")
	// check that it does NOT contain metrics from some libs we use
	require.NotContains(body, "catalyst-api")
	require.NotContains(body, "transcode_segment_duration_seconds")
	require.NotContains(body, "livepeer_analyzer")
}
