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

	// test if it contains some random metric
	require.Contains(rw.Body.String(), "# TYPE livepeer_task_runner_http_requests_in_flight gauge\n")
}
