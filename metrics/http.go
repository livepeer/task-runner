package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	httpReqsDuration = Factory.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: FQName("http_requests_duration_seconds"),
			Help: "Request duration of HTTP requests in seconds",
		},
		[]string{"code", "method", "api"},
	)
	httpReqsTimeToHeaders = Factory.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: FQName("http_requests_time_to_headers_seconds"),
			Help: "Time until HTTP headers are written, in seconds",
		},
		[]string{"code", "method", "api"},
	)
	httpReqsInFlight = Factory.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: FQName("http_requests_in_flight"),
			Help: "Number of current requests in-flight for the specific API",
		},
		[]string{"api"},
	)
)

func ObservedHandlerFunc(apiName string, handler http.HandlerFunc) http.Handler {
	return ObservedHandler(apiName, handler)
}

func ObservedHandler(apiName string, handler http.Handler) http.Handler {
	apiLabel := prometheus.Labels{"api": apiName}
	handler = promhttp.InstrumentHandlerTimeToWriteHeader(
		httpReqsTimeToHeaders.MustCurryWith(apiLabel),
		handler)
	handler = promhttp.InstrumentHandlerDuration(
		httpReqsDuration.MustCurryWith(apiLabel),
		handler)
	handler = promhttp.InstrumentHandlerInFlight(
		httpReqsInFlight.WithLabelValues(apiName),
		handler)
	return handler
}
