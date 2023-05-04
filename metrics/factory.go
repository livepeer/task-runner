package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	Namespace = "livepeer"
	Subsystem = "task_runner"
	// We use catalyst-api as a library which uses prometheus lib and default
	// registry. Avoid metric conflicts by using a custom registry.
	Registry = prometheus.NewRegistry()
	Factory  = promauto.With(Registry)
)

func FQName(name string) string {
	return prometheus.BuildFQName(Namespace, Subsystem, name)
}

func ScrapeHandler() http.Handler {
	handler := promhttp.HandlerFor(Registry, promhttp.HandlerOpts{})
	handler = promhttp.InstrumentMetricHandler(Registry, handler)
	return handler
}
