package api

import (
	"context"
	"fmt"
	"net/http"
	"path"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/task-runner/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type APIHandlerOptions struct {
	APIRoot, ServerName string
	Prometheus          bool
}

type apiHandler struct {
	opts      APIHandlerOptions
	serverCtx context.Context
}

func NewHandler(serverCtx context.Context, opts APIHandlerOptions) http.Handler {
	handler := &apiHandler{opts, serverCtx}

	router := httprouter.New()
	router.HandlerFunc("GET", "/_healthz", handler.healthcheck)
	if opts.Prometheus {
		router.Handler("GET", "/metrics", promhttp.Handler())
	}
	router.Handler("POST", CataylistHookPath(opts.APIRoot, ":id"),
		metrics.ObservedHandlerFunc("catalyst_hook", handler.catalystHook))
	return handler.withDefaultHeaders(router)
}

func (h *apiHandler) withDefaultHeaders(next http.Handler) http.HandlerFunc {
	return func(rw http.ResponseWriter, r *http.Request) {
		if h.opts.ServerName != "" {
			rw.Header().Set("Server", h.opts.ServerName)
		}
		next.ServeHTTP(rw, r)
	}
}

func (h *apiHandler) healthcheck(rw http.ResponseWriter, r *http.Request) {
	rw.WriteHeader(http.StatusOK)
}

func (h *apiHandler) catalystHook(rw http.ResponseWriter, r *http.Request) {
	taskId := httprouter.ParamsFromContext(r.Context()).ByName("id")
	// TODO: something
	rw.WriteHeader(http.StatusOK)
	rw.Write([]byte("task " + taskId))
}

func CataylistHookPath(apiRoot, taskId string) string {
	return path.Join(apiRoot, fmt.Sprintf("/webhook/catalyst/task/%s", taskId))
}
