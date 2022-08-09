package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"

	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/task-runner/clients"
	"github.com/livepeer/task-runner/metrics"
	"github.com/livepeer/task-runner/task"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type APIHandlerOptions struct {
	APIRoot, ServerName string
	CatalystSecret      string
	Prometheus          bool
}

type apiHandler struct {
	opts      APIHandlerOptions
	serverCtx context.Context
	runner    task.Runner
}

func NewHandler(serverCtx context.Context, opts APIHandlerOptions, runner task.Runner) http.Handler {
	handler := &apiHandler{opts, serverCtx, runner}

	router := httprouter.New()
	router.HandlerFunc("GET", "/_healthz", handler.healthcheck)
	if opts.Prometheus {
		router.Handler("GET", "/metrics", promhttp.Handler())
	}

	hookHandler := metrics.ObservedHandlerFunc("catalyst_hook", handler.catalystHook)
	hookHandler = authorized(opts.CatalystSecret, hookHandler)
	router.Handler("POST", CataylistHookPath(opts.APIRoot, ":id"), hookHandler)

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
	nextStep := r.URL.Query().Get("nextStep")

	var payload *clients.CatalystCallback
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		respondError(rw, http.StatusBadRequest, err)
		return
	}

	err := h.runner.HandleCatalysis(r.Context(), taskId, nextStep, payload)
	if err != nil {
		respondError(rw, http.StatusInternalServerError, err)
		return
	}
	rw.WriteHeader(http.StatusNoContent)
}

func CataylistHookPath(apiRoot, taskId string) string {
	return path.Join(apiRoot, fmt.Sprintf("/webhook/catalyst/task/%s", taskId))
}
