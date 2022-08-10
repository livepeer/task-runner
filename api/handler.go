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
	*httprouter.Router
	opts      APIHandlerOptions
	serverCtx context.Context
	runner    task.Runner
}

func NewHandler(serverCtx context.Context, opts APIHandlerOptions, runner task.Runner) http.Handler {
	router := &apiHandler{httprouter.New(), opts, serverCtx, runner}

	router.HandlerFunc("GET", "/_healthz", router.healthcheck)
	if opts.Prometheus {
		router.Handler("GET", "/metrics", promhttp.Handler())
	}

	hookHandler := metrics.ObservedHandlerFunc("catalyst_hook", router.catalystHook)
	hookHandler = authorized(opts.CatalystSecret, hookHandler)
	router.Handler("POST", CataylistHookPath(opts.APIRoot, ":id"), hookHandler)
	return router
}

func (h *apiHandler) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	if h.opts.ServerName != "" {
		rw.Header().Set("Server", h.opts.ServerName)
	}
	h.Router.ServeHTTP(rw, r)
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
