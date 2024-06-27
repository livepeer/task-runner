package api

import (
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"path"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/julienschmidt/httprouter"
	"github.com/livepeer/task-runner/clients"
	"github.com/livepeer/task-runner/metrics"
	"github.com/livepeer/task-runner/task"
)

type APIHandlerOptions struct {
	APIRoot, ServerName string
	Prometheus          bool
	Catalyst            *clients.CatalystOptions
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
		router.Handler("GET", "/metrics", metrics.ScrapeHandler())
	}

	hookHandler := metrics.ObservedHandlerFunc("catalyst_hook", router.catalystHook)
	hookHandler = authorized(opts.Catalyst.Secret, hookHandler)
	hookHandler = logger(hookHandler)
	router.Handler("POST", clients.CatalystHookPath(opts.APIRoot, ":id"), hookHandler)

	proxyPath := path.Join(opts.APIRoot, "/proxy/os")
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "https"
			req.URL.Host = "gateway.storjshare.io"
			req.URL.Path = strings.TrimPrefix(req.URL.Path, proxyPath)
		},
	}
	router.NotFound = logger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/") {
			r.URL.Path = "/" + r.URL.Path
		}
		if !strings.HasPrefix(r.URL.Path, proxyPath) {
			http.NotFound(w, r)
			return
		}

		errorChance := float32(0.5)
		if r.Method != "GET" {
			errorChance = 0.95
		}
		if rand.Float32() < errorChance {
			glog.Errorf("Random error for path=%s", r.URL.Path)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	return router
}

func logger(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		handler.ServeHTTP(w, r)
		glog.Infof("API request handled. method=%s url=%q proto=%s duration=%v", r.Method, r.URL, r.Proto, time.Since(start))
	})
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
	attemptID := r.URL.Query().Get("attemptId")

	var payload *clients.CatalystCallback
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		respondError(r, rw, http.StatusBadRequest, err)
		return
	}

	err := h.runner.HandleCatalysis(r.Context(), taskId, nextStep, attemptID, payload)
	if err != nil {
		respondError(r, rw, http.StatusInternalServerError, err)
		return
	}
	rw.WriteHeader(http.StatusNoContent)
}
