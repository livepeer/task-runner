package api

import (
	"context"
	"net/http"

	"github.com/julienschmidt/httprouter"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type APIHandlerOptions struct {
	APIRoot    string
	Prometheus bool
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
	return router
}

func (h *apiHandler) healthcheck(rw http.ResponseWriter, r *http.Request) {
	rw.WriteHeader(http.StatusOK)
}
