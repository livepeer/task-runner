package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client"
)

type errorResponse struct {
	Errors []string `json:"errors"`
}

func respondError(r *http.Request, rw http.ResponseWriter, defaultStatus int, errs ...error) {
	status := defaultStatus
	response := errorResponse{}
	for _, err := range errs {
		response.Errors = append(response.Errors, err.Error())
		if errors.Is(err, api.ErrNotExists) {
			status = http.StatusNotFound
		}
	}
	glog.Warningf("API ended in error. method=%s url=%q status=%d, errors=%+v", r.Method, r.URL, status, response.Errors)
	respondJson(rw, status, response)
}

func respondJson(rw http.ResponseWriter, status int, response interface{}) {
	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	rw.WriteHeader(status)
	if err := json.NewEncoder(rw).Encode(response); err != nil {
		glog.Errorf("Error writing response. err=%q, response=%+v", err, response)
	}
}

func nonNilErrs(errs ...error) []error {
	var nonNil []error
	for _, err := range errs {
		if err != nil {
			nonNil = append(nonNil, err)
		}
	}
	return nonNil
}
