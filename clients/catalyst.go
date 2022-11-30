package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/catalyst-api/clients"
)

var (
	ErrRateLimited        = errors.New("rate limited")
	CatalystStatusSuccess = clients.TranscodeStatusCompleted.String()
	CatalystStatusError   = clients.TranscodeStatusError.String()
)

const (
	rateLimitRetryInitialDelay = 2 * time.Second
	maxAttempts                = 4
)

type UploadVODRequest struct {
	Url             string           `json:"url"`
	CallbackUrl     string           `json:"callback_url"`
	OutputLocations []OutputLocation `json:"output_locations,omitempty"`
}

type OutputLocation struct {
	Type            string          `json:"type"`
	URL             string          `json:"url,omitempty"`
	PinataAccessKey string          `json:"pinata_access_key,omitempty"`
	Outputs         *OutputsRequest `json:"outputs,omitempty"`
}

type OutputsRequest struct {
	SourceMp4          bool `json:"source_mp4"`
	SourceSegments     bool `json:"source_segments"`
	TranscodedSegments bool `json:"transcoded_segments"`
}

type CatalystOptions struct {
	BaseURL    string
	Secret     string
	OwnBaseURL *url.URL
}

type CatalystCallback = clients.TranscodeStatusCompletedMessage

type Catalyst interface {
	UploadVOD(ctx context.Context, upload UploadVODRequest) error
	CatalystHookURL(taskId, nextStep, attemptID string) string
}

func NewCatalyst(opts CatalystOptions) Catalyst {
	return &catalyst{opts, BaseClient{
		BaseUrl: opts.BaseURL,
		BaseHeaders: map[string]string{
			"Authorization": "Bearer " + opts.Secret,
		},
	}}
}

type catalyst struct {
	CatalystOptions
	BaseClient
}

func (c *catalyst) UploadVOD(ctx context.Context, upload UploadVODRequest) error {
	body, err := json.Marshal(upload)
	if err != nil {
		return err
	}
	retryDelay := rateLimitRetryInitialDelay
	for attempt := 1; ; attempt++ {
		var res json.RawMessage
		err = c.DoRequest(ctx, Request{
			Method:      "POST",
			URL:         "/api/vod",
			Body:        bytes.NewReader(body),
			ContentType: "application/json",
		}, &res)
		glog.Infof("Catalyst upload VOD request: req=%q err=%q res=%q", body, err, string(res))

		if !isTooManyRequestsErr(err) {
			return err
		}

		if attempt >= maxAttempts {
			return ErrRateLimited
		}
		select {
		case <-time.After(retryDelay):
			retryDelay = 2 * retryDelay
			continue
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Catalyst hook helpers

func (c *catalyst) CatalystHookURL(taskId, nextStep, attemptID string) string {
	// Own base URL already includes root path, so no need to add it
	hookURL := c.OwnBaseURL.JoinPath(CatalystHookPath("", taskId))
	query := hookURL.Query()
	query.Set("nextStep", nextStep)
	query.Set("attemptId", attemptID)
	hookURL.RawQuery = query.Encode()
	return hookURL.String()
}

func CatalystHookPath(apiRoot, taskId string) string {
	return path.Join(apiRoot, fmt.Sprintf("/webhook/catalyst/task/%s", taskId))
}

func isTooManyRequestsErr(err error) bool {
	var statusErr *HTTPStatusError
	return errors.As(err, &statusErr) && statusErr.Status == http.StatusTooManyRequests
}
