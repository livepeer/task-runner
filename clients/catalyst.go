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
	ErrYieldExecution     = errors.New("yield execution")
	CatalystStatusSuccess = clients.TranscodeStatusCompleted.String()
	CatalystStatusError   = clients.TranscodeStatusError.String()
)

const rateLimitRetryAfter := 15*time.Second

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
	for ctx.Err() == nil {
		var res json.RawMessage
		err = c.DoRequest(ctx, Request{
			Method:      "POST",
			URL:         "/api/vod",
			Body:        bytes.NewReader(body),
			ContentType: "application/json",
		}, &res)
		glog.Infof("Catalyst upload VOD request: req=%q err=%q res=%q", body, err, string(res))
		var statusErr *HTTPStatusError
		if errors.As(err, &statusErr) && statusErr.Status == http.StatusTooManyRequests {
			select {
			case <-time.After(rateLimitRetryAfter):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return err
	}
	return ctx.Err()
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
