package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path"

	"github.com/livepeer/go-api-client"
)

type UploadVODRequest struct {
	Url             string           `json:"url"`
	CallbackUrl     string           `json:"callback_url"`
	Mp4Output       bool             `json:"mp4_output"`
	OutputLocations []OutputLocation `json:"output_locations,omitempty"`
}

type OutputLocation struct {
	Type            string          `json:"type"`
	URL             string          `json:"url"`
	PinataAccessKey string          `json:"pinata_access_key"`
	Outputs         *OutputsRequest `json:"outputs,omitempty"`
}

type OutputsRequest struct {
	SourceMp4          bool `json:"source_mp4"`
	SourceSegments     bool `json:"source_segments"`
	TranscodedSegments bool `json:"transcoded_segments"`
}

type CatalystCallback struct {
	Status          string         `json:"status"`
	CompletionRatio float64        `json:"completion_ratio"`
	Error           string         `json:"error"`
	Unretriable     bool           `json:"unretriable"`
	Outputs         []OutputInfo   `json:"outputs"`
	Spec            *api.AssetSpec `json:"spec"` // TODO: Update this to final schema
}

type OutputInfo struct {
	Type     string            `json:"type"`
	Manifest string            `json:"manifest"`
	Videos   map[string]string `json:"videos"`
}

type CatalystOptions struct {
	BaseURL    string
	Secret     string
	OwnBaseURL *url.URL
}

type Catalyst interface {
	UploadVOD(ctx context.Context, upload UploadVODRequest) error
	CatalystHookURL(taskId, nextStep string) string
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
	return c.DoRequest(ctx, Request{
		Method:      "POST",
		URL:         "/api/vod",
		Body:        bytes.NewReader(body),
		ContentType: "application/json",
	}, nil)
}

// Catalyst hook helpers

func (c *catalyst) CatalystHookURL(taskId, nextStep string) string {
	// Own base URL already includes root path, so no need to add it
	path := fmt.Sprintf("%s?nextStep=%s", CatalystHookPath("", taskId), nextStep)
	hookURL := c.OwnBaseURL.JoinPath(path)
	return hookURL.String()
}

func CatalystHookPath(apiRoot, taskId string) string {
	return path.Join(apiRoot, fmt.Sprintf("/webhook/catalyst/task/%s", taskId))
}
