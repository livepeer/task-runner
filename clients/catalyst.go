package clients

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/livepeer/go-api-client"
)

type UploadVODRequest struct {
	Url             string           `json:"url"`
	CallbackUrl     string           `json:"callback_url"`
	Mp4Output       bool             `json:"mp4_output"`
	OutputLocations []OutputLocation `json:"output_locations,omitempty"`
}

type OutputLocation struct {
	Type            string `json:"type"`
	URL             string `json:"url"`
	PinataAccessKey string `json:"pinata_access_key"`
}

type CatalystCallback struct {
	Status          string         `json:"status"`
	CompletionRatio float64        `json:"completion_ratio"`
	Error           string         `json:"error"`
	Retriable       bool           `json:"retriable"`
	Outputs         []OutputInfo   `json:"outputs"`
	Spec            *api.AssetSpec `json:"spec"` // TODO: Update this to final schema
}

type OutputInfo struct {
	Type     string            `json:"type"`
	Manifest string            `json:"manifest"`
	Videos   map[string]string `json:"videos"`
}

type Catalyst interface {
	UploadVOD(ctx context.Context, upload UploadVODRequest) error
}

func NewCatalyst(url, authSecret string) Catalyst {
	return &catalyst{
		BaseClient: BaseClient{
			BaseUrl: url,
			BaseHeaders: map[string]string{
				"Authorization": "Bearer " + authSecret,
			},
		},
	}
}

type catalyst struct {
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
