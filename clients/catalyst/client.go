package catalyst

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/livepeer/task-runner/clients"
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

type Client interface {
	UploadVOD(ctx context.Context, upload UploadVODRequest) error
}

func NewClient(url, authSecret string) Client {
	return &client{
		BaseClient: clients.BaseClient{
			BaseUrl: url,
			BaseHeaders: map[string]string{
				"Authorization": "Bearer " + authSecret,
			},
		},
	}
}

type client struct {
	clients.BaseClient
}

func (c *client) UploadVOD(ctx context.Context, upload UploadVODRequest) error {
	body, err := json.Marshal(upload)
	if err != nil {
		return err
	}
	return c.DoRequest(ctx, clients.Request{
		Method:      "POST",
		URL:         "/api/vod",
		Body:        bytes.NewReader(body),
		ContentType: "application/json",
	}, nil)
}
