package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/golang/glog"
)

const (
	pinataBaseUrl = "https://api.pinata.cloud"
	jsonMimeType  = "application/json"
	pinataOptions = `{"cidVersion":1}`
)

type PinInfo struct {
	ID          string `json:"id"`
	IPFSPinHash string `json:"ipfs_pin_hash"`
	Size        int64  `json:"size"`
	Metadata    struct {
		Name      string            `json:"name"`
		KeyValues map[string]string `json:"keyvalues"`
	} `json:"metadata"`
}

type PinList struct {
	Count int64     `json:"count"`
	Pins  []PinInfo `json:"rows"`
}

type IPFS interface {
	PinContent(ctx context.Context, name, contentType string, data io.Reader) (cid string, metadata interface{}, err error)
	Unpin(ctx context.Context, cid string) error
	List(ctx context.Context, pageSize, pageOffset int) (*PinList, int, error)
}

func NewPinataClientJWT(jwt string, filesMetadata map[string]string) IPFS {
	return &pinataClient{
		BaseClient: BaseClient{
			BaseUrl: pinataBaseUrl,
			BaseHeaders: map[string]string{
				"Authorization": "Bearer " + jwt,
			},
		},
		filesMetadata: marshalFilesMetadata(filesMetadata),
	}
}

func NewPinataClientAPIKey(apiKey, apiSecret string, filesMetadata map[string]string) IPFS {
	return &pinataClient{
		BaseClient: BaseClient{
			BaseUrl: pinataBaseUrl,
			BaseHeaders: map[string]string{
				"pinata_api_key":        apiKey,
				"pinata_secret_api_key": apiSecret,
			},
		},
		filesMetadata: marshalFilesMetadata(filesMetadata),
	}
}

type pinataClient struct {
	BaseClient
	filesMetadata []byte
}

type uploadResponse struct {
	IPFSHash    string    `json:"ipfsHash"`
	PinSize     int64     `json:"pinSize"`
	Timestamp   time.Time `json:"timestamp"`
	IsDuplicate bool      `json:"isDuplicate"`
}

func (p *pinataClient) PinContent(ctx context.Context, filename, fileContentType string, data io.Reader) (string, interface{}, error) {
	parts := []part{
		{"file", filename, fileContentType, data},
		{"pinataOptions", "", jsonMimeType, strings.NewReader(pinataOptions)},
	}
	if p.filesMetadata != nil {
		parts = append(parts, part{"pinataMetadata", "", jsonMimeType, bytes.NewReader(p.filesMetadata)})
	}
	body, contentType := multipartBody(parts)
	defer body.Close()

	var res *uploadResponse
	err := p.DoRequest(ctx, Request{
		Method:      "POST",
		URL:         "/pinning/pinFileToIPFS",
		Body:        body,
		ContentType: contentType,
	}, &res)
	if err != nil {
		return "", nil, err
	}
	return res.IPFSHash, res, nil
}

func (p *pinataClient) Unpin(ctx context.Context, cid string) error {
	return p.DoRequest(ctx, Request{
		Method: "DELETE",
		URL:    "/pinning/unpin/" + cid,
	}, nil)
}

func (p *pinataClient) List(ctx context.Context, pageSize, pageOffset int) (pl *PinList, next int, err error) {
	err = p.DoRequest(ctx, Request{
		Method: "GET",
		URL:    fmt.Sprintf("/data/pinList?status=pinned&pageLimit=%d&pageOffset=%d", pageSize, pageOffset),
	}, &pl)
	if err != nil {
		return nil, -1, err
	}

	next = -1
	if len(pl.Pins) >= pageSize {
		next = pageOffset + len(pl.Pins)
	}
	return pl, next, err
}

func marshalFilesMetadata(keyvalues map[string]string) []byte {
	if len(keyvalues) == 0 {
		return nil
	}
	metadata := map[string]interface{}{"keyvalues": keyvalues}
	bytes, err := json.Marshal(metadata)
	if err != nil {
		glog.Warningf("Error marshalling Piñata files metadata: err=%q", err)
		return nil
	}
	return bytes
}
