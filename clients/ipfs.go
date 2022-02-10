package clients

import (
	"context"
	"io"
	"time"
)

const (
	pinataBaseUrl = "https://api.pinata.cloud"
)

type IPFS interface {
	PinContent(ctx context.Context, name, contentType string, data io.Reader) (cid string, metadata interface{}, err error)
	Unpin(ctx context.Context, cid string) error
}

func NewPinataClientJWT(jwt string) IPFS {
	return &pinataClient{
		BaseClient: BaseClient{
			BaseUrl: pinataBaseUrl,
			BaseHeaders: map[string]string{
				"Authorization": "Bearer " + jwt,
			},
		},
	}
}

func NewPinataClientAPIKey(apiKey, apiSecret string) IPFS {
	return &pinataClient{
		BaseClient: BaseClient{
			BaseUrl: pinataBaseUrl,
			BaseHeaders: map[string]string{
				"pinata_api_key":        apiKey,
				"pinata_secret_api_key": apiSecret,
			},
		},
	}
}

type pinataClient struct {
	BaseClient
}

type uploadResponse struct {
	IPFSHash    string    `json:"ipfsHash"`
	PinSize     int64     `json:"pinSize"`
	Timestamp   time.Time `json:"timestamp"`
	IsDuplicate bool      `json:"isDuplicate"`
}

func (p *pinataClient) PinContent(ctx context.Context, filename, fileContentType string, data io.Reader) (string, interface{}, error) {
	body, contentType := multipartBody([]part{
		{"file", filename, fileContentType, data},
	})
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