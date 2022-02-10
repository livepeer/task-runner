package clients

import (
	"context"
	"io"
	"net/http"
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
		baseClient: baseClient{
			baseUrl: pinataBaseUrl,
			baseHeaders: http.Header{
				"Authorization": []string{"Bearer " + jwt},
			},
		},
	}
}

func NewPinataClientAPIKey(apiKey, apiSecret string) IPFS {
	return &pinataClient{
		baseClient: baseClient{
			baseUrl: pinataBaseUrl,
			baseHeaders: http.Header{
				"pinata_api_key":        []string{apiKey},
				"pinata_secret_api_key": []string{apiSecret},
			},
		},
	}
}

type pinataClient struct {
	baseClient
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
	err := p.doRequest(ctx, request{
		method:      "POST",
		url:         "/pinning/pinFileToIPFS",
		body:        body,
		contentType: contentType,
	}, &res)
	if err != nil {
		return "", nil, err
	}
	return res.IPFSHash, res, nil
}

func (p *pinataClient) Unpin(ctx context.Context, cid string) error {
	return p.doRequest(ctx, request{
		method: "DELETE",
		url:    "/pinning/unpin/" + cid,
	}, nil)
}
