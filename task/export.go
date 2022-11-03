package task

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
)

var DefaultClient = clients.BaseClient{}

// TODO: Create some thumbnail from the video for the image
const livepeerLogoUrl = "ipfs://bafkreidmlgpjoxgvefhid2xjyqjnpmjjmq47yyrcm6ifvoovclty7sm4wm"

type ExportTaskConfig struct {
	PinataAccessToken  string
	PlayerImmutableURL *url.URL
	PlayerExternalURL  *url.URL
}

func TaskExport(tctx *TaskContext) (*data.TaskOutput, error) {
	var (
		ctx    = tctx.Context
		asset  = tctx.InputAsset
		size   = asset.Size
		osSess = tctx.inputOS
	)
	file, err := osSess.ReadData(ctx, videoFileName(asset.PlaybackID))
	if err != nil {
		return nil, err
	}
	defer file.Body.Close()
	if file.Size != nil && *file.Size > 0 {
		size = uint64(*file.Size)
	}

	ctx, cancel := context.WithTimeout(ctx, fileUploadTimeout)
	defer cancel()
	tctx = tctx.WithContext(ctx)

	content := NewReadCounter(file.Body)
	go ReportProgress(ctx, tctx.lapi, tctx.Task.ID, size, content.Count, 0, 1)
	output, err := uploadFile(tctx, asset, content)
	if err != nil {
		return nil, err
	}
	return &data.TaskOutput{Export: output}, nil
}

type internalMetadata struct {
	DestType string      `json:"destType"`
	Pinata   interface{} `json:"pinata"`
}

func uploadFile(tctx *TaskContext, asset *api.Asset, content io.Reader) (*data.ExportTaskOutput, error) {
	params := tctx.Task.Params.Export
	contentType := "video/" + asset.VideoSpec.Format
	if c := params.Custom; c != nil {
		req := clients.Request{
			Method:      strings.ToUpper(c.Method),
			URL:         c.URL,
			Headers:     c.Headers,
			Body:        content,
			ContentType: contentType,
		}
		if req.Method == "" {
			req.Method = "PUT"
		}
		if err := DefaultClient.DoRequest(tctx, req, nil); err != nil {
			if httpErr, ok := err.(*clients.HTTPStatusError); ok && httpErr.Status < 500 {
				err = UnretriableError{err}
			}
			return nil, fmt.Errorf("error on export request: %w", err)
		}
		return &data.ExportTaskOutput{Internal: internalMetadata{DestType: "custom"}}, nil
	} else if params.IPFS == nil {
		return nil, fmt.Errorf("missing `ipfs` or `custom` export desination params: %+v", params)
	}

	destType := "own-pinata"
	ipfs := tctx.ipfs
	if p := params.IPFS.Pinata; p != nil {
		destType = "ext-pinata"
		extMetadata := map[string]string{
			"createdBy": clients.UserAgent,
		}
		if p.JWT != "" {
			ipfs = clients.NewPinataClientJWT(p.JWT, extMetadata)
		} else {
			ipfs = clients.NewPinataClientAPIKey(p.APIKey, p.APISecret, extMetadata)
		}
	}
	videoCID, metadata, err := ipfs.PinContent(tctx, "asset-"+asset.PlaybackID, contentType, content)
	if err != nil {
		return nil, err
	}
	template, nftMetadata := params.IPFS.NFTMetadataTemplate, params.IPFS.NFTMetadata
	metadataCID, err := saveNFTMetadata(tctx, ipfs, asset, videoCID, template, nftMetadata, tctx.ExportTaskConfig)
	if err != nil {
		return nil, err
	}
	return &data.ExportTaskOutput{
		Internal: &internalMetadata{
			DestType: destType,
			Pinata:   metadata,
		},
		IPFS: &data.IPFSExportInfo{
			VideoFileCID:   videoCID,
			NFTMetadataCID: metadataCID,
		},
	}, nil
}

func saveNFTMetadata(ctx context.Context, ipfs clients.IPFS,
	asset *api.Asset, videoCID string, template api.NFTMetadataTemplate,
	overrides map[string]interface{}, config ExportTaskConfig) (string, error) {
	if template == api.NFTMetadataTemplatePlayer && asset.PlaybackRecordingID == "" {
		return "", fmt.Errorf("cannot create player NFT for asset without playback URL")
	}
	if template == "" {
		template = api.NFTMetadataTemplatePlayer
		if asset.PlaybackRecordingID == "" {
			template = api.NFTMetadataTemplateFile
		}
	}
	nftMetadata := nftMetadata(asset, videoCID, template, config)
	mergeJson(nftMetadata, overrides)

	rawMetadata, err := json.Marshal(nftMetadata)
	if err != nil {
		glog.Errorf("Error marshalling NFT metadata assetId=%s err=%q", asset.ID, err)
		return "", err
	}
	cid, _, err := ipfs.PinContent(ctx, "metadata-"+asset.PlaybackID, "application/json", bytes.NewReader(rawMetadata))
	if err != nil {
		glog.Errorf("Error saving NFT metadata assetId=%s err=%q", asset.ID, err)
		return "", err
	}
	return cid, nil
}

func nftMetadata(asset *api.Asset, videoCID string, template api.NFTMetadataTemplate, config ExportTaskConfig) map[string]interface{} {
	videoUrl := "ipfs://" + videoCID
	switch template {
	default:
		fallthrough
	case api.NFTMetadataTemplateFile:
		return map[string]interface{}{
			"name":          asset.Name,
			"description":   fmt.Sprintf("Livepeer video from asset %q", asset.Name),
			"image":         livepeerLogoUrl,
			"animation_url": videoUrl,
			"properties": map[string]interface{}{
				"video": videoUrl,
			},
		}
	case api.NFTMetadataTemplatePlayer:
		return map[string]interface{}{
			"name":          asset.Name,
			"description":   fmt.Sprintf("Livepeer video from asset %q", asset.Name),
			"image":         livepeerLogoUrl,
			"animation_url": buildPlayerUrl(config.PlayerImmutableURL, asset.PlaybackID, true),
			"external_url":  buildPlayerUrl(config.PlayerExternalURL, asset.PlaybackID, false),
			// TODO: Consider migrating these to `attributes` instead.
			"properties": map[string]interface{}{
				"video":                   videoUrl,
				"com.livepeer.playbackId": asset.PlaybackID,
			},
		}
	}
}

func buildPlayerUrl(base *url.URL, playbackID string, loop bool) string {
	url := *base
	query := url.Query()
	if loop {
		query.Set("loop", "1")
	}
	query.Set("v", playbackID)
	url.RawQuery = query.Encode()
	return url.String()
}

func mergeJson(dst, src map[string]interface{}) {
	for k, v := range src {
		if v == nil {
			delete(dst, k)
		} else if srcObj, isObj := v.(map[string]interface{}); isObj {
			if dstObj, isDstObj := dst[k].(map[string]interface{}); isDstObj {
				mergeJson(dstObj, srcObj)
			} else {
				dst[k] = v
			}
		} else {
			dst[k] = v
		}
	}
}
