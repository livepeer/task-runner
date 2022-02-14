package task

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/golang/glog"
	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
)

var DefaultClient = clients.BaseClient{}

const livepeerLogoUrl = "ipfs://bafkreidmlgpjoxgvefhid2xjyqjnpmjjmq47yyrcm6ifvoovclty7sm4wm"

func TaskExport(tctx *TaskContext) (*data.TaskOutput, error) {
	var (
		ctx    = tctx.Context
		asset  = tctx.InputAsset
		size   = asset.Size
		osSess = tctx.inputOS
		params = *tctx.Task.Params.Export
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
	content := NewReadCounter(file.Body)
	go ReportProgress(ctx, tctx.lapi, tctx.Task.ID, size, content.Count)
	output, err := uploadFile(ctx, tctx.ipfs, params, asset, content)
	if err != nil {
		return nil, err
	}
	return &data.TaskOutput{Export: output}, nil
}

type internalMetadata struct {
	DestType string      `json:"destType"`
	Pinata   interface{} `json:"pinata"`
}

func uploadFile(ctx context.Context, ipfs clients.IPFS, params livepeerAPI.ExportTaskParams, asset *livepeerAPI.Asset, content io.Reader) (*data.ExportTaskOutput, error) {
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
		if err := DefaultClient.DoRequest(ctx, req, nil); err != nil {
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
	videoCID, metadata, err := ipfs.PinContent(ctx, "asset-"+asset.PlaybackID, contentType, content)
	if err != nil {
		return nil, err
	}
	// This one is a nice to have so we don't return an error. If it fails we just
	// ignore and don't return the metadata CID.
	metadataCID := saveNFTMetadata(ctx, ipfs, asset, videoCID, params.IPFS.NFTMetadata)
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

func saveNFTMetadata(ctx context.Context, ipfs clients.IPFS, asset *livepeerAPI.Asset, videoCID string, customMetadata map[string]interface{}) string {
	nftMetadata := nftMetadata(asset, videoCID, customMetadata)
	rawMetadata, err := json.Marshal(nftMetadata)
	if err != nil {
		glog.Errorf("Error marshalling NFT metadata assetId=%s err=%q", asset.ID, err)
		return ""
	}
	cid, _, err := ipfs.PinContent(ctx, "metadata-"+asset.PlaybackID, "application/json", bytes.NewReader(rawMetadata))
	if err != nil {
		glog.Errorf("Error saving NFT metadata assetId=%s err=%q", asset.ID, err)
		return ""
	}
	return cid
}

func nftMetadata(asset *livepeerAPI.Asset, videoCID string, customMetadata map[string]interface{}) map[string]interface{} {
	videoUrl := "ipfs://" + videoCID
	metadata := map[string]interface{}{
		"name":        asset.Name,
		"description": fmt.Sprintf("Livepeer video from asset %q", asset.Name),
		// TODO: Create some thumbnail from the video for the image
		"image":         livepeerLogoUrl,
		"animation_url": videoUrl,
		"properties": map[string]interface{}{
			"video": videoUrl,
		},
	}
	mergeJson(metadata, customMetadata)
	return metadata
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
