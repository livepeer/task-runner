package task

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"path"
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

func TaskExport(tctx *TaskContext) (*TaskHandlerOutput, error) {
	var (
		ctx          = tctx.Context
		asset        = tctx.InputAsset
		size         = asset.Size
		osSess       = tctx.inputOS
		customParams = tctx.Params.Export.Custom
		ipfsParams   = tctx.Params.Export.IPFS
	)
	downloadFile := ""
	for _, file := range asset.Files {
		if file.Type == "static_transcoded_mp4" || file.Type == "source_file" {
			downloadFile = file.Path
			if strings.HasSuffix(asset.DownloadURL, file.Path) {
				// we've found an exact match to the download URL so exit the loop
				break
			}
		}
	}
	if downloadFile == "" {
		return nil, fmt.Errorf("unable to find download filename for asset: %s", asset.ID)
	}

	file, err := osSess.ReadData(ctx, path.Join(asset.PlaybackID, downloadFile))
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
	content := tctx.Progress.TrackReader(file.Body, size, 1)

	name := "asset-" + asset.PlaybackID
	contentType := "video/" + asset.VideoSpec.Format
	cid, internalMetadata, err := uploadFile(tctx, customParams, ipfsParams, name, content, contentType)
	if err != nil {
		return nil, err
	}
	nftCid, err := uploadNftMetadata(tctx, ipfsParams, cid)
	if err != nil {
		return nil, err
	}
	return &TaskHandlerOutput{
		TaskOutput: &data.TaskOutput{
			Export: &data.ExportTaskOutput{
				Internal: internalMetadata,
				IPFS: &data.IPFSExportInfo{
					VideoFileCID:   cid,
					NFTMetadataCID: nftCid,
				},
			},
		},
	}, nil
}

func TaskExportData(tctx *TaskContext) (*TaskHandlerOutput, error) {
	var (
		params       = tctx.Params.ExportData
		customParams = params.Custom
		ipfsParams   = params.IPFS
	)
	content := strings.NewReader(params.Content)
	name := fmt.Sprintf("%s-%s", params.Type, params.ID)
	contentType := "application/json"
	cid, _, err := uploadFile(tctx, customParams, ipfsParams, name, content, contentType)
	if err != nil {
		return nil, err
	}

	return &TaskHandlerOutput{
		TaskOutput: &data.TaskOutput{
			ExportData: &data.ExportDataTaskOutput{
				IPFS: &data.IPFSExportDataInfo{
					CID: cid,
				},
			},
		},
	}, nil
}

func uploadNftMetadata(tctx *TaskContext, ipfsParams *api.IPFSParams, cid string) (string, error) {
	var (
		params = tctx.Task.Params.Export
		asset  = tctx.InputAsset
	)
	template, nftMetadata := params.IPFS.NFTMetadataTemplate, params.IPFS.NFTMetadata
	if template == api.NFTMetadataTemplatePlayer && asset.PlaybackURL == "" {
		return "", fmt.Errorf("cannot create player NFT for asset without playback URL")
	}
	ipfs, _ := createIpfs(tctx, ipfsParams)
	return saveNFTMetadata(tctx, ipfs, asset, cid, template, nftMetadata, tctx.ExportTaskConfig)
}

type internalMetadata struct {
	DestType string      `json:"destType"`
	Pinata   interface{} `json:"pinata"`
}

func uploadFile(tctx *TaskContext, customParams *api.CustomParams, ipfsParams *api.IPFSParams, name string, content io.Reader, contentType string) (string, *internalMetadata, error) {
	if c := customParams; c != nil {
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
			return "", nil, fmt.Errorf("error on export request: %w", err)
		}
		return "", &internalMetadata{DestType: "custom"}, nil
	} else if ipfsParams == nil {
		return "", nil, fmt.Errorf("missing `ipfs` or `custom` export desination params: custom=%+v, ipfs=%+v", customParams, ipfsParams)
	}

	ipfs, destType := createIpfs(tctx, ipfsParams)
	cid, metadata, err := ipfs.PinContent(tctx, name, contentType, content)
	if err != nil {
		return "", nil, err
	}

	return cid, &internalMetadata{
		DestType: destType,
		Pinata:   metadata,
	}, nil
}

func createIpfs(tctx *TaskContext, ipfsParams *api.IPFSParams) (clients.IPFS, string) {
	ipfs := tctx.ipfs
	destType := "own-pinata"
	if p := ipfsParams.Pinata; p != nil {
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
	return ipfs, destType
}

func saveNFTMetadata(ctx context.Context, ipfs clients.IPFS,
	asset *api.Asset, videoCID string, template api.NFTMetadataTemplate,
	overrides map[string]interface{}, config ExportTaskConfig) (string, error) {
	if template == "" {
		template = api.NFTMetadataTemplateFile
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
		}
	case api.NFTMetadataTemplatePlayer:
		return map[string]interface{}{
			"name":          asset.Name,
			"description":   fmt.Sprintf("Livepeer video from asset %q", asset.Name),
			"image":         livepeerLogoUrl,
			"animation_url": buildPlayerUrl(config.PlayerImmutableURL, videoCID, true),
			"external_url":  buildPlayerUrl(config.PlayerExternalURL, videoCID, false),
			// TODO: Consider migrating these to `attributes` instead. ref: https://github.com/livepeer/task-runner/pull/82#discussion_r1013538456
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
