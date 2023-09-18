package task

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/golang/glog"
	api "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-api-client/logs"
	"github.com/livepeer/go-tools/drivers"
	"github.com/livepeer/task-runner/webcrypto"
)

const IPFS_PREFIX = "ipfs://"
const ARWEAVE_PREFIX = "ar://"

type ImportTaskConfig struct {
	// Ordered list of IPFS gateways (includes /ipfs/ suffix) to import assets from
	ImportIPFSGatewayURLs []*url.URL
}

func getFile(ctx context.Context, osSess drivers.OSSession, cfg ImportTaskConfig, params api.UploadTaskParams, vodDecryptPrivateKey string) (name string, size uint64, content io.ReadCloser, err error) {
	name, size, content, err = getFileRaw(ctx, osSess, cfg, params)
	if err != nil || !isEncryptionEnabled(params) {
		return
	}

	decryptPrivateKey, err := webcrypto.LoadPrivateKey(vodDecryptPrivateKey)
	if err != nil {
		return "", 0, nil, fmt.Errorf("failed to load private key: %w", err)
	}
	glog.V(logs.VVERBOSE).Infof("Decrypting file with key file=%s keyHash=%x", params.URL, sha256.Sum256([]byte(params.Encryption.EncryptedKey)))
	decrypted, err := webcrypto.DecryptAESCBC(content, decryptPrivateKey, params.Encryption.EncryptedKey)
	if err != nil {
		content.Close()
		return "", 0, nil, fmt.Errorf("failed to decrypt input file: %w", err)
	}

	glog.V(logs.VVERBOSE).Infof("Returning decrypted stream for file=%s", params.URL)
	return name, size, decrypted, nil
}

func getFileRaw(ctx context.Context, osSess drivers.OSSession, cfg ImportTaskConfig, params api.UploadTaskParams) (name string, size uint64, content io.ReadCloser, err error) {
	if upedObjKey := params.UploadedObjectKey; upedObjKey != "" {
		// TODO: We should simply "move" the file in case of direct import since we
		// know the file is already in the object store. Independently, we also have
		// to delete the uploaded file after copying to the new location.
		fileInfo, err := osSess.ReadData(ctx, upedObjKey)
		if err != nil {
			return "", 0, nil, UnretriableError{fmt.Errorf("error reading direct uploaded file: %w", err)}
		}
		if fileInfo.Size != nil && *fileInfo.Size > 0 {
			size = uint64(*fileInfo.Size)
		}
		return fileInfo.FileInfo.Name, size, fileInfo.Body, nil
	} else if params.URL == "" {
		return "", 0, nil, fmt.Errorf("no import URL or direct upload object key: %+v", params)
	}

	if strings.HasPrefix(params.URL, IPFS_PREFIX) {
		cid := strings.TrimPrefix(params.URL, IPFS_PREFIX)
		return getFileIPFS(ctx, cfg.ImportIPFSGatewayURLs, cid)
	}

	if strings.HasPrefix(params.URL, ARWEAVE_PREFIX) {
		txID := strings.TrimPrefix(params.URL, ARWEAVE_PREFIX)
		// arweave.net is the main gateway for Arweave right now
		// In the future, given more gateways, we can pass a list of gateway URLs similar to what we do for IPFS
		gatewayUrl := "https://arweave.net/" + txID
		return getFileWithUrl(ctx, gatewayUrl)
	}

	return getFileWithUrl(ctx, params.URL)
}

func getFileIPFS(ctx context.Context, gateways []*url.URL, cid string) (name string, size uint64, content io.ReadCloser, err error) {
	for _, gateway := range gateways {
		url := gateway.JoinPath(cid).String()
		name, size, content, err = getFileWithUrl(ctx, url)
		if err == nil {
			return name, size, content, nil
		}
		glog.Infof("Failed to get file from IPFS cid=%v url=%v err=%v", cid, gateway, err)
	}

	return "", 0, nil, err
}

func getFileWithUrl(ctx context.Context, url string) (name string, size uint64, content io.ReadCloser, err error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", 0, nil, UnretriableError{fmt.Errorf("error creating http request: %w", err)}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, nil, fmt.Errorf("error on import request: %w", err)
	}
	if resp.StatusCode >= 300 {
		resp.Body.Close()
		err := fmt.Errorf("bad status code from import request: %d %s", resp.StatusCode, resp.Status)
		if resp.StatusCode < 500 {
			err = UnretriableError{err}
		}
		return "", 0, nil, err
	}
	if resp.ContentLength > 0 {
		size = uint64(resp.ContentLength)
	}
	return filename(req, resp), size, resp.Body, nil
}

func filename(req *http.Request, resp *http.Response) string {
	contentDisposition := resp.Header.Get("Content-Disposition")
	_, params, _ := mime.ParseMediaType(contentDisposition)
	if filename, ok := params["filename"]; ok {
		return filename
	}
	if base := path.Base(req.URL.Path); len(base) > 1 {
		return base
	}
	return ""
}
