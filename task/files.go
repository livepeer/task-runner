package task

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/go-api-client/logs"
	"github.com/livepeer/go-tools/drivers"
	"github.com/livepeer/task-runner/webcrypto"
)

func getFile(ctx context.Context, osSess drivers.OSSession, cfg UploadTaskConfig, params api.UploadTaskParams, vodDecryptPrivateKey string) (name string, size uint64, content io.ReadCloser, err error) {
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

func getFileRaw(ctx context.Context, osSess drivers.OSSession, cfg UploadTaskConfig, params api.UploadTaskParams) (name string, size uint64, content io.ReadCloser, err error) {
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

func readFileToMemory(r io.Reader) (io.ReadSeekCloser, error) {
	fileInMem, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return nopCloser{bytes.NewReader(fileInMem)}, nil
}

type autodeletingFile struct {
	*os.File
}

func (adf *autodeletingFile) Reader() io.ReadSeekCloser {
	adf.Seek(0, io.SeekStart)
	return adf
}

func (adf *autodeletingFile) Close() error {
	err := adf.File.Close()
	os.Remove(adf.File.Name())
	return err
}

func getTempFile(size int64) (*os.File, error) {
	file, err := os.CreateTemp("", "transcode")
	if err != nil {
		glog.Errorf("Error creating temporary file err=%v", err)
		return nil, err
	}
	glog.Infof("Created temporary file name=%s", file.Name())
	if size > 0 {
		offset, err := file.Seek(size, io.SeekStart)
		if err != nil || offset != size {
			os.Remove(file.Name())
			glog.Errorf("Error creating temporary file name=%s with size=%d offset=%d err=%v", file.Name(), size, offset, err)
			return nil, err
		}
		file.Seek(0, io.SeekStart)
	}
	return file, nil
}

func readFile(name string, sizePtr *int64, content io.Reader) (io.ReadSeekCloser, error) {
	var size int64
	if sizePtr != nil {
		size = *sizePtr
	}
	glog.Infof("Source file name=%s size=%d", name, size)
	if size > 0 && size < maxFileSizeForMemory {
		// use memory
		return readFileToMemory(content)
	}
	if file, err := getTempFile(size); err != nil {
		return readFileToMemory(content)
	} else {
		if _, err = file.ReadFrom(content); err != nil {
			file.Close()
			os.Remove(file.Name())
			return nil, err
		}
		file.Seek(0, io.SeekStart)
		return &autodeletingFile{file}, nil
	}
}

type Accumulator struct {
	size uint64
}

func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

func (a *Accumulator) Size() uint64 {
	return atomic.LoadUint64(&a.size)
}

func (a *Accumulator) Accumulate(size uint64) {
	atomic.AddUint64(&a.size, size)
}

type nopCloser struct {
	*bytes.Reader
}

func (nopCloser) Close() error { return nil }
