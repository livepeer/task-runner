package task

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"golang.org/x/sync/errgroup"
)

func TaskDelete(tctx *TaskContext) (*TaskHandlerOutput, error) {

	var (
		ctx    = tctx.Context
		asset  = tctx.InputAsset
		osSess = tctx.outputOS
	)

	directory := asset.PlaybackID

	files, err := osSess.ListFiles(ctx, directory, "/")

	if err != nil {
		glog.Errorf("Error listing files in directory %v", directory)
	}

	totalFiles := files.Files()

	accumulator := NewAccumulator()
	tctx.Progress.TrackCount(accumulator.Size, uint64(len(totalFiles)), 0)

	const maxRetries = 3
	const retryInterval = time.Second

	eg := errgroup.Group{}

	for _, file := range totalFiles {
		filename := file.Name
		eg.Go(func() error {
			var err error
			for i := 0; i < maxRetries; i++ {
				err = osSess.DeleteFile(ctx, filename)
				if err == nil {
					accumulator.Accumulate(1)
					return nil
				}
				glog.Errorf("Error deleting file %v: %v (retrying...)", filename, err)
				time.Sleep(retryInterval)
			}
			return fmt.Errorf("failed to delete file %v after %d retries", filename, maxRetries)
		})
	}

	if err := eg.Wait(); err != nil {
		glog.Errorf("Error deleting files: %v", err)
	}

	if ipfs := asset.AssetSpec.Storage.IPFS; ipfs != nil {
		err = UnpinFromIpfs(*tctx, ipfs.CID, "cid")
		if err != nil {
			glog.Errorf("Error unpinning from IPFS %v", ipfs.CID)
		}
		err = UnpinFromIpfs(*tctx, ipfs.NFTMetadata.CID, "nftMetadataCid")
		if err != nil {
			glog.Errorf("Error unpinning metadata from IPFS %v", ipfs.NFTMetadata.CID)
		}
	}

	return &TaskHandlerOutput{
		TaskOutput: &data.TaskOutput{},
	}, nil
}

func UnpinFromIpfs(tctx TaskContext, cid string, filter string) error {
	assets, _, err := tctx.lapi.ListAssets(api.ListOptions{
		Filters: map[string]interface{}{
			filter: cid,
		},
		AllUsers: true,
	})

	if err != nil {
		return err
	}

	if len(assets) == 1 {
		return tctx.ipfs.Unpin(tctx.Context, cid)
	}

	return nil
}
