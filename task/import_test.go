package task

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	livepeerAPI "github.com/livepeer/go-api-client"
	"github.com/livepeer/go-livepeer/drivers"
	"github.com/stretchr/testify/assert"
)

const (
	host            = "s3.filebase.com"
	endpoint        = "https://" + host
	bucket          = "test-victor"
	region          = "us-east-1"
	accessKey       = "REDACTED"
	accessKeySecret = "REDACTED"

	osPath = "s3+https://" + accessKey + ":" + accessKeySecret + "@" + host + "/" + region + "/" + bucket
)

func TestImport(t *testing.T) {
	assert := assert.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	url := "https://eric-test-livepeer.s3.amazonaws.com/bbbx3_720.mp4"

	var task *livepeerAPI.Task
	err := json.Unmarshal([]byte(`{"params":{"import":{"url":"`+url+`"}}}`), &task)
	assert.NoError(err)
	os, err := drivers.ParseOSURL(osPath, true)
	assert.NoError(err)

	result, err := TaskImport(&TaskContext{
		Context:     ctx,
		Task:        task,
		OutputAsset: &livepeerAPI.Asset{PlaybackID: "test-playback-id"},
		outputOS:    os.NewSession("test_import_bbb"),
	})
	assert.NoError(err)
	fmt.Println(json.MarshalIndent(result, "", "  "))
}
