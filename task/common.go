package task

import (
	"path"
	"time"
)

const (
	fileUploadTimeout = 5 * time.Minute
)

func videoFileName(playbackID string) string {
	return path.Join(playbackID, "video")
}

func metadataFileName(playbackID string) string {
	return path.Join(playbackID, "video.json")
}
