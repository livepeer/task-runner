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

func hlsRootPlaylistFileName(playbackID string) string {
	return path.Join(playbackID, "hls", "index.m3u8")
}

func metadataFileName(playbackID string) string {
	return path.Join(playbackID, "video.json")
}
