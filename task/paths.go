package task

import (
	"errors"
	"net/url"
	"path"
	"strings"
	"time"
)

const (
	fileUploadTimeout = 5 * time.Minute
)

func videoFileName(playbackID string) string {
	return path.Join(playbackID, "video")
}

func hlsRootPlaylistFileName(playbackID string) string {
	return path.Join(playbackID, "index.m3u8")
}

func metadataFileName(playbackID string) string {
	return path.Join(playbackID, "video.json")
}

func extractOSUriFilePath(osUri, playbackID string) (string, error) {
	u, err := url.Parse(osUri)
	if err != nil {
		return "", err
	}
	parts := strings.SplitN(u.Path, playbackID+"/", 2)
	if len(parts) != 2 {
		return "", errors.New("no playback ID in URL path")
	}
	return "/" + parts[1], nil
}

func toAssetRelativePath(playbackID string, path string) string {
	return strings.TrimPrefix(path, playbackID+"/")
}
