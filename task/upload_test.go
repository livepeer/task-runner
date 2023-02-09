package task

import (
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestToTranscodeFileTaskOutput(t *testing.T) {
	tests := []struct {
		name               string
		catalystApiOutputs []video.OutputVideo
		want               data.TranscodeFileTaskOutput
		hasError           bool
	}{
		{
			name: "Object Store",
			catalystApiOutputs: []video.OutputVideo{
				{
					Type:     "object_store",
					Manifest: "s3+https://user:pass@host.com/outbucket/video/catalyst/index.m3u8",
					Videos: []video.OutputVideoFile{
						{
							Type:     "m3u8",
							Location: "s3+https://user:pass@host.com/outbucket/video/catalyst/index360p0.m3u8",
						},
						{
							Type:     "m3u8",
							Location: "s3+https://user:pass@host.com/outbucket/video/catalyst/index720p0.m3u8",
						},
					},
					MP4Outputs: []video.OutputVideoFile{
						{
							Type:     "mp4",
							Location: "s3+https://user:pass@host.com/outbucket/video/catalyst/static360p0.mp4",
						},
						{
							Type:     "mp4",
							Location: "s3+https://user:pass@host.com/outbucket/video/catalyst/static720p0.mp4",
						},
					},
				},
			},
			want: data.TranscodeFileTaskOutput{
				Hls: data.TranscodeFileTaskOutputPath{
					Path: "/video/catalyst/index.m3u8",
				},
				Mp4: data.TranscodeFileTaskOutputMp4{
					Renditions: []data.TranscodeFileTaskOutputPath{
						{Path: "/video/catalyst/static360p0.mp4"},
						{Path: "/video/catalyst/static720p0.mp4"},
					},
				},
			},
		},
		{
			name: "Web3 Storage",
			catalystApiOutputs: []video.OutputVideo{
				{
					Type:     "object_store",
					Manifest: "ipfs://bafybeibn34yirlf5mv4xvaouty7gveyc6hvsgkbq6nornzr4w53r7frgvq/video/catalyst/index.m3u8",
					Videos: []video.OutputVideoFile{
						{
							Type:     "m3u8",
							Location: "ipfs://bafybeibn34yirlf5mv4xvaouty7gveyc6hvsgkbq6nornzr4w53r7frgvq/video/catalyst/index360p0.m3u8",
						},
						{
							Type:     "m3u8",
							Location: "ipfs://bafybeibn34yirlf5mv4xvaouty7gveyc6hvsgkbq6nornzr4w53r7frgvq/video/catalyst/index720p0.m3u8",
						},
					},
					MP4Outputs: []video.OutputVideoFile{
						{
							Type:     "mp4",
							Location: "ipfs://bafybeibn34yirlf5mv4xvaouty7gveyc6hvsgkbq6nornzr4w53r7frgvq/video/catalyst/static360p0.mp4",
						},
						{
							Type:     "mp4",
							Location: "ipfs://bafybeibn34yirlf5mv4xvaouty7gveyc6hvsgkbq6nornzr4w53r7frgvq/video/catalyst/static720p0.mp4",
						},
					},
				},
			},
			want: data.TranscodeFileTaskOutput{
				BaseUrl: "ipfs://bafybeibn34yirlf5mv4xvaouty7gveyc6hvsgkbq6nornzr4w53r7frgvq",
				Hls: data.TranscodeFileTaskOutputPath{
					Path: "/video/catalyst/index.m3u8",
				},
				Mp4: data.TranscodeFileTaskOutputMp4{
					Renditions: []data.TranscodeFileTaskOutputPath{
						{Path: "/video/catalyst/static360p0.mp4"},
						{Path: "/video/catalyst/static720p0.mp4"},
					},
				},
			},
		},
		{
			name:               "No output",
			catalystApiOutputs: []video.OutputVideo{},
			want:               data.TranscodeFileTaskOutput{},
			hasError:           true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toTranscodeFileTaskOutput(tt.catalystApiOutputs)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}
