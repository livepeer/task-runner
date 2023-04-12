package task

import (
	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestToTranscodeFileTaskOutput(t *testing.T) {
	tests := []struct {
		name     string
		callback *clients.CatalystCallback
		want     data.TranscodeFileTaskOutput
		hasError bool
	}{
		{
			name: "Object Store",
			callback: &clients.CatalystCallback{
				InputVideo: video.InputVideo{
					Duration:  1.23,
					SizeBytes: 4,
				},
				Outputs: []video.OutputVideo{
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
				}},
			want: data.TranscodeFileTaskOutput{
				Hls: &data.TranscodeFileTaskOutputPath{
					Path: "/video/catalyst/index.m3u8",
				},
				Mp4: []data.TranscodeFileTaskOutputPath{
					{Path: "/video/catalyst/static360p0.mp4"},
					{Path: "/video/catalyst/static720p0.mp4"},
				},
				InputVideo: &data.InputVideo{
					Duration:  1.23,
					SizeBytes: 4,
				},
			},
		},
		{
			name: "Web3 Storage",
			callback: &clients.CatalystCallback{Outputs: []video.OutputVideo{
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
			}},
			want: data.TranscodeFileTaskOutput{
				BaseUrl: "ipfs://bafybeibn34yirlf5mv4xvaouty7gveyc6hvsgkbq6nornzr4w53r7frgvq",
				Hls: &data.TranscodeFileTaskOutputPath{
					Path: "/video/catalyst/index.m3u8",
				},
				Mp4: []data.TranscodeFileTaskOutputPath{
					{Path: "/video/catalyst/static360p0.mp4"},
					{Path: "/video/catalyst/static720p0.mp4"},
				},
				InputVideo: &data.InputVideo{},
			},
		},
		{
			name: "No MP4 outputs",
			callback: &clients.CatalystCallback{Outputs: []video.OutputVideo{
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
				},
			}},
			want: data.TranscodeFileTaskOutput{
				BaseUrl: "ipfs://bafybeibn34yirlf5mv4xvaouty7gveyc6hvsgkbq6nornzr4w53r7frgvq",
				Hls: &data.TranscodeFileTaskOutputPath{
					Path: "/video/catalyst/index.m3u8",
				},
				InputVideo: &data.InputVideo{},
			},
		},
		{
			name:     "No output",
			callback: &clients.CatalystCallback{},
			want:     data.TranscodeFileTaskOutput{InputVideo: &data.InputVideo{}},
			hasError: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := toTranscodeFileTaskOutput(tt.callback)
			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestOutputLocations(t *testing.T) {
	tests := []struct {
		name                    string
		outURL                  string
		hls                     string
		hlsRelPath              string
		mp4                     string
		mp4RelPath              string
		expectedOutputLocations []clients.OutputLocation
		hasError                bool
	}{
		{
			name:       "Only HLS",
			outURL:     "s3+https://user:pass@host.com/outbucket",
			hls:        "enabled",
			hlsRelPath: "video/hls",
			mp4:        "disabled",
			mp4RelPath: "",
			expectedOutputLocations: []clients.OutputLocation{
				{
					Type: "object_store",
					URL:  "s3+https://user:pass@host.com/outbucket/video/hls",
					Outputs: &clients.OutputsRequest{
						HLS: "enabled",
					},
				},
				{
					Type: "object_store",
					URL:  "s3+https://user:pass@host.com/outbucket",
					Outputs: &clients.OutputsRequest{
						MP4: "disabled",
					},
				},
			},
		},
		{
			name:       "HLS and Short Video MP4",
			outURL:     "s3+https://user:pass@host.com/outbucket",
			hls:        "enabled",
			hlsRelPath: "video/hls",
			mp4:        "only_short",
			mp4RelPath: "video/mp4",
			expectedOutputLocations: []clients.OutputLocation{
				{
					Type: "object_store",
					URL:  "s3+https://user:pass@host.com/outbucket/video/hls",
					Outputs: &clients.OutputsRequest{
						HLS: "enabled",
					},
				},
				{
					Type: "object_store",
					URL:  "s3+https://user:pass@host.com/outbucket/video/mp4",
					Outputs: &clients.OutputsRequest{
						MP4: "only_short",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, gotOutputLocations, err := outputLocations(tt.outURL, tt.hls, tt.hlsRelPath, tt.mp4, tt.mp4RelPath)

			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedOutputLocations, gotOutputLocations)
		})
	}
}
