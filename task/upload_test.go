package task

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/livepeer/catalyst-api/video"
	"github.com/livepeer/go-api-client"
	"github.com/livepeer/livepeer-data/pkg/data"
	"github.com/livepeer/task-runner/clients"
	"github.com/stretchr/testify/require"
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
		thumbs                  string
		thumbsRelPath           string
		sourceCopy              bool
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
		{
			name:       "HLS and Short Video MP4",
			outURL:     "s3+https://user:pass@host.com/outbucket",
			hls:        "enabled",
			hlsRelPath: "video/hls",
			mp4:        "only_short",
			mp4RelPath: "video/mp4",
			sourceCopy: true,
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
				{
					Type: "object_store",
					URL:  "s3+https://user:pass@host.com/outbucket/video/hls/video",
					Outputs: &clients.OutputsRequest{
						SourceMp4: true,
					},
				},
			},
		},
		{
			name:          "Thumbnails",
			outURL:        "s3+https://user:pass@host.com/outbucket",
			hls:           "enabled",
			hlsRelPath:    "video/hls",
			thumbs:        "enabled",
			thumbsRelPath: "video/thumbs",
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
						MP4: "",
					},
				},
				{
					Type: "object_store",
					URL:  "s3+https://user:pass@host.com/outbucket/video/thumbs",
					Outputs: &clients.OutputsRequest{
						Thumbnails: "enabled",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, gotOutputLocations, err := outputLocations(tt.outURL, out(tt.hls, tt.hlsRelPath), out(tt.mp4, tt.mp4RelPath), output{}, out(tt.thumbs, tt.thumbsRelPath), tt.sourceCopy)

			if tt.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.expectedOutputLocations, gotOutputLocations)
		})
	}
}

func TestIsHLSFile(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"https+s3://access-key:secret-key@bucket/some/path/file.m3u8", true},
		{"https+s3://access-key:secret-key@bucket/some/path/file.m3u8?foo", true},
		{"file.m3u8", true},
		{"file.mp4", false},
		{"file", false},
		{".m3u8", true},
		{"file.m3u", false},
		{"file.m3u8.txt", false},
	}

	for _, test := range tests {
		got := isHLSFile(test.input)
		if got != test.want {
			t.Errorf("isHLSFile(%q) = %v, want %v", test.input, got, test.want)
		}
	}
}

func Test_handleUploadVOD(t *testing.T) {
	tests := []struct {
		name     string
		args     handleUploadVODParams
		manifest string
		want     *TaskHandlerOutput
		wantErr  string
	}{
		{
			name: "happy",
			args: handleUploadVODParams{
				tctx: &TaskContext{
					Context: context.Background(),
					TaskInfo: data.TaskInfo{
						Step: "resultPartial",
					},
					OutputAsset: &api.Asset{
						PlaybackID: "playbackID",
					},
					Task: &api.Task{
						Type: "upload",
					},
				},
			},
			manifest: "/hls/playbackID/master.m3u8",
			want: &TaskHandlerOutput{
				TaskOutput: &data.TaskOutput{
					Upload: &data.UploadTaskOutput{
						AssetSpec: api.AssetSpec{Files: []api.AssetFile{
							{
								Type: "catalyst_hls_manifest",
								Path: "master.m3u8",
							},
						}},
					},
				},
				Continue: true,
			},
		},
		{
			name: "happy transcode api",
			args: handleUploadVODParams{
				tctx: &TaskContext{
					Context: context.Background(),
					TaskInfo: data.TaskInfo{
						Step: "resultPartial",
					},
					OutputAsset: &api.Asset{
						PlaybackID: "playbackID",
					},
					Task: &api.Task{
						Type: "transcode-file",
					},
				},
			},
			manifest: "/hls/playbackID/master.m3u8",
			want: &TaskHandlerOutput{
				Continue: true,
			},
		},
		{
			name: "parsing error",
			args: handleUploadVODParams{
				tctx: &TaskContext{
					Context: context.Background(),
					TaskInfo: data.TaskInfo{
						Step:      "resultPartial",
						StepInput: []byte{},
					},
				},
			},
			wantErr: "error parsing step input: unexpected end of JSON input",
		},
		{
			name: "url extract error",
			args: handleUploadVODParams{
				tctx: &TaskContext{
					Context: context.Background(),
					TaskInfo: data.TaskInfo{
						Step: "resultPartial",
					},
					OutputAsset: &api.Asset{
						PlaybackID: "playbackID",
					},
				},
			},
			manifest: "foo",
			wantErr:  "error extracting file path from output manifest: no playback ID in URL path",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.manifest != "" {
				input := &video.OutputVideo{Manifest: tt.manifest}
				inputBytes, err := json.Marshal(input)
				require.NoError(t, err)
				tt.args.tctx.StepInput = inputBytes
			}

			got, err := handleUploadVOD(tt.args)
			if tt.wantErr != "" {
				require.EqualError(t, err, tt.wantErr)
			}
			require.Equal(t, tt.want, got)
		})
	}
}
