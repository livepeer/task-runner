package clients

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	ownURL, _     = url.Parse("https://call.me/maybe")
	baseUploadReq = UploadVODRequest{
		Url: "https://example.com/video.mp4",
		OutputLocations: []OutputLocation{
			{
				Type: "object_store",
				URL:  "s3+https://im.not/gonna/write/this",
			},
		},
	}
)

func TestCatalystHookPath(t *testing.T) {
	require := require.New(t)
	c := NewCatalyst(CatalystOptions{OwnBaseURL: ownURL})
	url := c.CatalystHookURL("1234", "nothing", "first try")
	require.Equal("https://call.me/maybe/webhook/catalyst/task/1234?attemptId=first+try&nextStep=nothing", url)
}

func TestDefaultRateLimitBackoff(t *testing.T) {
	require := require.New(t)
	c := NewCatalyst(CatalystOptions{})
	actualBackoff := c.(*catalyst).CatalystOptions.RateLimitInitialBackoff
	require.NotEqual(0, actualBackoff)
	require.Equal(defaultRateLimitInitialBackoff, actualBackoff)
}

func TestCatalystRateLimiting(t *testing.T) {
	require := require.New(t)
	payload := baseUploadReq
	callNum := 0
	rateLimitUntilCallNum := math.MaxInt
	catalystMock := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		callNum++
		defer r.Body.Close()
		require.Equal("POST", r.Method)
		require.Equal("/api/vod", r.URL.Path)
		require.Equal("Bearer SuperSecret", r.Header.Get("Authorization"))

		var actualPayload UploadVODRequest
		err := json.NewDecoder(r.Body).Decode(&actualPayload)
		require.NoError(err)
		require.Equal(payload, actualPayload)

		if callNum <= rateLimitUntilCallNum {
			rw.WriteHeader(http.StatusTooManyRequests)
		} else {
			rw.WriteHeader(http.StatusOK)
			_, err = rw.Write([]byte(`"good luck out there"`))
			require.NoError(err)
		}
	}))
	defer catalystMock.Close()

	c := NewCatalyst(CatalystOptions{
		BaseURL:                 catalystMock.URL,
		Secret:                  "SuperSecret",
		OwnBaseURL:              ownURL,
		RateLimitInitialBackoff: 50 * time.Millisecond, // 4 retries, total 350ms sleep time
	})
	payload.CallbackUrl = c.CatalystHookURL("1234", "nothing", "first try")

	// check it returns rate limited error if the context expires
	ctx, _ := context.WithTimeout(context.Background(), 25*time.Millisecond)
	err := c.UploadVOD(ctx, payload)
	require.Error(ctx.Err())
	require.ErrorIs(err, ErrRateLimited)
	require.Equal(1, callNum)

	// check it returns rate limited error if the max attempts (4) are reached
	callNum = 0
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	err = c.UploadVOD(ctx, payload)
	require.NoError(ctx.Err())
	require.ErrorIs(err, ErrRateLimited)
	require.Equal(4, callNum)

	// check it works when the server returns 200 even on the last attempt
	callNum, rateLimitUntilCallNum = 0, 3
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Second)
	err = c.UploadVOD(ctx, payload)
	require.NoError(ctx.Err())
	require.NoError(err)
	require.Equal(4, callNum)

	// check it doesn't retry on successes
	callNum, rateLimitUntilCallNum = 0, 0
	ctx, _ = context.WithTimeout(context.Background(), 25*time.Millisecond)
	err = c.UploadVOD(ctx, payload)
	require.NoError(ctx.Err())
	require.NoError(err)
	require.Equal(1, callNum)
}

func TestWithoutCredentials(t *testing.T) {
	require.Equal(t,
		`{"url":"s3+https://jv4s7zwfugeb7uccnnl2bwigikka:xxxxx@gateway.storjshare.io/inbucket/source.mp4","callback_url":"","output_locations":[{"type":"object_store","url":"s3+https://jv4s7zwfugeb7uccnnl2bwigikka:xxxxx@gateway.storjshare.io/outbucket/sourcetest/hls/index.m3u8"}],"clip_strategy":{}}`,
		withoutCredentials(UploadVODRequest{
			Url: "s3+https://jv4s7zwfugeb7uccnnl2bwigikka:j3axkol3vqndxy4vs6mgmv4tzs47kaxazj3uesegybny2q7n74jwq@gateway.storjshare.io/inbucket/source.mp4",
			OutputLocations: []OutputLocation{
				{
					Type: "object_store",
					URL:  "s3+https://jv4s7zwfugeb7uccnnl2bwigikka:j3axkol3vqndxy4vs6mgmv4tzs47kaxazj3uesegybny2q7n74jwq@gateway.storjshare.io/outbucket/sourcetest/hls/index.m3u8",
				},
			},
		}))
}
