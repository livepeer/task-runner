package clients

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPinataUnpin_NotPinnedByCurrentUser(t *testing.T) {
	// Simulate Pinata returning CURRENT_USER_HAS_NOT_PINNED_CID on DELETE
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"reason":"CURRENT_USER_HAS_NOT_PINNED_CID","details":"The current user has not pinned the cid: bafkrei123","message":"The current user has not pinned the cid: bafkrei123"}}`))
	}))
	defer srv.Close()

	client := &pinataClient{
		BaseClient: BaseClient{
			BaseUrl: srv.URL,
			BaseHeaders: map[string]string{
				"Authorization": "Bearer test-jwt",
			},
		},
	}

	err := client.Unpin(context.Background(), "bafkrei123")
	require.NoError(t, err, "Unpin should not return an error when CID is not pinned by current user")
}

func TestPinataUnpin_OtherError(t *testing.T) {
	// Simulate a different 400 error (not CURRENT_USER_HAS_NOT_PINNED_CID)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"reason":"SOME_OTHER_ERROR","message":"Something else went wrong"}}`))
	}))
	defer srv.Close()

	client := &pinataClient{
		BaseClient: BaseClient{
			BaseUrl: srv.URL,
			BaseHeaders: map[string]string{
				"Authorization": "Bearer test-jwt",
			},
		},
	}

	err := client.Unpin(context.Background(), "bafkrei123")
	require.Error(t, err, "Unpin should return an error for other HTTP 400 errors")
}

func TestPinataUnpin_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := &pinataClient{
		BaseClient: BaseClient{
			BaseUrl: srv.URL,
			BaseHeaders: map[string]string{
				"Authorization": "Bearer test-jwt",
			},
		},
	}

	err := client.Unpin(context.Background(), "bafkrei123")
	require.NoError(t, err, "Unpin should succeed on 200")
}
