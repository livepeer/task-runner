package catalyst

import (
	"context"
	"fmt"

	"github.com/livepeer/go-api-client"
)

type CallbackPayload struct {
	Status          string       `json:"status"`
	CompletionRatio float64      `json:"completion_ratio"`
	Error           string       `json:"error"`
	Retriable       bool         `json:"retriable"`
	Outputs         []OutputInfo `json:"outputs"`
}

type OutputInfo struct {
	Type     string            `json:"type"`
	Manifest string            `json:"manifest"`
	Videos   map[string]string `json:"videos"`
}

type Handler struct {
	Client
	lapi api.Client
}

func NewHandler(url, authSecret string, lapi api.Client) *Handler {
	return &Handler{NewClient(url, authSecret), lapi}
}

func (h Handler) HandleCallback(ctx context.Context, taskId string, callback CallbackPayload) error {
	task, err := h.lapi.GetTask(taskId)
	if err != nil {
		return fmt.Errorf("failed to get task %s: %w", taskId, err)
	}
	if callback.Status == "error" || callback.Status == "completed" {
		// TODO: Send task continuation or abortion event
		return nil
	}
	progress := 0.95 * callback.CompletionRatio
	err = h.lapi.UpdateTaskStatus(task.ID, "running", progress)
	if err != nil {
		return fmt.Errorf("failed to update task %s status: %w", taskId, err)
	}
	return nil
}
