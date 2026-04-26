package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/mahavrilla/sf-lazarus/internal/restore"
	"github.com/mahavrilla/sf-lazarus/internal/salesforce"
)

// Upserter submits a Bulk API 2.0 upsert job and returns its job ID.
type Upserter interface {
	BulkUpsert(ctx context.Context, object string, records []salesforce.Record, log *slog.Logger) (string, error)
	GetJobStatus(ctx context.Context, jobID string) (salesforce.JobStatus, error)
}

// Handler holds the dependencies for the restore HTTP API.
type Handler struct {
	restore  *restore.Service
	sf       Upserter
	log      *slog.Logger
}

// NewHandler constructs a Handler.
func NewHandler(svc *restore.Service, sf Upserter, log *slog.Logger) *Handler {
	return &Handler{restore: svc, sf: sf, log: log}
}

// previewRequest is the shared request body for preview and execute.
type previewRequest struct {
	ObjectType string `json:"object_type"`
	RecordID   string `json:"record_id"`
	Before     string `json:"before"` // RFC3339
}

// Preview handles POST /restore/preview.
// It returns the last-known-good record(s) without writing anything.
func (h *Handler) Preview(w http.ResponseWriter, r *http.Request) {
	var req previewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	opts, err := parseOptions(req)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	records, err := h.restore.LastGoodState(r.Context(), opts)
	if err != nil {
		h.log.Error("preview query failed", slog.String("object", req.ObjectType), slog.Any("error", err))
		jsonError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	jsonOK(w, map[string]any{"records": records})
}

// Execute handles POST /restore/execute.
// It retrieves last-known-good records and submits a Bulk API upsert job.
func (h *Handler) Execute(w http.ResponseWriter, r *http.Request) {
	var req previewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	opts, err := parseOptions(req)
	if err != nil {
		jsonError(w, err.Error(), http.StatusBadRequest)
		return
	}

	rows, err := h.restore.LastGoodState(r.Context(), opts)
	if err != nil {
		h.log.Error("execute query failed", slog.String("object", req.ObjectType), slog.Any("error", err))
		jsonError(w, "query failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if len(rows) == 0 {
		jsonError(w, "no records found before the given timestamp", http.StatusNotFound)
		return
	}

	records := make([]salesforce.Record, len(rows))
	for i, row := range rows {
		records[i] = salesforce.Record(row)
	}

	jobID, err := h.sf.BulkUpsert(r.Context(), opts.ObjectType, records, h.log)
	if err != nil {
		h.log.Error("bulk upsert failed", slog.String("object", req.ObjectType), slog.Any("error", err))
		jsonError(w, "upsert failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	h.log.Info("restore job submitted",
		slog.String("object", opts.ObjectType),
		slog.String("job_id", jobID),
		slog.Int("records", len(records)),
	)

	jsonOK(w, map[string]any{"job_id": jobID})
}

// Status handles GET /restore/status/{id}.
func (h *Handler) Status(w http.ResponseWriter, r *http.Request) {
	jobID := strings.TrimPrefix(r.URL.Path, "/restore/status/")
	if jobID == "" {
		jsonError(w, "job ID is required", http.StatusBadRequest)
		return
	}

	status, err := h.sf.GetJobStatus(r.Context(), jobID)
	if err != nil {
		h.log.Error("get job status failed", slog.String("job_id", jobID), slog.Any("error", err))
		jsonError(w, "get status failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	jsonOK(w, map[string]any{
		"status":            status.Status,
		"records_processed": status.RecordsProcessed,
		"errors":            status.Errors,
	})
}

func parseOptions(req previewRequest) (restore.Options, error) {
	if req.ObjectType == "" {
		return restore.Options{}, fmt.Errorf("object_type is required")
	}
	if req.Before == "" {
		return restore.Options{}, fmt.Errorf("before is required")
	}
	before, err := time.Parse(time.RFC3339, req.Before)
	if err != nil {
		return restore.Options{}, fmt.Errorf("before must be RFC3339: %w", err)
	}
	return restore.Options{
		ObjectType: req.ObjectType,
		RecordID:   req.RecordID,
		Before:     before,
	}, nil
}

func jsonOK(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func jsonError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck
}
