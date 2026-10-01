package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

// Record statuses reported by the API.
const (
	statusStored = "stored"
)

const (
	defaultListLimit = 20
	maxListLimit     = 100
)

const timestampFormat = time.RFC3339Nano

// recordResponse is the stable view of one record. Field order is part of the
// published contract.
type recordResponse struct {
	RecordID           string          `json:"record_id"`
	Status             string          `json:"status"`
	IdempotencyKey     string          `json:"idempotency_key"`
	RequestFingerprint string          `json:"request_fingerprint"`
	ResponseSnapshot   json.RawMessage `json:"response_snapshot"`
	CreatedAt          string          `json:"created_at"`
	ExpiresAt          string          `json:"expires_at"`
}

type recordEnvelope struct {
	Record recordResponse `json:"record"`
}

type listRecordsResponse struct {
	Records    []recordResponse `json:"records"`
	NextCursor string           `json:"next_cursor"`
}

type errorEnvelope struct {
	Error any `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type fingerprintConflictBody struct {
	Code                       string `json:"code"`
	Message                    string `json:"message"`
	ExistingRecordID           string `json:"existing_record_id"`
	ExistingRequestFingerprint string `json:"existing_request_fingerprint"`
}

type submitRecordRequest struct {
	IdempotencyKey     string          `json:"idempotency_key"`
	RequestFingerprint string          `json:"request_fingerprint"`
	ResponseSnapshot   json.RawMessage `json:"response_snapshot"`
	ExpiresAt          string          `json:"expires_at"`
}

func newRecordResponse(rec *store.Record) recordResponse {
	return recordResponse{
		RecordID:           rec.ID,
		Status:             statusStored,
		IdempotencyKey:     rec.IdempotencyKey,
		RequestFingerprint: rec.RequestFingerprint,
		ResponseSnapshot:   rec.ResponseSnapshot,
		CreatedAt:          rec.CreatedAt.Format(timestampFormat),
		ExpiresAt:          rec.ExpiresAt.Format(timestampFormat),
	}
}

func registerIdempotencyRoutes(router *gin.Engine, st *store.Store) {
	v1 := router.Group("/v1/idempotency")
	v1.POST("/records", submitRecord(st))
	v1.GET("/records", listRecords(st))
	v1.GET("/records/:key", getRecord(st))
}

// submitRecord stores the first response snapshot for an idempotency key and
// replays it for matching repeat submissions.
func submitRecord(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		var request submitRecordRequest
		decoder := json.NewDecoder(c.Request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			respondInvalid(c, "request body must be a valid idempotency record JSON object")
			return
		}
		if decoder.More() {
			respondInvalid(c, "request body must contain a single JSON object")
			return
		}
		if strings.TrimSpace(request.IdempotencyKey) == "" {
			respondInvalid(c, "idempotency_key is required")
			return
		}
		if strings.TrimSpace(request.RequestFingerprint) == "" {
			respondInvalid(c, "request_fingerprint is required")
			return
		}
		if len(request.ResponseSnapshot) == 0 {
			request.ResponseSnapshot = json.RawMessage("null")
		}
		if !json.Valid(request.ResponseSnapshot) {
			respondInvalid(c, "response_snapshot must be valid JSON")
			return
		}
		if strings.TrimSpace(request.ExpiresAt) == "" {
			respondInvalid(c, "expires_at is required")
			return
		}
		expiresAt, err := time.Parse(time.RFC3339, request.ExpiresAt)
		if err != nil {
			respondInvalid(c, "expires_at must be an RFC 3339 timestamp")
			return
		}
		expiresAt = expiresAt.UTC()
		createdAt := time.Now().UTC()
		if !expiresAt.After(createdAt) {
			respondInvalid(c, "expires_at must be later than the record creation time")
			return
		}

		recordID, err := store.NewRecordID()
		if err != nil {
			respondInternal(c)
			return
		}
		candidate := store.Record{
			ID:                 recordID,
			IdempotencyKey:     request.IdempotencyKey,
			RequestFingerprint: request.RequestFingerprint,
			ResponseSnapshot:   request.ResponseSnapshot,
			CreatedAt:          createdAt,
			ExpiresAt:          expiresAt,
		}
		stored, inserted, err := st.PutIfAbsent(c.Request.Context(), candidate)
		if err != nil {
			respondInternal(c)
			return
		}
		if !inserted {
			if stored.RequestFingerprint != request.RequestFingerprint {
				c.JSON(http.StatusConflict, errorEnvelope{Error: fingerprintConflictBody{
					Code:                       "idempotency_fingerprint_conflict",
					Message:                    "a record for this idempotency key already exists with a different request fingerprint",
					ExistingRecordID:           stored.ID,
					ExistingRequestFingerprint: stored.RequestFingerprint,
				}})
				return
			}
			c.JSON(http.StatusOK, recordEnvelope{Record: newRecordResponse(stored)})
			return
		}
		c.JSON(http.StatusCreated, recordEnvelope{Record: newRecordResponse(stored)})
	}
}

// getRecord returns the single active record for one idempotency key.
func getRecord(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := c.Param("key")
		if strings.TrimSpace(key) == "" {
			respondNotFound(c)
			return
		}
		rec, err := st.ActiveRecord(c.Request.Context(), key)
		if err != nil {
			respondInternal(c)
			return
		}
		if rec == nil {
			respondNotFound(c)
			return
		}
		c.JSON(http.StatusOK, recordEnvelope{Record: newRecordResponse(rec)})
	}
}

// listRecords lists active records with expiry filters and stable cursor
// pagination in created-at-descending order.
func listRecords(st *store.Store) gin.HandlerFunc {
	return func(c *gin.Context) {
		filter := store.ListFilter{Limit: defaultListLimit}

		if raw := c.Query("limit"); raw != "" {
			limit, err := parsePositiveInt(raw, maxListLimit)
			if err != nil {
				respondInvalid(c, "limit must be an integer between 1 and 100")
				return
			}
			filter.Limit = limit
		}
		if raw := c.Query("status"); raw != "" {
			if raw != statusStored {
				respondInvalid(c, "status must be 'stored'")
				return
			}
		}
		if raw := c.Query("expires_after"); raw != "" {
			t, err := parseTimestamp(raw)
			if err != nil {
				respondInvalid(c, "expires_after must be an RFC 3339 timestamp")
				return
			}
			filter.ExpiresAfter = &t
		}
		if raw := c.Query("expires_before"); raw != "" {
			t, err := parseTimestamp(raw)
			if err != nil {
				respondInvalid(c, "expires_before must be an RFC 3339 timestamp")
				return
			}
			filter.ExpiresBefore = &t
		}
		if raw := c.Query("cursor"); raw != "" {
			cursor, err := decodeCursor(raw)
			if err != nil {
				c.JSON(http.StatusBadRequest, errorEnvelope{Error: errorBody{
					Code:    "invalid_cursor",
					Message: "the pagination cursor is invalid",
				}})
				return
			}
			filter.Cursor = &cursor
		}

		records, hasMore, err := st.ListActive(c.Request.Context(), filter)
		if err != nil {
			respondInternal(c)
			return
		}
		response := listRecordsResponse{Records: []recordResponse{}}
		for i := range records {
			response.Records = append(response.Records, newRecordResponse(&records[i]))
		}
		if hasMore && len(records) > 0 {
			last := records[len(records)-1]
			encoded, err := encodeCursor(store.Cursor{CreatedAt: last.CreatedAt, RecordID: last.ID})
			if err != nil {
				respondInternal(c)
				return
			}
			response.NextCursor = encoded
		}
		c.JSON(http.StatusOK, response)
	}
}

func parseTimestamp(raw string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC(), nil
}

func parsePositiveInt(raw string, maximum int) (int, error) {
	value := 0
	for _, r := range raw {
		if r < '0' || r > '9' {
			return 0, errors.New("not an integer")
		}
		value = value*10 + int(r-'0')
		if value > maximum {
			return 0, errors.New("out of range")
		}
	}
	if value < 1 {
		return 0, errors.New("must be positive")
	}
	return value, nil
}

func respondInvalid(c *gin.Context, message string) {
	c.JSON(http.StatusBadRequest, errorEnvelope{Error: errorBody{
		Code:    "invalid_idempotency_record",
		Message: message,
	}})
}

func respondNotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, errorEnvelope{Error: errorBody{
		Code:    "not_found",
		Message: "no active idempotency record exists for this key",
	}})
}

func respondInternal(c *gin.Context) {
	c.JSON(http.StatusInternalServerError, errorEnvelope{Error: errorBody{
		Code:    "internal_error",
		Message: "the request could not be completed",
	}})
}
