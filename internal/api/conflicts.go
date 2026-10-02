package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

const codeInvalidConflictQuery = "invalid_conflict_query"

// conflictEventResponse lists the fixed audit fields in their fixed order. Response snapshots are
// never part of an event and are not reachable through this endpoint.
type conflictEventResponse struct {
	ID                         string `json:"id"`
	IdempotencyKey             string `json:"idempotency_key"`
	ObservedRequestFingerprint string `json:"observed_request_fingerprint"`
	ExistingRecordID           string `json:"existing_record_id"`
	ExistingRequestFingerprint string `json:"existing_request_fingerprint"`
	CreatedAt                  string `json:"created_at"`
}

func toConflictEventResponse(event *store.ConflictEvent) conflictEventResponse {
	return conflictEventResponse{
		ID:                         event.ID,
		IdempotencyKey:             event.IdempotencyKey,
		ObservedRequestFingerprint: event.ObservedRequestFingerprint,
		ExistingRecordID:           event.ExistingRecordID,
		ExistingRequestFingerprint: event.ExistingRequestFingerprint,
		CreatedAt:                  event.CreatedAt.Format(time.RFC3339Nano),
	}
}

// marshalConflictEventObject renders one event with the fixed field order.
func marshalConflictEventObject(event conflictEventResponse) ([]byte, error) {
	id, err := json.Marshal(event.ID)
	if err != nil {
		return nil, err
	}
	key, err := json.Marshal(event.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	observed, err := json.Marshal(event.ObservedRequestFingerprint)
	if err != nil {
		return nil, err
	}
	existingID, err := json.Marshal(event.ExistingRecordID)
	if err != nil {
		return nil, err
	}
	existingFingerprint, err := json.Marshal(event.ExistingRequestFingerprint)
	if err != nil {
		return nil, err
	}
	createdAt, err := json.Marshal(event.CreatedAt)
	if err != nil {
		return nil, err
	}

	var body bytes.Buffer
	body.Grow(len(id) + len(key) + len(observed) + len(existingID) + len(existingFingerprint) + len(createdAt) + 128)
	body.WriteString(`{"id":`)
	body.Write(id)
	body.WriteString(`,"idempotency_key":`)
	body.Write(key)
	body.WriteString(`,"observed_request_fingerprint":`)
	body.Write(observed)
	body.WriteString(`,"existing_record_id":`)
	body.Write(existingID)
	body.WriteString(`,"existing_request_fingerprint":`)
	body.Write(existingFingerprint)
	body.WriteString(`,"created_at":`)
	body.Write(createdAt)
	body.WriteByte('}')
	return body.Bytes(), nil
}

// writeConflictListResponse writes {"events":[...],"next_cursor":...}.
func writeConflictListResponse(c *gin.Context, events []conflictEventResponse, nextCursor string) {
	encodedCursor, err := json.Marshal(nextCursor)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}

	var body bytes.Buffer
	body.WriteString(`{"events":[`)
	for i := range events {
		if i > 0 {
			body.WriteByte(',')
		}
		object, err := marshalConflictEventObject(events[i])
		if err != nil {
			writeStorageUnavailable(c)
			return
		}
		body.Write(object)
	}
	body.WriteString(`],"next_cursor":`)
	body.Write(encodedCursor)
	body.WriteByte('}')
	c.Data(http.StatusOK, "application/json; charset=utf-8", body.Bytes())
}

func writeInvalidConflictQuery(c *gin.Context, message string) {
	c.JSON(http.StatusBadRequest, gin.H{"error": errorBody{Code: codeInvalidConflictQuery, Message: message}})
}

// parseConflictQueryLimit accepts only a decimal integer in 1..100; anything else is an invalid
// conflict query (not a record error).
func parseConflictQueryLimit(c *gin.Context, raw string) (int, bool) {
	limit := 0
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			writeInvalidConflictQuery(c, "limit must be a positive integer")
			return 0, false
		}
		limit = limit*10 + int(digit-'0')
		if limit > maxPageLimit {
			limit = maxPageLimit + 1
		}
	}
	if limit < 1 || limit > maxPageLimit {
		writeInvalidConflictQuery(c, limitOutOfRangeMessage)
		return 0, false
	}
	return limit, true
}

// handleListConflicts serves the read-only fingerprint-conflict audit. Every filter matches its
// stored value character-for-character (raw query value, no trimming or normalization); only an
// absent/empty parameter is unfiltered. No event field can be written through this endpoint and
// no response snapshot is exposed.
func handleListConflicts(c *gin.Context, st *store.Store) {
	filter := store.ConflictFilter{Limit: defaultPageLimit}

	filter.IdempotencyKey = c.Query("key")
	filter.ObservedRequestFingerprint = c.Query("observed_request_fingerprint")
	filter.ExistingRecordID = c.Query("existing_record_id")

	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		limit, ok := parseConflictQueryLimit(c, rawLimit)
		if !ok {
			return
		}
		filter.Limit = limit
	}

	if rawCursor := strings.TrimSpace(c.Query("cursor")); rawCursor != "" {
		cursor, err := parsePageCursor(rawCursor)
		if err != nil {
			writeInvalidCursor(c)
			return
		}
		filter.CursorCreatedAt = cursor.CreatedAt
		filter.CursorID = cursor.ID
	}

	// Fetch one extra row to decide whether another page exists without an unstable total count.
	filter.Limit++
	events, err := st.ListConflictEvents(c.Request.Context(), filter)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrInvalidID), errors.Is(err, store.ErrCursorTarget):
			writeInvalidCursor(c)
		default:
			writeStorageUnavailable(c)
		}
		return
	}

	nextCursor := ""
	if len(events) == filter.Limit {
		last := events[len(events)-2]
		nextCursor = pageCursor{CreatedAt: last.CreatedAt, ID: last.ID}.encode()
		events = events[:len(events)-1]
	}

	page := make([]conflictEventResponse, 0, len(events))
	for i := range events {
		page = append(page, toConflictEventResponse(&events[i]))
	}
	writeConflictListResponse(c, page, nextCursor)
}
