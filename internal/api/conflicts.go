package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

const codeInvalidConflictQuery = "invalid_conflict_query"

// conflictEventResponse lists its fields in the fixed order every conflict event uses. It never
// carries a response snapshot: events expose only the two fingerprints and the identity of the
// record they collided with.
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

// handleListConflicts serves the read-only fingerprint-conflict audit. Events are appended by the
// submission path only when a 409 conflict is durably recorded; this handler never writes and an
// empty result set is an HTTP 200 empty page rather than an error.
func handleListConflicts(c *gin.Context, st *store.Store) {
	filter := store.ConflictFilter{Limit: defaultPageLimit}

	// All three filters match the raw query value character-for-character (no trimming, case
	// folding or prefix matching); an absent or empty value means the filter is not applied.
	filter.IdempotencyKey = c.Query("key")
	filter.ObservedRequestFingerprint = c.Query("observed_request_fingerprint")
	filter.ExistingRecordID = c.Query("existing_record_id")

	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		limit, ok := parseConflictLimit(c, rawLimit)
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
	writeConflictListResponse(c, events, nextCursor)
}

func parseConflictLimit(c *gin.Context, raw string) (int, bool) {
	limit := 0
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			writeInvalidConflictQuery(c, fmt.Sprintf("limit must be an integer between 1 and %d", maxPageLimit))
			return 0, false
		}
		limit = limit*10 + int(digit-'0')
		if limit > maxPageLimit {
			limit = maxPageLimit + 1
		}
	}
	if limit < 1 || limit > maxPageLimit {
		writeInvalidConflictQuery(c, fmt.Sprintf("limit must be between 1 and %d", maxPageLimit))
		return 0, false
	}
	return limit, true
}

// writeConflictListResponse writes {"events":[...],"next_cursor":...}. Every event field is a
// string, so struct marshaling already preserves the fixed field order.
func writeConflictListResponse(c *gin.Context, events []store.ConflictEvent, nextCursor string) {
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
		object, err := json.Marshal(toConflictEventResponse(&events[i]))
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
	c.JSON(http.StatusBadRequest, gin.H{"error": errorBody{
		Code:    codeInvalidConflictQuery,
		Message: message,
	}})
}
