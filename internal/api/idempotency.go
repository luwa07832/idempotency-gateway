package api

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

const (
	codeFingerprintConflict = "idempotency_fingerprint_conflict"
	codeInvalidRecord       = "invalid_idempotency_record"
	codeNotFound            = "not_found"
	codeInvalidCursor       = "invalid_cursor"
	codeStorageUnavailable  = "storage_unavailable"

	defaultPageLimit = 50
	maxPageLimit     = 100
)

// submitRequest is the only accepted body for record submission. Times are RFC 3339 UTC strings;
// response_snapshot is any JSON value and is replayed byte-for-byte as stored.
type submitRequest struct {
	IdempotencyKey     string          `json:"idempotency_key"`
	RequestFingerprint string          `json:"request_fingerprint"`
	ResponseSnapshot   json.RawMessage `json:"response_snapshot"`
	ExpiresAt          *string         `json:"expires_at"`
}

// validatedSubmitRequest carries the normalized fields of an accepted submission.
type validatedSubmitRequest struct {
	idempotencyKey     string
	requestFingerprint string
	responseSnapshot   json.RawMessage
	expiresAt          time.Time
}

// validateSubmitRequest enforces the fixed submission contract. The message return value carries
// the fixed message for InvalidIdempotencyRequest. Expiry must be strictly later than now: a
// timestamp equal to the creation moment is already expired and is rejected.
func validateSubmitRequest(rawBody []byte, now time.Time) (validatedSubmitRequest, string, bool) {
	var request submitRequest
	if err := json.Unmarshal(rawBody, &request); err != nil {
		return validatedSubmitRequest{}, "request body must be a single JSON object", false
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" {
		return validatedSubmitRequest{}, "idempotency_key must not be empty", false
	}
	if strings.TrimSpace(request.RequestFingerprint) == "" {
		return validatedSubmitRequest{}, "request_fingerprint must not be empty", false
	}
	if request.ExpiresAt == nil || strings.TrimSpace(*request.ExpiresAt) == "" {
		return validatedSubmitRequest{}, "expires_at must be provided", false
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*request.ExpiresAt))
	if err != nil {
		return validatedSubmitRequest{}, "expires_at must be an RFC 3339 timestamp", false
	}
	expiresAt = expiresAt.UTC()
	if !expiresAt.After(now) {
		return validatedSubmitRequest{}, "expires_at must be later than the current time", false
	}
	snapshot := request.ResponseSnapshot
	if len(snapshot) == 0 {
		snapshot = json.RawMessage("null")
	}
	if !isValidJSON(snapshot) {
		return validatedSubmitRequest{}, "response_snapshot must be valid JSON", false
	}
	return validatedSubmitRequest{
		idempotencyKey:     request.IdempotencyKey,
		requestFingerprint: request.RequestFingerprint,
		responseSnapshot:   snapshot,
		expiresAt:          expiresAt,
	}, "", true
}

// recordResponse lists its fields in the fixed order every record-shaped result uses.
type recordResponse struct {
	ID                 string          `json:"id"`
	IdempotencyKey     string          `json:"idempotency_key"`
	Status             string          `json:"status"`
	RequestFingerprint string          `json:"request_fingerprint"`
	ResponseSnapshot   json.RawMessage `json:"response_snapshot"`
	CreatedAt          string          `json:"created_at"`
	ExpiresAt          string          `json:"expires_at"`
}

type recordEnvelope struct {
	Record recordResponse `json:"record"`
}

type listResponse struct {
	Records    []recordResponse `json:"records"`
	NextCursor string           `json:"next_cursor"`
}

type conflictBody struct {
	Code               string `json:"code"`
	Message            string `json:"message"`
	RecordID           string `json:"record_id"`
	RequestFingerprint string `json:"request_fingerprint"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// pageCursor is the opaque continuation token. Only the last row's creation time and id are
// needed because ordering is (created_at DESC, id DESC).
type pageCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

func (p pageCursor) encode() string {
	raw, _ := json.Marshal(p)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func parsePageCursor(token string) (pageCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return pageCursor{}, err
	}
	var cursor pageCursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return pageCursor{}, err
	}
	if cursor.ID == "" || cursor.CreatedAt.IsZero() {
		return pageCursor{}, errors.New("cursor missing fields")
	}
	return cursor, nil
}

// writeHistoryResponse serializes the audit page directly instead of going through c.JSON: the
// standard encoding/json Marshaler path compacts Marshaler output, which would normalize the
// stored response_snapshot bytes. Building the document from encoded strings and raw snapshot
// bytes preserves the first committed JSON verbatim while keeping the fixed field order.
func writeHistoryResponse(c *gin.Context, records []store.Record, nextCursor string) {
	var b bytes.Buffer
	b.Grow(512)
	b.WriteString(`{"records":[`)
	for i := range records {
		if i > 0 {
			b.WriteByte(',')
		}
		writeHistoryRecord(&b, &records[i])
	}
	b.WriteString(`],"next_cursor":`)
	encodedCursor, err := json.Marshal(nextCursor)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	b.Write(encodedCursor)
	b.WriteByte('}')
	c.Data(http.StatusOK, "application/json; charset=utf-8", b.Bytes())
}

func writeHistoryRecord(b *bytes.Buffer, record *store.Record) {
	b.WriteString(`{"id":`)
	writeJSONString(b, record.ID)
	writeJSONField(b, "idempotency_key", record.IdempotencyKey)
	writeJSONField(b, "status", "expired")
	writeJSONField(b, "request_fingerprint", record.RequestFingerprint)
	b.WriteString(`,"response_snapshot":`)
	snapshot := record.ResponseSnapshot
	if len(snapshot) == 0 {
		snapshot = []byte("null")
	}
	b.Write(snapshot)
	writeJSONField(b, "created_at", record.CreatedAt.Format(time.RFC3339Nano))
	writeJSONField(b, "expires_at", record.ExpiresAt.Format(time.RFC3339Nano))
	b.WriteByte('}')
}

func writeJSONField(b *bytes.Buffer, name, value string) {
	b.WriteString(`,"`)
	b.WriteString(name)
	b.WriteString(`":`)
	writeJSONString(b, value)
}

func writeJSONString(b *bytes.Buffer, value string) {
	encoded, err := json.Marshal(value)
	if err != nil {
		b.WriteString("null")
		return
	}
	b.Write(encoded)
}

func registerIdempotencyRoutes(router *gin.Engine, st *store.Store) {
	router.POST("/v1/idempotency/records", func(c *gin.Context) {
		handleSubmit(c, st)
	})
	router.GET("/v1/idempotency/records/:key", func(c *gin.Context) {
		handleGetRecord(c, st)
	})
	router.GET("/v1/idempotency/records", func(c *gin.Context) {
		handleListRecords(c, st)
	})
	router.GET("/v1/idempotency/history", func(c *gin.Context) {
		handleListHistory(c, st)
	})
}

func handleSubmit(c *gin.Context, st *store.Store) {
	now := time.Now().UTC()

	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil || len(bytes.TrimSpace(rawBody)) == 0 {
		writeInvalidRecord(c, "request body must be a single JSON object")
		return
	}
	validated, message, ok := validateSubmitRequest(rawBody, now)
	if !ok {
		writeInvalidRecord(c, message)
		return
	}

	id, err := store.NewRecordID()
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	candidate := store.Record{
		ID:                 id,
		IdempotencyKey:     validated.idempotencyKey,
		RequestFingerprint: validated.requestFingerprint,
		ResponseSnapshot:   validated.responseSnapshot,
		CreatedAt:          now,
		ExpiresAt:          validated.expiresAt,
	}

	record, outcome, err := st.PutRecord(c.Request.Context(), candidate, now)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	switch outcome {
	case store.PutConflict:
		c.JSON(http.StatusConflict, gin.H{"error": conflictBody{
			Code:               codeFingerprintConflict,
			Message:            "a record with this idempotency key exists but its request fingerprint differs",
			RecordID:           record.ID,
			RequestFingerprint: record.RequestFingerprint,
		}})
	default:
		// Both PutCreated and PutReplayed use the exact same success envelope, so a replay is
		// indistinguishable in shape from the first successful execution.
		c.JSON(http.StatusOK, recordEnvelope{Record: toRecordResponse(record)})
	}
}

func handleGetRecord(c *gin.Context, st *store.Store) {
	key := c.Param("key")
	if strings.TrimSpace(key) == "" {
		writeNotFound(c)
		return
	}
	record, err := st.ActiveRecordByKey(c.Request.Context(), key, time.Now().UTC())
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	if record == nil {
		writeNotFound(c)
		return
	}
	c.JSON(http.StatusOK, recordEnvelope{Record: toRecordResponse(record)})
}

func handleListRecords(c *gin.Context, st *store.Store) {
	now := time.Now().UTC()
	filter := store.ListFilter{Limit: defaultPageLimit}

	if key := strings.TrimSpace(c.Query("key")); key != "" {
		filter.IdempotencyKey = key
	}
	// The fingerprint is matched character-for-character: the raw query value is used as-is with
	// no trimming, case folding or other normalization. Only the empty (absent) value falls back
	// to the unfiltered baseline; a whitespace-only value is still a real filter that matches
	// nothing.
	filter.RequestFingerprint = c.Query("request_fingerprint")

	switch strings.TrimSpace(c.Query("status")) {
	case "", "active":
	case "expired":
	default:
		writeInvalidRecord(c, "status must be active or expired")
		return
	}

	if raw := strings.TrimSpace(c.Query("expires_before")); raw != "" {
		parsed, ok := parseQueryTime(c, raw)
		if !ok {
			return
		}
		filter.ExpiresBefore = &parsed
	}
	if raw := strings.TrimSpace(c.Query("expires_after")); raw != "" {
		parsed, ok := parseQueryTime(c, raw)
		if !ok {
			return
		}
		filter.ExpiresAfter = &parsed
	}

	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		limit, ok := parseQueryLimit(c, rawLimit)
		if !ok {
			return
		}
		filter.Limit = limit
	}

	if rawCursor := strings.TrimSpace(c.Query("cursor")); rawCursor != "" {
		cursor, err := parsePageCursor(rawCursor)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": errorBody{
				Code:    codeInvalidCursor,
				Message: "pagination cursor is invalid",
			}})
			return
		}
		filter.CursorCreatedAt = cursor.CreatedAt
		filter.CursorID = cursor.ID
	}

	// Expired records never participate in normal results; the page is deterministically empty
	// rather than surfacing historical snapshots. Parameters above are still validated first.
	if strings.TrimSpace(c.Query("status")) == "expired" {
		c.JSON(http.StatusOK, listResponse{Records: []recordResponse{}, NextCursor: ""})
		return
	}

	// Fetch one extra row to decide whether another page exists without an unstable total count.
	filter.Limit++
	records, err := st.ListRecords(c.Request.Context(), filter, now)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}

	nextCursor := ""
	if len(records) == filter.Limit {
		last := records[len(records)-2]
		nextCursor = pageCursor{CreatedAt: last.CreatedAt, ID: last.ID}.encode()
		records = records[:len(records)-1]
	}

	response := listResponse{Records: make([]recordResponse, 0, len(records))}
	for i := range records {
		response.Records = append(response.Records, toRecordResponse(&records[i]))
	}
	response.NextCursor = nextCursor
	c.JSON(http.StatusOK, response)
}

func handleListHistory(c *gin.Context, st *store.Store) {
	now := time.Now().UTC()

	key := strings.TrimSpace(c.Query("key"))
	if key == "" {
		writeInvalidRecord(c, "key must identify an idempotency record")
		return
	}

	filter := store.HistoryFilter{IdempotencyKey: key, Limit: defaultPageLimit}
	// The fingerprint is matched character-for-character against the raw query value: no trimming,
	// case folding or other normalization. An empty value simply does not filter.
	filter.RequestFingerprint = c.Query("request_fingerprint")

	if raw := strings.TrimSpace(c.Query("expires_before")); raw != "" {
		parsed, ok := parseQueryTime(c, raw)
		if !ok {
			return
		}
		filter.ExpiresBefore = &parsed
	}
	if raw := strings.TrimSpace(c.Query("expires_after")); raw != "" {
		parsed, ok := parseQueryTime(c, raw)
		if !ok {
			return
		}
		filter.ExpiresAfter = &parsed
	}

	if rawLimit := strings.TrimSpace(c.Query("limit")); rawLimit != "" {
		limit, ok := parseQueryLimit(c, rawLimit)
		if !ok {
			return
		}
		filter.Limit = limit
	}

	if rawCursor := strings.TrimSpace(c.Query("cursor")); rawCursor != "" {
		cursor, err := parsePageCursor(rawCursor)
		if err != nil || !store.ValidRecordID(cursor.ID) {
			c.JSON(http.StatusBadRequest, gin.H{"error": errorBody{
				Code:    codeInvalidCursor,
				Message: "pagination cursor is invalid",
			}})
			return
		}
		filter.CursorCreatedAt = cursor.CreatedAt
		filter.CursorID = cursor.ID
	}

	// Fetch one extra row to decide whether another page exists without an unstable total count.
	filter.Limit++
	records, err := st.ListHistoryRecords(c.Request.Context(), filter, now)
	if err != nil {
		if errors.Is(err, store.ErrInvalidID) {
			c.JSON(http.StatusBadRequest, gin.H{"error": errorBody{
				Code:    codeInvalidCursor,
				Message: "pagination cursor is invalid",
			}})
			return
		}
		writeStorageUnavailable(c)
		return
	}

	nextCursor := ""
	if len(records) == filter.Limit {
		last := records[len(records)-2]
		nextCursor = pageCursor{CreatedAt: last.CreatedAt, ID: last.ID}.encode()
		records = records[:len(records)-1]
	}

	writeHistoryResponse(c, records, nextCursor)
}

func parseQueryTime(c *gin.Context, raw string) (time.Time, bool) {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		writeInvalidRecord(c, "time filters must be RFC 3339 timestamps")
		return time.Time{}, false
	}
	return parsed.UTC(), true
}

func parseQueryLimit(c *gin.Context, raw string) (int, bool) {
	limit := 0
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			writeInvalidRecord(c, "limit must be a positive integer")
			return 0, false
		}
		limit = limit*10 + int(digit-'0')
		if limit > maxPageLimit {
			limit = maxPageLimit + 1
		}
	}
	if limit < 1 || limit > maxPageLimit {
		writeInvalidRecord(c, fmt.Sprintf("limit must be between 1 and %d", maxPageLimit))
		return 0, false
	}
	return limit, true
}

func toRecordResponse(record *store.Record) recordResponse {
	return recordResponse{
		ID:                 record.ID,
		IdempotencyKey:     record.IdempotencyKey,
		Status:             "active",
		RequestFingerprint: record.RequestFingerprint,
		ResponseSnapshot:   record.ResponseSnapshot,
		CreatedAt:          record.CreatedAt.Format(time.RFC3339Nano),
		ExpiresAt:          record.ExpiresAt.Format(time.RFC3339Nano),
	}
}

func isValidJSON(raw json.RawMessage) bool {
	var value any
	return json.Unmarshal(raw, &value) == nil
}

func writeInvalidRecord(c *gin.Context, message string) {
	c.JSON(http.StatusBadRequest, gin.H{"error": errorBody{Code: codeInvalidRecord, Message: message}})
}

func writeNotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{"error": errorBody{
		Code:    codeNotFound,
		Message: "no active idempotency record exists for this key",
	}})
}

func writeStorageUnavailable(c *gin.Context) {
	c.JSON(http.StatusServiceUnavailable, gin.H{"error": errorBody{
		Code:    codeStorageUnavailable,
		Message: "database is not available",
	}})
}
