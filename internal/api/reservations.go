package api

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

const codeReservationExpired = "idempotency_reservation_expired"

// reservationRequest is the only accepted body for reservation creation. Times are RFC 3339 UTC
// strings; a reservation carries no response snapshot until its first result is submitted.
type reservationRequest struct {
	IdempotencyKey     string  `json:"idempotency_key"`
	RequestFingerprint string  `json:"request_fingerprint"`
	ExpiresAt          *string `json:"expires_at"`
}

// resultRequest is the only accepted body for result submission. response_snapshot is any JSON
// value and is stored byte-for-byte on the record the first result materializes into.
type resultRequest struct {
	RequestFingerprint string          `json:"request_fingerprint"`
	ResponseSnapshot   json.RawMessage `json:"response_snapshot"`
}

// reservationResponse lists its fields in the fixed order every reservation-shaped result uses.
type reservationResponse struct {
	ID                 string `json:"id"`
	IdempotencyKey     string `json:"idempotency_key"`
	RequestFingerprint string `json:"request_fingerprint"`
	Status             string `json:"status"`
	CreatedAt          string `json:"created_at"`
	ExpiresAt          string `json:"expires_at"`
}

// reservationConflictBody is the fixed fingerprint-conflict shape for reservation endpoints:
// the error object followed by the stored reservation's id and request fingerprint.
type reservationConflictBody struct {
	Code               string `json:"code"`
	Message            string `json:"message"`
	ReservationID      string `json:"reservation_id"`
	RequestFingerprint string `json:"request_fingerprint"`
}

// reservationStatus derives the placeholder's lifecycle at now: a linked result means completed,
// an expiry at or before now without a result means expired, otherwise the placeholder is still
// pending.
func reservationStatus(reservation *store.Reservation, now time.Time) string {
	if reservation.RecordID != "" {
		return "completed"
	}
	if !reservation.ExpiresAt.After(now) {
		return "expired"
	}
	return "pending"
}

func toReservationResponse(reservation *store.Reservation, now time.Time) reservationResponse {
	return reservationResponse{
		ID:                 reservation.ID,
		IdempotencyKey:     reservation.IdempotencyKey,
		RequestFingerprint: reservation.RequestFingerprint,
		Status:             reservationStatus(reservation, now),
		CreatedAt:          reservation.CreatedAt.Format(time.RFC3339Nano),
		ExpiresAt:          reservation.ExpiresAt.Format(time.RFC3339Nano),
	}
}

// handleCreateReservation elects the execution placeholder for one idempotency key. The first
// request creates a pending placeholder (201, outcome created); a same-key same-fingerprint
// request reuses the unexpired placeholder (200, outcome replayed); a same-key
// different-fingerprint request conflicts. Once the placeholder expires without owning the key
// any longer, the same key can create a fresh placeholder.
func handleCreateReservation(c *gin.Context, st *store.Store) {
	now := time.Now().UTC()

	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil || len(bytes.TrimSpace(rawBody)) == 0 {
		writeInvalidRecord(c, "request body must be a single JSON object")
		return
	}
	var request reservationRequest
	if err := json.Unmarshal(rawBody, &request); err != nil {
		writeInvalidRecord(c, "request body must be a single JSON object")
		return
	}
	expiresAt, message, ok := validateKeyFingerprintExpiry(request.IdempotencyKey, request.RequestFingerprint, request.ExpiresAt, now)
	if !ok {
		writeInvalidRecord(c, message)
		return
	}

	id, err := store.NewReservationID()
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	candidate := store.Reservation{
		ID:                 id,
		IdempotencyKey:     request.IdempotencyKey,
		RequestFingerprint: request.RequestFingerprint,
		CreatedAt:          now,
		ExpiresAt:          expiresAt,
	}

	reservation, outcome, err := st.PutReservation(c.Request.Context(), candidate, now)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	switch outcome {
	case store.PutConflict:
		writeReservationConflict(c, reservation, "a reservation with this idempotency key exists but its request fingerprint differs")
	default:
		// PutCreated and PutReplayed share the exact same reservation envelope; only the status
		// code and the outcome header distinguish the first placeholder from a reuse.
		outcomeLabel := "replayed"
		status := http.StatusOK
		if outcome == store.PutCreated {
			outcomeLabel = "created"
			status = http.StatusCreated
		}
		c.Header(headerOutcome, outcomeLabel)
		c.JSON(status, gin.H{"reservation": toReservationResponse(reservation, now)})
	}
}

// handleSubmitResult attaches the execution result to its placeholder. The first accepted
// result materializes as a regular record (rec_ id, timestamps copied from the placeholder,
// snapshot stored byte-for-byte) and is returned with outcome created; later submissions with
// the matching fingerprint replay that record byte-for-byte with outcome replayed. A mismatched
// fingerprint conflicts, an expired placeholder rejects results, and an unknown or malformed
// reservation id is a client error. Nothing is written unless the whole result commits.
func handleSubmitResult(c *gin.Context, st *store.Store) {
	now := time.Now().UTC()

	reservationID := c.Param("reservation_id")
	if !store.IsReservationID(reservationID) {
		writeInvalidRecord(c, "reservation_id must be \"res_\" followed by 32 lowercase hexadecimal characters")
		return
	}

	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil || len(bytes.TrimSpace(rawBody)) == 0 {
		writeInvalidRecord(c, "request body must be a single JSON object")
		return
	}
	var request resultRequest
	if err := json.Unmarshal(rawBody, &request); err != nil {
		writeInvalidRecord(c, "request body must be a single JSON object")
		return
	}
	if strings.TrimSpace(request.RequestFingerprint) == "" {
		writeInvalidRecord(c, "request_fingerprint must not be empty")
		return
	}
	snapshot := request.ResponseSnapshot
	if len(snapshot) == 0 {
		snapshot = json.RawMessage("null")
	}
	if !isValidJSONBytes(snapshot) {
		writeInvalidRecord(c, "response_snapshot must be valid JSON")
		return
	}

	recordID, err := store.NewRecordID()
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	reservation, record, outcome, err := st.SubmitResult(c.Request.Context(), reservationID, request.RequestFingerprint, snapshot, recordID, now)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	switch outcome {
	case store.ResultNotFound:
		writeReservationNotFound(c)
	case store.ResultConflict:
		writeReservationConflict(c, reservation, "the reservation was created with a different request fingerprint")
	case store.ResultExpired:
		c.JSON(http.StatusConflict, gin.H{"error": errorBody{
			Code:    codeReservationExpired,
			Message: "the reservation expired before its first result was submitted",
		}})
	default:
		// ResultCreated and ResultReplayed share the record envelope; only the outcome header
		// distinguishes the first stored result from a byte-for-byte replay.
		outcomeLabel := "replayed"
		if outcome == store.ResultCreated {
			outcomeLabel = "created"
		}
		c.Header(headerOutcome, outcomeLabel)
		c.Header(headerRecordID, record.ID)
		response := toRecordResponse(record)
		// Status is derived at response time: a replay after the record's expiry reports
		// expired, while the record itself is never rewritten.
		if !record.ExpiresAt.After(now) {
			response.Status = "expired"
		}
		writeRecordEnvelope(c, response)
	}
}

func writeReservationConflict(c *gin.Context, reservation *store.Reservation, message string) {
	c.JSON(http.StatusConflict, gin.H{"error": reservationConflictBody{
		Code:               codeFingerprintConflict,
		Message:            message,
		ReservationID:      reservation.ID,
		RequestFingerprint: reservation.RequestFingerprint,
	}})
}

func writeReservationNotFound(c *gin.Context) {
	c.JSON(http.StatusNotFound, gin.H{"error": errorBody{
		Code:    codeNotFound,
		Message: "no idempotency reservation exists for this reservation id",
	}})
}
