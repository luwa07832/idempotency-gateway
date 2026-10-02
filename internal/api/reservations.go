package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

const (
	codeReservationExpired = "idempotency_reservation_expired"

	headerReservationID = "Idempotency-Reservation-ID"
)

// reservationCreateRequest is the accepted body for placeholder creation. Times are RFC 3339 UTC
// strings and expiry must be strictly later than the creation moment.
type reservationCreateRequest struct {
	IdempotencyKey     string  `json:"idempotency_key"`
	RequestFingerprint string  `json:"request_fingerprint"`
	ExpiresAt          *string `json:"expires_at"`
}

// reservationResultRequest carries the first execution result for a placeholder.
type reservationResultRequest struct {
	RequestFingerprint string          `json:"request_fingerprint"`
	ResponseSnapshot   json.RawMessage `json:"response_snapshot"`
}

// reservationResponse lists its fields in the fixed order the placeholder contract uses; unlike
// records there is deliberately no response_snapshot field before a result exists.
type reservationResponse struct {
	ID                 string `json:"id"`
	IdempotencyKey     string `json:"idempotency_key"`
	RequestFingerprint string `json:"request_fingerprint"`
	Status             string `json:"status"`
	CreatedAt          string `json:"created_at"`
	ExpiresAt          string `json:"expires_at"`
}

type reservationConflictBody struct {
	Code               string `json:"code"`
	Message            string `json:"message"`
	ReservationID      string `json:"reservation_id"`
	RequestFingerprint string `json:"request_fingerprint"`
}

func registerReservationRoutes(router *gin.Engine, st *store.Store) {
	router.POST("/v1/idempotency/reservations", func(c *gin.Context) {
		handleCreateReservation(c, st)
	})
	router.POST("/v1/idempotency/reservations/:reservation_id/results", func(c *gin.Context) {
		handleReservationResult(c, st)
	})
}

// validateReservationCreate enforces the placeholder creation contract. It mirrors the record
// submission rules except that no response snapshot exists yet. Expiry must be strictly later
// than now: equality already means expired and is rejected.
func validateReservationCreate(rawBody []byte, now time.Time) (store.Reservation, string, bool) {
	var request reservationCreateRequest
	if err := json.Unmarshal(rawBody, &request); err != nil {
		return store.Reservation{}, "request body must be a single JSON object", false
	}
	if strings.TrimSpace(request.IdempotencyKey) == "" {
		return store.Reservation{}, "idempotency_key must not be empty", false
	}
	if strings.TrimSpace(request.RequestFingerprint) == "" {
		return store.Reservation{}, "request_fingerprint must not be empty", false
	}
	if request.ExpiresAt == nil || strings.TrimSpace(*request.ExpiresAt) == "" {
		return store.Reservation{}, "expires_at must be provided", false
	}
	expiresAt, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(*request.ExpiresAt))
	if err != nil {
		return store.Reservation{}, "expires_at must be an RFC 3339 timestamp", false
	}
	expiresAt = expiresAt.UTC()
	if !expiresAt.After(now) {
		return store.Reservation{}, "expires_at must be later than the current time", false
	}
	return store.Reservation{
		IdempotencyKey:     request.IdempotencyKey,
		RequestFingerprint: request.RequestFingerprint,
		CreatedAt:          now,
		ExpiresAt:          expiresAt,
	}, "", true
}

func validateReservationResult(rawBody []byte) (string, []byte, string, bool) {
	var request reservationResultRequest
	if err := json.Unmarshal(rawBody, &request); err != nil {
		return "", nil, "request body must be a single JSON object", false
	}
	if strings.TrimSpace(request.RequestFingerprint) == "" {
		return "", nil, "request_fingerprint must not be empty", false
	}
	snapshot := request.ResponseSnapshot
	if len(snapshot) == 0 {
		snapshot = json.RawMessage("null")
	}
	if !isValidJSONBytes(snapshot) {
		return "", nil, "response_snapshot must be valid JSON", false
	}
	return request.RequestFingerprint, snapshot, "", true
}

func handleCreateReservation(c *gin.Context, st *store.Store) {
	now := time.Now().UTC()

	rawBody, err := io.ReadAll(c.Request.Body)
	if err != nil || len(bytes.TrimSpace(rawBody)) == 0 {
		writeInvalidRecord(c, "request body must be a single JSON object")
		return
	}
	candidate, message, ok := validateReservationCreate(rawBody, now)
	if !ok {
		writeInvalidRecord(c, message)
		return
	}

	id, err := store.NewReservationID()
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	candidate.ID = id

	reservation, outcome, err := st.ReserveReservation(c.Request.Context(), candidate, now)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	switch outcome {
	case store.ReservationConflict:
		writeReservationConflict(c, reservation)
	default:
		// ReservationCreated and ReservationReplayed share the exact same success envelope; only
		// HTTP status and the outcome header distinguish the first placeholder from its reuse.
		status := http.StatusOK
		outcomeLabel := "replayed"
		if outcome == store.ReservationCreated {
			status = http.StatusCreated
			outcomeLabel = "created"
		}
		c.Header(headerOutcome, outcomeLabel)
		c.JSON(status, toReservationResponse(reservation))
	}
}

func handleReservationResult(c *gin.Context, st *store.Store) {
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
	fingerprint, snapshot, message, ok := validateReservationResult(rawBody)
	if !ok {
		writeInvalidRecord(c, message)
		return
	}

	record, reservation, outcome, err := st.CompleteReservation(
		c.Request.Context(), reservationID, fingerprint, snapshot, now)
	switch {
	case errors.Is(err, store.ErrReservationNotFound):
		writeReservationNotFound(c)
		return
	case err != nil:
		writeStorageUnavailable(c)
		return
	}
	switch outcome {
	case store.CompleteConflict:
		writeReservationConflict(c, reservation)
	case store.CompleteExpired:
		writeReservationExpired(c, reservation)
	default:
		// CompleteCreated and CompleteReplayed share the record envelope with the existing record
		// contract; only the outcome header distinguishes the first result from a replay.
		outcomeLabel := "replayed"
		if outcome == store.CompleteCreated {
			outcomeLabel = "created"
		}
		c.Header(headerOutcome, outcomeLabel)
		c.Header(headerRecordID, record.ID)
		writeRecordEnvelope(c, toRecordResponse(record))
	}
}

func toReservationResponse(reservation *store.Reservation) reservationResponse {
	return reservationResponse{
		ID:                 reservation.ID,
		IdempotencyKey:     reservation.IdempotencyKey,
		RequestFingerprint: reservation.RequestFingerprint,
		Status:             "pending",
		CreatedAt:          reservation.CreatedAt.Format(time.RFC3339Nano),
		ExpiresAt:          reservation.ExpiresAt.Format(time.RFC3339Nano),
	}
}

func writeReservationConflict(c *gin.Context, reservation *store.Reservation) {
	c.JSON(http.StatusConflict, gin.H{"error": reservationConflictBody{
		Code:               codeFingerprintConflict,
		Message:            "a reservation with this idempotency key exists but its request fingerprint differs",
		ReservationID:      reservation.ID,
		RequestFingerprint: reservation.RequestFingerprint,
	}})
}

func writeReservationExpired(c *gin.Context, reservation *store.Reservation) {
	c.JSON(http.StatusConflict, gin.H{"error": reservationConflictBody{
		Code:               codeReservationExpired,
		Message:            "the reservation expired before its execution result was committed",
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
