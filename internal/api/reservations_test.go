package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func reservationBody(key, fingerprint string, expiresAt time.Time) string {
	return `{"idempotency_key":"` + key + `","request_fingerprint":"` + fingerprint +
		`","expires_at":"` + expiresAt.Format(time.RFC3339Nano) + `"}`
}

func resultBody(fingerprint, snapshot string) string {
	return `{"request_fingerprint":"` + fingerprint + `","response_snapshot":` + snapshot + `}`
}

func decodeError(t *testing.T, body string) map[string]any {
	t.Helper()
	var parsed struct {
		Error map[string]any `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("decode error body %q: %v", body, err)
	}
	return parsed.Error
}

func TestCreateReservationStatusShapeAndReuse(t *testing.T) {
	handler := newAPIRouter(t)
	expiresAt := time.Now().UTC().Add(time.Hour)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations",
		reservationBody("k1", "fp1", expiresAt))
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d body = %s", created.Code, created.Body.String())
	}
	if got := created.Header().Get(headerOutcome); got != "created" {
		t.Fatalf("create outcome header = %q, want created", got)
	}
	var createdParsed struct {
		ID                 string `json:"id"`
		IdempotencyKey     string `json:"idempotency_key"`
		RequestFingerprint string `json:"request_fingerprint"`
		Status             string `json:"status"`
		CreatedAt          string `json:"created_at"`
		ExpiresAt          string `json:"expires_at"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createdParsed); err != nil {
		t.Fatalf("decode: %v body = %s", err, created.Body.String())
	}
	if !storeIsReservationID(createdParsed.ID) {
		t.Fatalf("reservation id shape = %q", createdParsed.ID)
	}
	if createdParsed.IdempotencyKey != "k1" || createdParsed.RequestFingerprint != "fp1" ||
		createdParsed.Status != "pending" || createdParsed.CreatedAt == "" ||
		createdParsed.ExpiresAt != expiresAt.Format(time.RFC3339Nano) {
		t.Fatalf("unexpected reservation body: %+v", createdParsed)
	}
	if strings.Contains(created.Body.String(), "response_snapshot") {
		t.Fatalf("pending placeholder leaked a snapshot: %s", created.Body.String())
	}

	replayed := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations",
		reservationBody("k1", "fp1", expiresAt))
	if replayed.Code != http.StatusOK {
		t.Fatalf("reuse status = %d body = %s", replayed.Code, replayed.Body.String())
	}
	if got := replayed.Header().Get(headerOutcome); got != "replayed" {
		t.Fatalf("reuse outcome header = %q, want replayed", got)
	}
	if replayed.Body.String() != created.Body.String() {
		t.Fatalf("reuse body = %s\nwant %s", replayed.Body.String(), created.Body.String())
	}

	conflict := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations",
		reservationBody("k1", "fp-other", expiresAt))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body = %s", conflict.Code, conflict.Body.String())
	}
	errBody := decodeError(t, conflict.Body.String())
	if errBody["code"] != "idempotency_fingerprint_conflict" ||
		errBody["reservation_id"] != createdParsed.ID || errBody["request_fingerprint"] != "fp1" {
		t.Fatalf("unexpected conflict body: %v", errBody)
	}
}

func storeIsReservationID(id string) bool {
	return strings.HasPrefix(id, "res_") && len(id) == len("res_")+32
}

func TestCreateReservationValidation(t *testing.T) {
	handler := newAPIRouter(t)
	valid := time.Now().UTC().Add(time.Hour)
	cases := map[string]string{
		"missing key":         `{"idempotency_key":"","request_fingerprint":"fp1","expires_at":"` + valid.Format(time.RFC3339Nano) + `"}`,
		"missing fingerprint": `{"idempotency_key":"k1","request_fingerprint":"","expires_at":"` + valid.Format(time.RFC3339Nano) + `"}`,
		"missing expires_at":  `{"idempotency_key":"k1","request_fingerprint":"fp1"}`,
		"bad time":            `{"idempotency_key":"k1","request_fingerprint":"fp1","expires_at":"not-a-time"}`,
		"past expires_at":     reservationBody("k1", "fp1", time.Now().UTC().Add(-time.Minute)),
		"equal expires_at":    reservationBody("k1", "fp1", time.Now().UTC()),
		"not json":            `{"idempotency_key":`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			resp := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations", body)
			if resp.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", resp.Code, resp.Body.String())
			}
			if got := decodeError(t, resp.Body.String())["code"]; got != "invalid_idempotency_record" {
				t.Fatalf("code = %v", got)
			}
		})
	}

	// A rejected request must not have written anything: a valid follow-up is the single
	// creator and reports "created".
	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations",
		reservationBody("k1", "fp1", time.Now().UTC().Add(time.Hour)))
	if created.Code != http.StatusCreated || created.Header().Get(headerOutcome) != "created" {
		t.Fatalf("follow-up after rejected writes: status=%d outcome=%q",
			created.Code, created.Header().Get(headerOutcome))
	}
}

func TestReservationResultLifecycle(t *testing.T) {
	handler := newAPIRouter(t)
	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations",
		reservationBody("k1", "fp1", time.Now().UTC().Add(time.Hour)))
	var reservation struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &reservation); err != nil {
		t.Fatalf("decode reservation: %v", err)
	}

	first := doJSON(t, handler, http.MethodPost,
		"/v1/idempotency/reservations/"+reservation.ID+"/results",
		resultBody("fp1", `{"price": 100}`))
	if first.Code != http.StatusOK {
		t.Fatalf("first result status = %d body = %s", first.Code, first.Body.String())
	}
	if got := first.Header().Get(headerOutcome); got != "created" {
		t.Fatalf("first outcome = %q, want created", got)
	}
	if !strings.HasPrefix(first.Header().Get(headerRecordID), "rec_") {
		t.Fatalf("record id header = %q", first.Header().Get(headerRecordID))
	}
	if !strings.Contains(first.Body.String(), `"status":"active"`) ||
		!strings.Contains(first.Body.String(), `"response_snapshot":{"price": 100}`) {
		t.Fatalf("first result body lost fields or compacted snapshot: %s", first.Body.String())
	}

	replayed := doJSON(t, handler, http.MethodPost,
		"/v1/idempotency/reservations/"+reservation.ID+"/results",
		resultBody("fp1", `  {"price": 999}  `))
	if replayed.Code != http.StatusOK {
		t.Fatalf("replay status = %d", replayed.Code)
	}
	if got := replayed.Header().Get(headerOutcome); got != "replayed" {
		t.Fatalf("replay outcome = %q, want replayed", got)
	}
	if replayed.Body.String() != first.Body.String() {
		t.Fatalf("replay body = %s\nwant %s", replayed.Body.String(), first.Body.String())
	}
	if !strings.Contains(replayed.Body.String(), `"price": 100`) {
		t.Fatalf("replay exposed a later snapshot: %s", replayed.Body.String())
	}
}

func TestReservationResultGuards(t *testing.T) {
	handler := newAPIRouter(t)
	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations",
		reservationBody("k1", "fp1", time.Now().UTC().Add(time.Hour)))
	var reservation struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &reservation); err != nil {
		t.Fatalf("decode: %v", err)
	}

	conflict := doJSON(t, handler, http.MethodPost,
		"/v1/idempotency/reservations/"+reservation.ID+"/results",
		resultBody("fp-other", `null`))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body = %s", conflict.Code, conflict.Body.String())
	}
	errBody := decodeError(t, conflict.Body.String())
	if errBody["code"] != "idempotency_fingerprint_conflict" ||
		errBody["reservation_id"] != reservation.ID {
		t.Fatalf("unexpected conflict body: %v", errBody)
	}

	badID := doJSON(t, handler, http.MethodPost,
		"/v1/idempotency/reservations/not-an-id/results", resultBody("fp1", `null`))
	if badID.Code != http.StatusBadRequest {
		t.Fatalf("bad id status = %d", badID.Code)
	}
	if got := decodeError(t, badID.Body.String())["code"]; got != "invalid_idempotency_record" {
		t.Fatalf("bad id code = %v", got)
	}

	missing := doJSON(t, handler, http.MethodPost,
		"/v1/idempotency/reservations/res_00000000000000000000000000000000/results",
		resultBody("fp1", `null`))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d body = %s", missing.Code, missing.Body.String())
	}
	if got := decodeError(t, missing.Body.String())["code"]; got != "not_found" {
		t.Fatalf("missing code = %v", got)
	}

	invalid := doJSON(t, handler, http.MethodPost,
		"/v1/idempotency/reservations/"+reservation.ID+"/results",
		`{"request_fingerprint":"","response_snapshot":null}`)
	if invalid.Code != http.StatusBadRequest ||
		decodeError(t, invalid.Body.String())["code"] != "invalid_idempotency_record" {
		t.Fatalf("invalid body status = %d: %s", invalid.Code, invalid.Body.String())
	}
}

func TestReservationExpiryAllowsNewPlaceholder(t *testing.T) {
	handler := newAPIRouter(t)
	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations",
		reservationBody("k1", "fp1", time.Now().UTC().Add(20*time.Millisecond)))
	var reservation struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &reservation); err != nil {
		t.Fatalf("decode: %v", err)
	}

	time.Sleep(40 * time.Millisecond)
	expired := doJSON(t, handler, http.MethodPost,
		"/v1/idempotency/reservations/"+reservation.ID+"/results",
		resultBody("fp1", `null`))
	if expired.Code != http.StatusConflict {
		t.Fatalf("expired result status = %d body = %s", expired.Code, expired.Body.String())
	}
	errBody := decodeError(t, expired.Body.String())
	if errBody["code"] != "idempotency_reservation_expired" ||
		errBody["reservation_id"] != reservation.ID {
		t.Fatalf("unexpected expired body: %v", errBody)
	}

	recreated := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations",
		reservationBody("k1", "fp1", time.Now().UTC().Add(time.Hour)))
	if recreated.Code != http.StatusCreated {
		t.Fatalf("new placeholder after expiry status = %d body = %s",
			recreated.Code, recreated.Body.String())
	}
}
