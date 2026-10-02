package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

func reservationBody(key, fingerprint, expiresIn string) string {
	expiresAt := time.Now().UTC().Add(time.Hour)
	switch expiresIn {
	case "past":
		expiresAt = time.Now().UTC().Add(-time.Hour)
	case "short":
		expiresAt = time.Now().UTC().Add(60 * time.Millisecond)
	}
	return `{"idempotency_key":"` + key + `","request_fingerprint":"` + fingerprint + `","expires_at":"` + expiresAt.Format(time.RFC3339Nano) + `"}`
}

func createReservation(t *testing.T, handler http.Handler, key, fingerprint string) (reservationID string, body string) {
	t.Helper()
	response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations", reservationBody(key, fingerprint, ""))
	if response.Code != http.StatusCreated {
		t.Fatalf("create reservation status = %d body = %s", response.Code, response.Body.String())
	}
	var parsed struct {
		Reservation struct {
			ID string `json:"id"`
		} `json:"reservation"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode reservation: %v", err)
	}
	return parsed.Reservation.ID, response.Body.String()
}

func resultBody(fingerprint, snapshot string) string {
	return `{"request_fingerprint":"` + fingerprint + `","response_snapshot":` + snapshot + `}`
}

func TestReservationCreateReplayAndConflict(t *testing.T) {
	handler := newAPIRouter(t)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations", reservationBody("k1", "fp1", ""))
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d body = %s", created.Code, created.Body.String())
	}
	if outcome := created.Header().Get("Idempotency-Outcome"); outcome != "created" {
		t.Fatalf("create outcome header = %q", outcome)
	}
	createdBody := created.Body.String()
	if !strings.HasPrefix(createdBody, `{"reservation":{"id":"res_`) {
		t.Fatalf("unexpected create body: %s", createdBody)
	}
	if !strings.Contains(createdBody, `"idempotency_key":"k1","request_fingerprint":"fp1","status":"pending","created_at":"`) {
		t.Fatalf("create body missing fixed fields: %s", createdBody)
	}
	var createdParsed struct {
		Reservation struct {
			ID string `json:"id"`
		} `json:"reservation"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createdParsed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !store.IsReservationID(createdParsed.Reservation.ID) {
		t.Fatalf("reservation id shape: %s", createdParsed.Reservation.ID)
	}

	replayed := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations", reservationBody("k1", "fp1", ""))
	if replayed.Code != http.StatusOK {
		t.Fatalf("replay status = %d body = %s", replayed.Code, replayed.Body.String())
	}
	if outcome := replayed.Header().Get("Idempotency-Outcome"); outcome != "replayed" {
		t.Fatalf("replay outcome header = %q", outcome)
	}
	if replayed.Body.String() != createdBody {
		t.Fatalf("replay body = %s\nwant %s", replayed.Body.String(), createdBody)
	}

	conflict := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations", reservationBody("k1", "fp-other", ""))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body = %s", conflict.Code, conflict.Body.String())
	}
	conflictBody := conflict.Body.String()
	if !strings.HasPrefix(conflictBody, `{"error":{"code":"idempotency_fingerprint_conflict",`) {
		t.Fatalf("conflict body shape: %s", conflictBody)
	}
	if !strings.Contains(conflictBody, `"reservation_id":"`+createdParsed.Reservation.ID+`"`) ||
		!strings.Contains(conflictBody, `"request_fingerprint":"fp1"`) {
		t.Fatalf("conflict body missing stored identity: %s", conflictBody)
	}
	if outcome := conflict.Header().Get("Idempotency-Outcome"); outcome != "" {
		t.Fatalf("conflict must not carry an outcome header, got %q", outcome)
	}
}

func TestReservationValidationFailuresDoNotWrite(t *testing.T) {
	handler := newAPIRouter(t)
	cases := []struct {
		name string
		body string
	}{
		{"missing key", `{"request_fingerprint":"fp","expires_at":"2099-01-01T00:00:00Z"}`},
		{"blank key", `{"idempotency_key":"  ","request_fingerprint":"fp","expires_at":"2099-01-01T00:00:00Z"}`},
		{"missing fingerprint", `{"idempotency_key":"k","expires_at":"2099-01-01T00:00:00Z"}`},
		{"missing expires_at", `{"idempotency_key":"k","request_fingerprint":"fp"}`},
		{"bad expires_at", `{"idempotency_key":"k","request_fingerprint":"fp","expires_at":"not-a-time"}`},
		{"expires in the past", reservationBody("k", "fp", "past")},
		{"not an object", `[]`},
		{"empty body", ``},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations", tc.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", response.Code, response.Body.String())
			}
			if !strings.HasPrefix(response.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
				t.Fatalf("body = %s", response.Body.String())
			}
		})
	}

	// None of the rejected attempts owns the key: the first valid request still creates.
	response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations", reservationBody("k", "fp", ""))
	if response.Code != http.StatusCreated || response.Header().Get("Idempotency-Outcome") != "created" {
		t.Fatalf("first valid create = %d %s", response.Code, response.Body.String())
	}
}

func TestSubmitResultCreatesRecordAndReplays(t *testing.T) {
	handler := newAPIRouter(t)
	reservationID, reservationBody := createReservation(t, handler, "k1", "fp1")

	var reservationParsed struct {
		Reservation struct {
			CreatedAt string `json:"created_at"`
			ExpiresAt string `json:"expires_at"`
		} `json:"reservation"`
	}
	if err := json.Unmarshal([]byte(reservationBody), &reservationParsed); err != nil {
		t.Fatalf("decode reservation: %v", err)
	}

	result := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations/"+reservationID+"/results", resultBody("fp1", `{"price":100}`))
	if result.Code != http.StatusOK {
		t.Fatalf("first result status = %d body = %s", result.Code, result.Body.String())
	}
	if outcome := result.Header().Get("Idempotency-Outcome"); outcome != "created" {
		t.Fatalf("first result outcome = %q", outcome)
	}
	resultBodyText := result.Body.String()
	if !strings.HasPrefix(resultBodyText, `{"record":{"id":"rec_`) {
		t.Fatalf("unexpected result body: %s", resultBodyText)
	}
	if !strings.Contains(resultBodyText, `"status":"active"`) || !strings.Contains(resultBodyText, `"response_snapshot":{"price":100}`) {
		t.Fatalf("result body missing fixed fields: %s", resultBodyText)
	}
	// The record's timestamps are the placeholder's.
	if !strings.Contains(resultBodyText, `"created_at":"`+reservationParsed.Reservation.CreatedAt+`"`) ||
		!strings.Contains(resultBodyText, `"expires_at":"`+reservationParsed.Reservation.ExpiresAt+`"`) {
		t.Fatalf("record times do not follow the placeholder: %s vs %s", resultBodyText, reservationBody)
	}
	var resultParsed struct {
		Record struct {
			ID string `json:"id"`
		} `json:"record"`
	}
	if err := json.Unmarshal(result.Body.Bytes(), &resultParsed); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if recordIDHeader := result.Header().Get("Idempotency-Record-ID"); recordIDHeader != resultParsed.Record.ID {
		t.Fatalf("record id header = %q, want %s", recordIDHeader, resultParsed.Record.ID)
	}

	// The materialized record is visible through the existing record endpoints.
	byKey := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/k1", "")
	if byKey.Code != http.StatusOK || byKey.Body.String() != resultBodyText {
		t.Fatalf("get by key = %d %s, want the result record", byKey.Code, byKey.Body.String())
	}
	byID := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records-by-id/"+resultParsed.Record.ID, "")
	if byID.Code != http.StatusOK || byID.Body.String() != resultBodyText {
		t.Fatalf("get by id = %d %s, want the result record", byID.Code, byID.Body.String())
	}

	// A repeated result replays the first record byte-for-byte.
	replay := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations/"+reservationID+"/results", resultBody("fp1", `{"price":999}`))
	if replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d body = %s", replay.Code, replay.Body.String())
	}
	if outcome := replay.Header().Get("Idempotency-Outcome"); outcome != "replayed" {
		t.Fatalf("replay outcome = %q", outcome)
	}
	if replay.Body.String() != resultBodyText {
		t.Fatalf("replay body = %s\nwant %s", replay.Body.String(), resultBodyText)
	}
}

func TestSubmitResultErrors(t *testing.T) {
	handler := newAPIRouter(t)
	reservationID, _ := createReservation(t, handler, "k1", "fp1")

	for _, badID := range []string{"abc", "rec_00000000000000000000000000000000", "res_0000000000000000000000000000000", "res_0000000000000000000000000000000g"} {
		response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations/"+badID+"/results", resultBody("fp1", `{}`))
		if response.Code != http.StatusBadRequest ||
			!strings.HasPrefix(response.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
			t.Fatalf("bad id %q = %d %s", badID, response.Code, response.Body.String())
		}
	}

	unknown := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations/res_00000000000000000000000000000000/results", resultBody("fp1", `{}`))
	if unknown.Code != http.StatusNotFound ||
		!strings.HasPrefix(unknown.Body.String(), `{"error":{"code":"not_found",`) {
		t.Fatalf("unknown id = %d %s", unknown.Code, unknown.Body.String())
	}

	conflict := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations/"+reservationID+"/results", resultBody("fp-other", `{}`))
	if conflict.Code != http.StatusConflict ||
		!strings.HasPrefix(conflict.Body.String(), `{"error":{"code":"idempotency_fingerprint_conflict",`) {
		t.Fatalf("fingerprint conflict = %d %s", conflict.Code, conflict.Body.String())
	}
	if !strings.Contains(conflict.Body.String(), `"reservation_id":"`+reservationID+`"`) ||
		!strings.Contains(conflict.Body.String(), `"request_fingerprint":"fp1"`) {
		t.Fatalf("conflict body missing stored identity: %s", conflict.Body.String())
	}

	emptyFingerprint := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations/"+reservationID+"/results", `{"response_snapshot":{}}`)
	if emptyFingerprint.Code != http.StatusBadRequest ||
		!strings.HasPrefix(emptyFingerprint.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
		t.Fatalf("empty fingerprint = %d %s", emptyFingerprint.Code, emptyFingerprint.Body.String())
	}

	badSnapshot := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations/"+reservationID+"/results", `{"request_fingerprint":"fp1","response_snapshot":{"oops"}}`)
	if badSnapshot.Code != http.StatusBadRequest ||
		!strings.HasPrefix(badSnapshot.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
		t.Fatalf("bad snapshot = %d %s", badSnapshot.Code, badSnapshot.Body.String())
	}

	// None of the failures produced a result: the first valid submission still creates.
	first := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations/"+reservationID+"/results", resultBody("fp1", `{"ok":true}`))
	if first.Code != http.StatusOK || first.Header().Get("Idempotency-Outcome") != "created" {
		t.Fatalf("first valid result = %d %s", first.Code, first.Body.String())
	}
}

func TestReservationExpiresWithoutResult(t *testing.T) {
	handler := newAPIRouter(t)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations", reservationBody("k1", "fp1", "short"))
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d body = %s", created.Code, created.Body.String())
	}
	var parsed struct {
		Reservation struct {
			ID string `json:"id"`
		} `json:"reservation"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	reservationID := parsed.Reservation.ID

	time.Sleep(150 * time.Millisecond)

	expired := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations/"+reservationID+"/results", resultBody("fp1", `{}`))
	if expired.Code != http.StatusConflict ||
		!strings.HasPrefix(expired.Body.String(), `{"error":{"code":"idempotency_reservation_expired",`) {
		t.Fatalf("expired result = %d %s", expired.Code, expired.Body.String())
	}

	// The expired placeholder no longer owns the key: a fresh one is created for it.
	fresh := doJSON(t, handler, http.MethodPost, "/v1/idempotency/reservations", reservationBody("k1", "fp1", ""))
	if fresh.Code != http.StatusCreated || fresh.Header().Get("Idempotency-Outcome") != "created" {
		t.Fatalf("fresh reservation = %d %s", fresh.Code, fresh.Body.String())
	}
	if strings.Contains(fresh.Body.String(), `"id":"`+reservationID+`"`) {
		t.Fatalf("fresh reservation reused the expired id: %s", fresh.Body.String())
	}
}

// TestCrossInstancesConcurrentReservations races N instances sharing one DB_PATH through the
// full HTTP contract: exactly one placeholder is created for the key, every other request
// replays it, and exactly one first result is stored while the rest replay the winning record.
func TestCrossInstancesConcurrentReservations(t *testing.T) {
	const instances = 6
	routers, _ := newSharedRouters(t, instances)
	body := reservationBody("race-key", "fp", "")

	type createResponse struct {
		status  int
		id      string
		outcome string
	}
	creates := make([]createResponse, instances)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, router := range routers {
		wg.Add(1)
		go func(i int, router http.Handler) {
			defer wg.Done()
			<-start
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/idempotency/reservations", bytes.NewBufferString(body))
			request.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, request)
			result := createResponse{status: recorder.Code, outcome: recorder.Header().Get("Idempotency-Outcome")}
			if recorder.Code == http.StatusCreated || recorder.Code == http.StatusOK {
				var parsed struct {
					Reservation struct {
						ID string `json:"id"`
					} `json:"reservation"`
				}
				_ = json.Unmarshal(recorder.Body.Bytes(), &parsed)
				result.id = parsed.Reservation.ID
			}
			creates[i] = result
		}(i, router)
	}
	close(start)
	wg.Wait()

	winnerID := ""
	createdCount := 0
	for i, result := range creates {
		switch result.status {
		case http.StatusCreated:
			createdCount++
			winnerID = result.id
			if result.outcome != "created" {
				t.Fatalf("instance %d create outcome = %q", i, result.outcome)
			}
		case http.StatusOK:
			if result.outcome != "replayed" {
				t.Fatalf("instance %d replay outcome = %q", i, result.outcome)
			}
		default:
			t.Fatalf("instance %d create status = %d", i, result.status)
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want exactly 1", createdCount)
	}
	for i, result := range creates {
		if result.id != winnerID {
			t.Fatalf("instance %d saw reservation %s, want winner %s", i, result.id, winnerID)
		}
	}

	type resultResponse struct {
		status   int
		recordID string
		outcome  string
		snapshot string
	}
	results := make([]resultResponse, instances)
	start = make(chan struct{})
	for i, router := range routers {
		wg.Add(1)
		go func(i int, router http.Handler) {
			defer wg.Done()
			<-start
			snapshot := `{"attempt":` + strings.Repeat("1", i+1) + `}`
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/idempotency/reservations/"+winnerID+"/results",
				bytes.NewBufferString(resultBody("fp", snapshot)))
			request.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, request)
			result := resultResponse{status: recorder.Code, outcome: recorder.Header().Get("Idempotency-Outcome")}
			if recorder.Code == http.StatusOK {
				var parsed struct {
					Record struct {
						ID               string          `json:"id"`
						ResponseSnapshot json.RawMessage `json:"response_snapshot"`
					} `json:"record"`
				}
				_ = json.Unmarshal(recorder.Body.Bytes(), &parsed)
				result.recordID = parsed.Record.ID
				result.snapshot = string(parsed.Record.ResponseSnapshot)
				if header := recorder.Header().Get("Idempotency-Record-ID"); header != result.recordID {
					t.Errorf("instance %d record-id header = %q, want %s", i, header, result.recordID)
				}
			}
			results[i] = result
		}(i, router)
	}
	close(start)
	wg.Wait()

	winnerRecordID := ""
	winnerSnapshot := ""
	resultCreatedCount := 0
	for i, result := range results {
		if result.status != http.StatusOK {
			t.Fatalf("instance %d result status = %d", i, result.status)
		}
		if result.outcome == "created" {
			resultCreatedCount++
			winnerRecordID = result.recordID
			winnerSnapshot = result.snapshot
		} else if result.outcome != "replayed" {
			t.Fatalf("instance %d result outcome = %q", i, result.outcome)
		}
	}
	if resultCreatedCount != 1 {
		t.Fatalf("result created count = %d, want exactly 1", resultCreatedCount)
	}
	for i, result := range results {
		if result.recordID != winnerRecordID || result.snapshot != winnerSnapshot {
			t.Fatalf("instance %d saw record %s snapshot %s, want %s %s",
				i, result.recordID, result.snapshot, winnerRecordID, winnerSnapshot)
		}
	}
}
