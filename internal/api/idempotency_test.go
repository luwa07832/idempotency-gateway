package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

func newAPIRouter(t *testing.T) http.Handler {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewRouter(st)
}

func doJSON(t *testing.T, handler http.Handler, method, target string, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func submitBody(key, fingerprint, snapshot, expiresIn string) string {
	expiresAt := time.Now().UTC().Add(1 * time.Hour)
	if expiresIn == "past" {
		expiresAt = time.Now().UTC().Add(-time.Hour)
	}
	return `{"idempotency_key":"` + key + `","request_fingerprint":"` + fingerprint + `","response_snapshot":` + snapshot + `,"expires_at":"` + expiresAt.Format(time.RFC3339Nano) + `"}`
}

func TestSubmitCreateReplayAndConflict(t *testing.T) {
	handler := newAPIRouter(t)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{"price":100}`, ""))
	if created.Code != http.StatusOK {
		t.Fatalf("create status = %d body = %s", created.Code, created.Body.String())
	}
	createdBody := created.Body.String()
	if want := `{"record":{"id":"rec_`; !strings.HasPrefix(createdBody, want) {
		t.Fatalf("unexpected create body: %s", createdBody)
	}
	if !strings.Contains(createdBody, `"status":"active"`) || !strings.Contains(createdBody, `"response_snapshot":{"price":100}`) {
		t.Fatalf("create body missing fixed fields: %s", createdBody)
	}
	var createdParsed struct {
		Record struct {
			ID string `json:"id"`
		} `json:"record"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createdParsed); err != nil {
		t.Fatalf("decode: %v", err)
	}

	replayed := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{"price":999}`, ""))
	if replayed.Code != http.StatusOK {
		t.Fatalf("replay status = %d", replayed.Code)
	}
	if replayed.Body.String() != createdBody {
		t.Fatalf("replay body = %s\nwant first response %s", replayed.Body.String(), createdBody)
	}
	if !strings.Contains(replayed.Body.String(), `"price":100`) {
		t.Fatalf("replay exposed a later snapshot: %s", replayed.Body.String())
	}

	conflict := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp-other", `{"price":1}`, ""))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body = %s", conflict.Code, conflict.Body.String())
	}
	conflictBody := conflict.Body.String()
	if !strings.HasPrefix(conflictBody, `{"error":{"code":"idempotency_fingerprint_conflict",`) {
		t.Fatalf("conflict body shape: %s", conflictBody)
	}
	if !strings.Contains(conflictBody, `"record_id":"`+createdParsed.Record.ID+`"`) ||
		!strings.Contains(conflictBody, `"request_fingerprint":"fp1"`) {
		t.Fatalf("conflict body missing original identity: %s", conflictBody)
	}
}

func TestSubmitValidationFailuresDoNotWrite(t *testing.T) {
	handler := newAPIRouter(t)
	cases := []struct {
		name string
		body string
	}{
		{"missing key", `{"request_fingerprint":"fp","response_snapshot":{},"expires_at":"2099-01-01T00:00:00Z"}`},
		{"blank key", `{"idempotency_key":"  ","request_fingerprint":"fp","response_snapshot":{},"expires_at":"2099-01-01T00:00:00Z"}`},
		{"missing fingerprint", `{"idempotency_key":"k","response_snapshot":{},"expires_at":"2099-01-01T00:00:00Z"}`},
		{"missing expires_at", `{"idempotency_key":"k","request_fingerprint":"fp","response_snapshot":{}}`},
		{"bad expires_at", `{"idempotency_key":"k","request_fingerprint":"fp","response_snapshot":{},"expires_at":"not-a-time"}`},
		{"expires in the past", submitBody("k", "fp", `{}`, "past")},
		{"invalid snapshot json", `{"idempotency_key":"k","request_fingerprint":"fp","response_snapshot":{"oops"},"expires_at":"2099-01-01T00:00:00Z"}`},
		{"not an object", `[]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", tc.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", response.Code, response.Body.String())
			}
			if !strings.HasPrefix(response.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
				t.Fatalf("body = %s", response.Body.String())
			}
		})
	}

	missing := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/k", "")
	if missing.Code != http.StatusNotFound || !strings.HasPrefix(missing.Body.String(), `{"error":{"code":"not_found",`) {
		t.Fatalf("missing key lookup = %d %s", missing.Code, missing.Body.String())
	}
}

func TestGetByKeyReturnsRecordAndHidesExpired(t *testing.T) {
	handler := newAPIRouter(t)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("lookup", "fp", `[1,2,3]`, ""))
	var createdParsed struct {
		Record struct {
			ID string `json:"id"`
		} `json:"record"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createdParsed); err != nil {
		t.Fatalf("decode: %v", err)
	}

	found := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/lookup", "")
	if found.Code != http.StatusOK {
		t.Fatalf("get status = %d body = %s", found.Code, found.Body.String())
	}
	if found.Body.String() != created.Body.String() {
		t.Fatalf("get body = %s\nwant %s", found.Body.String(), created.Body.String())
	}

	past := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	body := `{"idempotency_key":"old","request_fingerprint":"fp","response_snapshot":"secret","expires_at":"` + past + `"}`
	rejected := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", body)
	if rejected.Code != http.StatusBadRequest {
		t.Fatalf("expired submission status = %d", rejected.Code)
	}
	if hidden := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/old", ""); hidden.Code != http.StatusNotFound {
		t.Fatalf("expired key status = %d body = %s", hidden.Code, hidden.Body.String())
	}
}

func TestListOrdersFiltersAndPaginates(t *testing.T) {
	handler := newAPIRouter(t)
	for _, key := range []string{"a", "b", "c"} {
		response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("key-"+key, "fp", `{"n":1}`, ""))
		if response.Code != http.StatusOK {
			t.Fatalf("seed %s: %d %s", key, response.Code, response.Body.String())
		}
	}

	page1 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?limit=2", "")
	if page1.Code != http.StatusOK {
		t.Fatalf("list status = %d", page1.Code)
	}
	var list1 struct {
		Records []struct {
			IdempotencyKey string `json:"idempotency_key"`
		} `json:"records"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(page1.Body.Bytes(), &list1); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(list1.Records) != 2 || list1.NextCursor == "" {
		t.Fatalf("page1 = %+v", list1)
	}
	if list1.Records[0].IdempotencyKey != "key-c" || list1.Records[1].IdempotencyKey != "key-b" {
		t.Fatalf("page1 order = %+v", list1.Records)
	}

	page2 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?limit=2&cursor="+list1.NextCursor, "")
	var list2 struct {
		Records []struct {
			IdempotencyKey string `json:"idempotency_key"`
		} `json:"records"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(page2.Body.Bytes(), &list2); err != nil {
		t.Fatalf("decode page2: %v", err)
	}
	if len(list2.Records) != 1 || list2.Records[0].IdempotencyKey != "key-a" || list2.NextCursor != "" {
		t.Fatalf("page2 = %+v", list2)
	}

	byKey := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?key=key-b", "")
	if strings.Count(byKey.Body.String(), `"idempotency_key"`) != 1 || !strings.Contains(byKey.Body.String(), "key-b") {
		t.Fatalf("key filter mixed records: %s", byKey.Body.String())
	}
}

func TestListInvalidCursorAndExpiredStatus(t *testing.T) {
	handler := newAPIRouter(t)
	doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp", `{}`, ""))

	badCursor := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?cursor=not-base64!!", "")
	if badCursor.Code != http.StatusBadRequest ||
		!strings.HasPrefix(badCursor.Body.String(), `{"error":{"code":"invalid_cursor",`) {
		t.Fatalf("invalid cursor = %d %s", badCursor.Code, badCursor.Body.String())
	}

	tampered := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?cursor=eyJpZCI6InJlY194In0", "")
	if tampered.Code != http.StatusBadRequest ||
		!strings.HasPrefix(tampered.Body.String(), `{"error":{"code":"invalid_cursor",`) {
		t.Fatalf("tampered cursor = %d %s", tampered.Code, tampered.Body.String())
	}

	expired := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?status=expired", "")
	if expired.Code != http.StatusOK || expired.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("expired listing = %d %s", expired.Code, expired.Body.String())
	}

	badStatus := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?status=weird", "")
	if badStatus.Code != http.StatusBadRequest ||
		!strings.HasPrefix(badStatus.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
		t.Fatalf("bad status = %d %s", badStatus.Code, badStatus.Body.String())
	}

	badLimit := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?limit=0", "")
	if badLimit.Code != http.StatusBadRequest ||
		!strings.HasPrefix(badLimit.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
		t.Fatalf("bad limit = %d %s", badLimit.Code, badLimit.Body.String())
	}
}

func TestRepeatedCallsAreByteIdentical(t *testing.T) {
	handler := newAPIRouter(t)
	first := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("stable", "fp", `{"z":1,"a":2}`, ""))
	second := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("stable", "fp", `{"z":9,"a":9}`, ""))
	if first.Body.String() != second.Body.String() {
		t.Fatalf("repeated calls differ:\n%s\n%s", first.Body.String(), second.Body.String())
	}
}

func TestSubmitRejectsExpiryNotStrictlyAfterNow(t *testing.T) {
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

	equal := `{"idempotency_key":"k","request_fingerprint":"fp","response_snapshot":{},"expires_at":"` + now.Format(time.RFC3339Nano) + `"}`
	if _, message, ok := validateSubmitRequest([]byte(equal), now); ok {
		t.Fatalf("expiry equal to now accepted")
	} else if message == "" {
		t.Fatalf("equal expiry returned no fixed message")
	}

	past := `{"idempotency_key":"k","request_fingerprint":"fp","response_snapshot":{},"expires_at":"` + now.Add(-time.Second).Format(time.RFC3339Nano) + `"}`
	if _, _, ok := validateSubmitRequest([]byte(past), now); ok {
		t.Fatalf("expiry earlier than now accepted")
	}

	future := `{"idempotency_key":"k","request_fingerprint":"fp","response_snapshot":{"v":1},"expires_at":"` + now.Add(time.Second).Format(time.RFC3339Nano) + `"}`
	validated, _, ok := validateSubmitRequest([]byte(future), now)
	if !ok {
		t.Fatalf("expiry later than now rejected")
	}
	if string(validated.responseSnapshot) != `{"v":1}` || !validated.expiresAt.Equal(now.Add(time.Second)) {
		t.Fatalf("validated request = %+v", validated)
	}
}

// newSharedRouters returns two HTTP handlers backed by two Store instances opened on the
// same database file, simulating a multi-instance deployment on one DB_PATH.
func newSharedRouters(t *testing.T) []http.Handler {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shared.db")
	handlers := make([]http.Handler, 0, 2)
	for i := 0; i < 2; i++ {
		st, err := store.Open(path)
		if err != nil {
			t.Fatalf("open instance %d: %v", i, err)
		}
		t.Cleanup(func() { st.Close() })
		handlers = append(handlers, NewRouter(st))
	}
	return handlers
}

func TestSharedFileConcurrentSubmitsReturnIdenticalRecord(t *testing.T) {
	handlers := newSharedRouters(t)

	const clients = 16
	codes := make([]int, clients)
	bodies := make([]string, clients)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			request := httptest.NewRequest(http.MethodPost, "/v1/idempotency/records",
				bytes.NewBufferString(submitBody("shared", "fp", `{"price":100}`, "")))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			handlers[i%len(handlers)].ServeHTTP(recorder, request)
			codes[i], bodies[i] = recorder.Code, recorder.Body.String()
		}(i)
	}
	close(start)
	wg.Wait()

	for i := range codes {
		if codes[i] != http.StatusOK {
			t.Fatalf("client %d status = %d body = %s", i, codes[i], bodies[i])
		}
		if bodies[i] != bodies[0] {
			t.Fatalf("client %d body = %s, want identical replay %s", i, bodies[i], bodies[0])
		}
	}

	list := doJSON(t, handlers[0], http.MethodGet, "/v1/idempotency/records?key=shared", "")
	if got := strings.Count(list.Body.String(), `"idempotency_key"`); got != 1 {
		t.Fatalf("list shows %d records, want exactly 1: %s", got, list.Body.String())
	}
}

func TestSharedFileConcurrentFingerprintConflict(t *testing.T) {
	handlers := newSharedRouters(t)

	const clients = 16
	codes := make([]int, clients)
	bodies := make([]string, clients)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			snapshot := fmt.Sprintf(`{"n":%d}`, i)
			request := httptest.NewRequest(http.MethodPost, "/v1/idempotency/records",
				bytes.NewBufferString(submitBody("shared-conflict", fmt.Sprintf("fp-%d", i), snapshot, "")))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			handlers[i%len(handlers)].ServeHTTP(recorder, request)
			codes[i], bodies[i] = recorder.Code, recorder.Body.String()
		}(i)
	}
	close(start)
	wg.Wait()

	winnerBody := ""
	winnerID := ""
	winnerFingerprint := ""
	for i := range codes {
		switch codes[i] {
		case http.StatusOK:
			if winnerBody != "" {
				t.Fatalf("client %d also returned 200; only one submission may win", i)
			}
			winnerBody = bodies[i]
			var parsed struct {
				Record struct {
					ID                 string `json:"id"`
					RequestFingerprint string `json:"request_fingerprint"`
				} `json:"record"`
			}
			if err := json.Unmarshal([]byte(bodies[i]), &parsed); err != nil {
				t.Fatalf("decode winner: %v", err)
			}
			winnerID = parsed.Record.ID
			winnerFingerprint = parsed.Record.RequestFingerprint
		case http.StatusConflict:
			if !strings.HasPrefix(bodies[i], `{"error":{"code":"idempotency_fingerprint_conflict",`) {
				t.Fatalf("client %d conflict body shape: %s", i, bodies[i])
			}
		default:
			t.Fatalf("client %d status = %d body = %s", i, codes[i], bodies[i])
		}
	}
	if winnerBody == "" {
		t.Fatalf("no submission won")
	}
	for i := range codes {
		if codes[i] != http.StatusConflict {
			continue
		}
		if !strings.Contains(bodies[i], `"record_id":"`+winnerID+`"`) ||
			!strings.Contains(bodies[i], `"request_fingerprint":"`+winnerFingerprint+`"`) {
			t.Fatalf("client %d conflict body does not report the winner: %s", i, bodies[i])
		}
	}

	lookup := doJSON(t, handlers[1], http.MethodGet, "/v1/idempotency/records/shared-conflict", "")
	if lookup.Code != http.StatusOK || lookup.Body.String() != winnerBody {
		t.Fatalf("first snapshot not preserved: %d %s, want %s", lookup.Code, lookup.Body.String(), winnerBody)
	}
	list := doJSON(t, handlers[1], http.MethodGet, "/v1/idempotency/records?key=shared-conflict", "")
	if got := strings.Count(list.Body.String(), `"idempotency_key"`); got != 1 {
		t.Fatalf("list shows %d records, want exactly 1: %s", got, list.Body.String())
	}
}
