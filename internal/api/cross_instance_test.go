package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

// newSharedRouters builds n independent routers backed by one database file, i.e. n service
// instances sharing DB_PATH without any shared in-process state.
func newSharedRouters(t *testing.T, n int) ([]http.Handler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "shared.db")
	routers := make([]http.Handler, n)
	for i := range routers {
		st, err := store.Open(path)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		t.Cleanup(func() { st.Close() })
		routers[i] = NewRouter(st)
	}
	return routers, path
}

func submitBodyWithFingerprint(key, fingerprint string) string {
	expiresAt := time.Now().UTC().Add(time.Hour)
	return `{"idempotency_key":"` + key + `","request_fingerprint":"` + fingerprint +
		`","response_snapshot":{"price":100},"expires_at":"` + expiresAt.Format(time.RFC3339Nano) + `"}`
}

// TestCrossInstancesHTTPSameFingerprint exercises the full HTTP contract for N instances racing
// with identical requests: all 200, same record id, identical body, one visible row afterwards.
func TestCrossInstancesHTTPSameFingerprint(t *testing.T) {
	const instances = 6
	routers, _ := newSharedRouters(t, instances)
	body := submitBody("race-same", "fp", `{"price":100}`, "")

	type response struct {
		status int
		body   string
		id     string
	}
	responses := make([]response, instances)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, router := range routers {
		wg.Add(1)
		go func(i int, router http.Handler) {
			defer wg.Done()
			<-start
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/idempotency/records", bytes.NewBufferString(body))
			request.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, request)
			result := response{status: recorder.Code, body: recorder.Body.String()}
			if recorder.Code == http.StatusOK {
				var parsed struct {
					Record struct {
						ID string `json:"id"`
					} `json:"record"`
				}
				_ = json.Unmarshal(recorder.Body.Bytes(), &parsed)
				result.id = parsed.Record.ID
			}
			responses[i] = result
		}(i, router)
	}
	close(start)
	wg.Wait()

	winnerID := ""
	winnerBody := ""
	for i, result := range responses {
		if result.status != http.StatusOK {
			t.Fatalf("instance %d status = %d body = %s", i, result.status, result.body)
		}
		if winnerID == "" {
			winnerID = result.id
			winnerBody = result.body
		}
		if result.id != winnerID || result.body != winnerBody {
			t.Fatalf("instance %d body = %s, want winner %s", i, result.body, winnerBody)
		}
	}

	// Exactly one active row is visible, from every instance.
	for i, router := range routers {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet,
			"/v1/idempotency/records?key=race-same&limit=100", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("list on instance %d: %d %s", i, recorder.Code, recorder.Body.String())
		}
		if count := bytes.Count(recorder.Body.Bytes(), []byte(`"id":"rec_`)); count != 1 {
			t.Fatalf("instance %d sees %d visible rows, body = %s", i, count, recorder.Body.String())
		}
		if !bytes.Contains(recorder.Body.Bytes(), []byte(`"id":"`+winnerID+`"`)) {
			t.Fatalf("visible record is not the winner: %s", recorder.Body.String())
		}
	}
}

// TestCrossInstancesHTTPFingerprintConflict checks that when instances race with different
// fingerprints, exactly one wins with 200 and every other response is 409 carrying the winning
// record_id and fingerprint; the winning snapshot stays unchanged.
func TestCrossInstancesHTTPFingerprintConflict(t *testing.T) {
	const instances = 8
	routers, _ := newSharedRouters(t, instances)
	bodies := make([]string, instances)
	for i := range bodies {
		bodies[i] = submitBodyWithFingerprint("race-conflict", "fp-"+string(rune('a'+i)))
	}

	type response struct {
		status int
		body   string
	}
	responses := make([]response, instances)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, router := range routers {
		wg.Add(1)
		go func(i int, router http.Handler) {
			defer wg.Done()
			<-start
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/idempotency/records", bytes.NewBufferString(bodies[i]))
			request.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, request)
			responses[i] = response{status: recorder.Code, body: recorder.Body.String()}
		}(i, router)
	}
	close(start)
	wg.Wait()

	winners := 0
	var winnerID, winnerFingerprint string
	for i, result := range responses {
		switch result.status {
		case http.StatusOK:
			winners++
			var parsed struct {
				Record struct {
					ID                 string `json:"id"`
					RequestFingerprint string `json:"request_fingerprint"`
				} `json:"record"`
			}
			if err := json.Unmarshal([]byte(result.body), &parsed); err != nil {
				t.Fatalf("decode winner %d: %v", i, err)
			}
			winnerID = parsed.Record.ID
			winnerFingerprint = parsed.Record.RequestFingerprint
		case http.StatusConflict:
			if !bytes.HasPrefix([]byte(result.body), []byte(`{"error":{"code":"idempotency_fingerprint_conflict",`)) {
				t.Fatalf("instance %d conflict shape: %s", i, result.body)
			}
			var parsed struct {
				Error struct {
					Code               string `json:"code"`
					RecordID           string `json:"record_id"`
					RequestFingerprint string `json:"request_fingerprint"`
				} `json:"error"`
			}
			if err := json.Unmarshal([]byte(result.body), &parsed); err != nil {
				t.Fatalf("decode conflict %d: %v", i, err)
			}
			if winnerID != "" && (parsed.Error.RecordID != winnerID || parsed.Error.RequestFingerprint != winnerFingerprint) {
				t.Fatalf("instance %d conflict pointed at %s/%s, want %s/%s",
					i, parsed.Error.RecordID, parsed.Error.RequestFingerprint, winnerID, winnerFingerprint)
			}
		default:
			t.Fatalf("instance %d unexpected status %d body %s", i, result.status, result.body)
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want exactly 1", winners)
	}
	for _, result := range responses {
		if result.status == http.StatusConflict {
			if !bytes.Contains([]byte(result.body), []byte(`"record_id":"`+winnerID+`"`)) ||
				!bytes.Contains([]byte(result.body), []byte(`"request_fingerprint":"`+winnerFingerprint+`"`)) {
				t.Fatalf("conflict body missing winner identity: %s (winner %s/%s)",
					result.body, winnerID, winnerFingerprint)
			}
		}
	}

	recorder := httptest.NewRecorder()
	routers[0].ServeHTTP(recorder, httptest.NewRequest(http.MethodGet,
		"/v1/idempotency/records/race-conflict", nil))
	if recorder.Code != http.StatusOK ||
		!bytes.Contains(recorder.Body.Bytes(), []byte(`"id":"`+winnerID+`"`)) ||
		!bytes.Contains(recorder.Body.Bytes(), []byte(`"request_fingerprint":"`+winnerFingerprint+`"`)) ||
		!bytes.Contains(recorder.Body.Bytes(), []byte(`{"price":100}`)) {
		t.Fatalf("winning snapshot changed or missing: %s", recorder.Body.String())
	}
}

// TestCrossInstancesHTTPExpiryThenConcurrentResubmit races resubmission after the original row has
// expired and asserts one new active record while history is invisible to get and list.
func TestCrossInstancesHTTPExpiryThenConcurrentResubmit(t *testing.T) {
	const instances = 6
	routers, _ := newSharedRouters(t, instances)

	pastExpiry := time.Now().UTC().Add(-2 * time.Hour).Format(time.RFC3339Nano)
	seed := `{"idempotency_key":"renew","request_fingerprint":"fp","response_snapshot":{"v":"old"},"expires_at":"` + pastExpiry + `"}`
	_ = doJSON(t, routers[0], http.MethodPost, "/v1/idempotency/records", seed)

	if get := doJSON(t, routers[1], http.MethodGet, "/v1/idempotency/records/renew", ""); get.Code != http.StatusNotFound {
		t.Fatalf("expired record visible before resubmit: %d", get.Code)
	}

	body := submitBody("renew", "fp", `{"v":"new"}`, "")
	statuses := make([]int, instances)
	bodies := make([]string, instances)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, router := range routers {
		wg.Add(1)
		go func(i int, router http.Handler) {
			defer wg.Done()
			<-start
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/idempotency/records", bytes.NewBufferString(body))
			request.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, request)
			statuses[i] = recorder.Code
			bodies[i] = recorder.Body.String()
		}(i, router)
	}
	close(start)
	wg.Wait()

	okCount := 0
	winnerID := ""
	for i, status := range statuses {
		if status != http.StatusOK {
			t.Fatalf("instance %d status = %d body = %s", i, status, bodies[i])
		}
		okCount++
		var parsed struct {
			Record struct {
				ID string `json:"id"`
			} `json:"record"`
		}
		if err := json.Unmarshal([]byte(bodies[i]), &parsed); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if winnerID == "" {
			winnerID = parsed.Record.ID
		} else if parsed.Record.ID != winnerID || bodies[i] != bodies[firstOKIndex(statuses)] {
			t.Fatalf("instance %d replayed a different record", i)
		}
	}
	if okCount != instances {
		t.Fatalf("ok count = %d", okCount)
	}

	get := doJSON(t, routers[0], http.MethodGet, "/v1/idempotency/records/renew", "")
	if get.Code != http.StatusOK || !bytes.Contains(get.Body.Bytes(), []byte(`"v":"new"`)) {
		t.Fatalf("active record after renewal: %d %s", get.Code, get.Body.String())
	}
	list := doJSON(t, routers[0], http.MethodGet, "/v1/idempotency/records?key=renew&limit=100", "")
	if count := bytes.Count(list.Body.Bytes(), []byte(`"id":"rec_`)); count != 1 {
		t.Fatalf("visible renewed rows = %d: %s", count, list.Body.String())
	}
	if bytes.Contains(list.Body.Bytes(), []byte(`"v":"old"`)) {
		t.Fatalf("expired history leaked into listing: %s", list.Body.String())
	}
}

func firstOKIndex(statuses []int) int {
	for i, status := range statuses {
		if status == http.StatusOK {
			return i
		}
	}
	return 0
}

// TestCrossInstancesHistoryAuditIsReadOnlyAcrossInstances writes one expired generation through a
// direct store open on the shared DB_PATH (submission cannot create an already-expired row) and
// then asserts every instance can audit it read-only while the active endpoints keep hiding it.
func TestCrossInstancesHistoryAuditIsReadOnlyAcrossInstances(t *testing.T) {
	const instances = 4
	routers, path := newSharedRouters(t, instances)

	writer, err := store.Open(path)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	t.Cleanup(func() { writer.Close() })

	now := time.Now().UTC()
	old := store.Record{
		ID:                 "rec_" + strings.Repeat("a", 32),
		IdempotencyKey:     "audit-key",
		RequestFingerprint: "fp-old",
		ResponseSnapshot:   []byte(`{"v":"old"}`),
		CreatedAt:          now.Add(-2 * time.Hour),
		ExpiresAt:          now.Add(-time.Hour),
	}
	if _, _, err := writer.PutRecord(context.Background(), old, now.Add(-2*time.Hour)); err != nil {
		t.Fatalf("seed expired row: %v", err)
	}

	for i, router := range routers {
		history := doJSON(t, router, http.MethodGet, "/v1/idempotency/history?key=audit-key", "")
		if history.Code != http.StatusOK {
			t.Fatalf("instance %d history status = %d %s", i, history.Code, history.Body.String())
		}
		if !bytes.Contains(history.Body.Bytes(), []byte(`"v":"old"`)) ||
			!bytes.Contains(history.Body.Bytes(), []byte(`"status":"expired"`)) {
			t.Fatalf("instance %d did not audit the shared history: %s", i, history.Body.String())
		}
		if bytes.Count(history.Body.Bytes(), []byte(`"id":"rec_`)) != 1 {
			t.Fatalf("instance %d history row count wrong: %s", i, history.Body.String())
		}

		active := doJSON(t, router, http.MethodGet, "/v1/idempotency/records/audit-key", "")
		if active.Code != http.StatusNotFound {
			t.Fatalf("instance %d leaked expired snapshot via active lookup: %d %s", i, active.Code, active.Body.String())
		}
	}

	// Querying history must not create placeholder rows: the key still 404s on the active path and
	// the history stays one row.
	for _, router := range routers {
		_ = doJSON(t, router, http.MethodGet, "/v1/idempotency/history?key=never-seen", "")
	}
	again := doJSON(t, routers[0], http.MethodGet, "/v1/idempotency/history?key=audit-key", "")
	if bytes.Count(again.Body.Bytes(), []byte(`"id":"rec_`)) != 1 {
		t.Fatalf("read-only audit changed row count: %s", again.Body.String())
	}
	if missing := doJSON(t, routers[0], http.MethodGet, "/v1/idempotency/history?key=never-seen", ""); missing.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("unknown key created placeholder data: %s", missing.Body.String())
	}
}
