package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

func previewRouterWithStore(t *testing.T) (*store.Store, http.Handler) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st, NewRouter(st)
}

func assertNoOutcomeHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Header().Get("Idempotency-Outcome") != "" || response.Header().Get("Idempotency-Record-ID") != "" {
		t.Fatalf("preview must not set outcome headers: %v", response.Header())
	}
}

func TestPreviewCreatedReplayedConflict(t *testing.T) {
	_, handler := previewRouterWithStore(t)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records/preview",
		submitBody("k1", "fp1", `{"price":100}`, ""))
	if created.Code != http.StatusOK || created.Body.String() != `{"outcome":"created"}` {
		t.Fatalf("created preview = %d %s", created.Code, created.Body.String())
	}
	assertNoOutcomeHeaders(t, created)

	// The created preview must not have inserted anything.
	if lookup := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/k1", ""); lookup.Code != http.StatusNotFound {
		t.Fatalf("preview created a record: lookup = %d %s", lookup.Code, lookup.Body.String())
	}

	submitted := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records",
		submitBody("k1", "fp1", `{"price":100}`, ""))
	if submitted.Code != http.StatusOK {
		t.Fatalf("seed submit = %d %s", submitted.Code, submitted.Body.String())
	}
	recordEnvelope := submitted.Body.String()

	replayed := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records/preview",
		submitBody("k1", "fp1", `{"price":999}`, ""))
	if replayed.Code != http.StatusOK {
		t.Fatalf("replayed preview status = %d", replayed.Code)
	}
	if want := `{"outcome":"replayed",` + recordEnvelope[1:]; replayed.Body.String() != want {
		t.Fatalf("replayed preview = %s\nwant %s", replayed.Body.String(), want)
	}
	if strings.Contains(replayed.Body.String(), `"price":999`) {
		t.Fatalf("preview leaked the candidate snapshot: %s", replayed.Body.String())
	}
	assertNoOutcomeHeaders(t, replayed)

	conflict := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records/preview",
		submitBody("k1", "fp-other", `{"price":1}`, ""))
	if conflict.Code != http.StatusOK {
		t.Fatalf("conflict preview status = %d", conflict.Code)
	}
	var parsed struct {
		Outcome            string `json:"outcome"`
		RecordID           string `json:"record_id"`
		RequestFingerprint string `json:"request_fingerprint"`
	}
	if err := json.Unmarshal(conflict.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode conflict preview: %v", err)
	}
	if parsed.Outcome != "conflict" || parsed.RecordID == "" || parsed.RequestFingerprint != "fp1" {
		t.Fatalf("conflict preview payload = %+v", parsed)
	}
	if want := `{"outcome":"conflict","record_id":"` + parsed.RecordID + `","request_fingerprint":"fp1"}`; conflict.Body.String() != want {
		t.Fatalf("conflict preview body = %s\nwant %s", conflict.Body.String(), want)
	}
	if strings.Contains(conflict.Body.String(), "response_snapshot") {
		t.Fatalf("conflict preview leaked the snapshot: %s", conflict.Body.String())
	}
	assertNoOutcomeHeaders(t, conflict)
}

func TestPreviewPreservesSnapshotBytes(t *testing.T) {
	_, handler := previewRouterWithStore(t)

	body := `{"idempotency_key":"k2","request_fingerprint":"fp2","response_snapshot":{ "z" : [ 1 , 2 ] },"expires_at":"` +
		time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano) + `"}`
	if submitted := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", body); submitted.Code != http.StatusOK {
		t.Fatalf("seed submit = %d %s", submitted.Code, submitted.Body.String())
	}
	replayed := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records/preview",
		submitBody("k2", "fp2", `{"z":999}`, ""))
	if !strings.Contains(replayed.Body.String(), `"response_snapshot":{ "z" : [ 1 , 2 ] }`) {
		t.Fatalf("preview did not keep snapshot bytes verbatim: %s", replayed.Body.String())
	}
}

func TestPreviewConflictWritesNoEvent(t *testing.T) {
	_, handler := previewRouterWithStore(t)

	if submitted := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records",
		submitBody("k3", "fp1", `{}`, "")); submitted.Code != http.StatusOK {
		t.Fatalf("seed submit = %d", submitted.Code)
	}
	for i := 0; i < 3; i++ {
		preview := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records/preview",
			submitBody("k3", "fp-other", `{}`, ""))
		if preview.Code != http.StatusOK {
			t.Fatalf("preview %d = %d", i, preview.Code)
		}
	}
	events := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts", "")
	if events.Code != http.StatusOK || !strings.Contains(events.Body.String(), `"events":[]`) {
		t.Fatalf("preview wrote conflict events: %s", events.Body.String())
	}

	// The conflict is still fresh: the real submit still records its event exactly once.
	submitConflict := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records",
		submitBody("k3", "fp-other", `{}`, ""))
	if submitConflict.Code != http.StatusConflict {
		t.Fatalf("submit after preview = %d", submitConflict.Code)
	}
	events = doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts", "")
	if strings.Count(events.Body.String(), `"id":"con_`) != 1 {
		t.Fatalf("expected exactly one conflict event after submit: %s", events.Body.String())
	}
}

func TestPreviewExpiredRowTreatedAsCreated(t *testing.T) {
	st, handler := previewRouterWithStore(t)
	now := time.Now().UTC()

	id, err := store.NewRecordID()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}
	expired := store.Record{
		ID:                 id,
		IdempotencyKey:     "gone",
		RequestFingerprint: "alpha",
		ResponseSnapshot:   []byte(`"secret"`),
		CreatedAt:          now.Add(-2 * time.Hour),
		ExpiresAt:          now.Add(-time.Hour),
	}
	if _, _, err := st.PutRecord(context.Background(), expired, now); err != nil {
		t.Fatalf("seed expired row: %v", err)
	}

	preview := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records/preview",
		submitBody("gone", "alpha", `"new"`, ""))
	if preview.Code != http.StatusOK || preview.Body.String() != `{"outcome":"created"}` {
		t.Fatalf("expired row preview = %d %s", preview.Code, preview.Body.String())
	}
}

func TestPreviewValidationFailures(t *testing.T) {
	_, handler := previewRouterWithStore(t)
	cases := []struct {
		name string
		body string
	}{
		{"empty body", ""},
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
			response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records/preview", tc.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", response.Code, response.Body.String())
			}
			if !strings.HasPrefix(response.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
				t.Fatalf("body = %s", response.Body.String())
			}
		})
	}
}

func TestPreviewOmittedSnapshotDefaultsToNull(t *testing.T) {
	_, handler := previewRouterWithStore(t)
	body := `{"idempotency_key":"k","request_fingerprint":"fp","expires_at":"2099-01-01T00:00:00Z"}`
	response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records/preview", body)
	if response.Code != http.StatusOK || response.Body.String() != `{"outcome":"created"}` {
		t.Fatalf("omitted snapshot = %d %s", response.Code, response.Body.String())
	}
}

func TestPreviewStorageUnavailable(t *testing.T) {
	st, handler := previewRouterWithStore(t)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records/preview",
		submitBody("k", "fp", `{}`, ""))
	if response.Code != http.StatusServiceUnavailable ||
		!strings.HasPrefix(response.Body.String(), `{"error":{"code":"storage_unavailable",`) {
		t.Fatalf("storage failure = %d %s", response.Code, response.Body.String())
	}
}
