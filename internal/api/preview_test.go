package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

const previewPath = "/v1/idempotency/records/preview"

// previewOutcome parses the preview envelope's outcome and record id.
func previewOutcome(t *testing.T, response *httptest.ResponseRecorder) (string, string) {
	t.Helper()
	var parsed struct {
		Outcome string `json:"outcome"`
		Record  *struct {
			ID string `json:"id"`
		} `json:"record"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode preview body: %v: %s", err, response.Body.String())
	}
	id := ""
	if parsed.Record != nil {
		id = parsed.Record.ID
	}
	return parsed.Outcome, id
}

func TestPreviewCreatedWhenKeyUnknown(t *testing.T) {
	handler := newAPIRouter(t)

	response := doJSON(t, handler, http.MethodPost, previewPath, submitBody("k-new", "fp", `{"price":100}`, ""))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", response.Code, response.Body.String())
	}
	if body := response.Body.String(); body != `{"outcome":"created"}` {
		t.Fatalf("body = %s", body)
	}
	if response.Header().Get(headerOutcome) != "" || response.Header().Get(headerRecordID) != "" {
		t.Fatalf("preview must not emit outcome headers: %v", response.Header())
	}

	// The preview wrote nothing: the key still has no active record.
	if lookup := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/k-new", ""); lookup.Code != http.StatusNotFound {
		t.Fatalf("preview created a record, lookup status = %d", lookup.Code)
	}
}

func TestPreviewReplayedMatchesStoredRecord(t *testing.T) {
	handler := newAPIRouter(t)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{"price": 100 }`, ""))
	if created.Code != http.StatusOK {
		t.Fatalf("seed status = %d", created.Code)
	}

	// A different snapshot in the preview body must be ignored: the stored first snapshot and its
	// exact bytes (including insignificant whitespace) come back unchanged.
	response := doJSON(t, handler, http.MethodPost, previewPath, submitBody("k1", "fp1", `{"price":999}`, ""))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	if !strings.HasPrefix(body, `{"outcome":"replayed","record":`) {
		t.Fatalf("body = %s", body)
	}
	if !strings.Contains(body, `"response_snapshot":{"price": 100 }`) {
		t.Fatalf("preview did not keep the first snapshot bytes: %s", body)
	}
	if strings.Contains(body, `"price":999`) {
		t.Fatalf("preview leaked the candidate snapshot: %s", body)
	}

	var preview struct {
		Record recordResponse `json:"record"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &preview); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if preview.Record.Status != "active" || preview.Record.IdempotencyKey != "k1" ||
		preview.Record.RequestFingerprint != "fp1" {
		t.Fatalf("preview record fields wrong: %+v", preview.Record)
	}

	// The embedded record object is byte-for-byte the one the GET entry returns.
	lookup := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/k1", "")
	if lookup.Code != http.StatusOK {
		t.Fatalf("lookup status = %d", lookup.Code)
	}
	storedObject := strings.TrimSuffix(strings.TrimPrefix(lookup.Body.String(), `{"record":`), `}`)
	if !strings.Contains(body, `"record":`+storedObject) {
		t.Fatalf("preview record %s not equal to stored record %s", body, lookup.Body.String())
	}

	if response.Header().Get(headerOutcome) != "" || response.Header().Get(headerRecordID) != "" {
		t.Fatalf("preview must not emit outcome headers: %v", response.Header())
	}
}

func TestPreviewConflictReturnsIdentityOnlyAndWritesNoEvent(t *testing.T) {
	handler := newAPIRouter(t)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{"secret":"x"}`, ""))
	var seed struct {
		Record struct {
			ID string `json:"id"`
		} `json:"record"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &seed); err != nil {
		t.Fatalf("decode seed: %v", err)
	}

	response := doJSON(t, handler, http.MethodPost, previewPath, submitBody("k1", "fp-other", `{"price":1}`, ""))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", response.Code, response.Body.String())
	}
	want := `{"outcome":"conflict","record_id":"` + seed.Record.ID + `","request_fingerprint":"fp1"}`
	if body := response.Body.String(); body != want {
		t.Fatalf("body = %s\nwant %s", body, want)
	}
	if strings.Contains(response.Body.String(), "response_snapshot") || strings.Contains(response.Body.String(), "secret") {
		t.Fatalf("conflict preview must not expose the snapshot: %s", response.Body.String())
	}
	if response.Header().Get(headerOutcome) != "" || response.Header().Get(headerRecordID) != "" {
		t.Fatalf("preview must not emit outcome headers: %v", response.Header())
	}

	// No conflict event may be persisted by the preview.
	events := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?key=k1", "")
	if events.Code != http.StatusOK || !strings.Contains(events.Body.String(), `"events":[]`) {
		t.Fatalf("preview wrote a conflict event: %s", events.Body.String())
	}

	// The stored record survives untouched and the preview created no generation.
	lookup := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/k1", "")
	if lookup.Body.String() != created.Body.String() {
		t.Fatalf("record changed after preview: %s", lookup.Body.String())
	}
}

func TestPreviewExpiredOrUnknownKeyIsCreated(t *testing.T) {
	st, handler := historyRouterWithStore(t)
	now := time.Now().UTC()
	seedHistoryHTTPRows(t, st, now)

	// "other" holds one expired generation only: the preview must not replay that historical
	// snapshot even when the fingerprint matches.
	response := doJSON(t, handler, http.MethodPost, previewPath, submitBody("other", "fp2", `{"v":"new"}`, ""))
	if response.Code != http.StatusOK || response.Body.String() != `{"outcome":"created"}` {
		t.Fatalf("expired-key preview = %d %s", response.Code, response.Body.String())
	}
	if response.Header().Get(headerOutcome) != "" || response.Header().Get(headerRecordID) != "" {
		t.Fatalf("preview must not emit outcome headers: %v", response.Header())
	}

	// History is unchanged by the observation.
	history := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=other", "")
	var parsed struct {
		Records []recordResponse `json:"records"`
	}
	if err := json.Unmarshal(history.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode history: %v", err)
	}
	if len(parsed.Records) != 1 {
		t.Fatalf("history rows = %d, want 1: %s", len(parsed.Records), history.Body.String())
	}
	for _, record := range parsed.Records {
		if strings.TrimSpace(string(record.ResponseSnapshot)) == `{"v":"new"}` {
			t.Fatalf("preview wrote a historical row: %s", history.Body.String())
		}
	}
}

func TestPreviewValidationFailures(t *testing.T) {
	handler := newAPIRouter(t)
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
			response := doJSON(t, handler, http.MethodPost, previewPath, tc.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400, body = %s", response.Code, response.Body.String())
			}
			if !strings.HasPrefix(response.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
				t.Fatalf("body = %s", response.Body.String())
			}
			if response.Header().Get(headerOutcome) != "" || response.Header().Get(headerRecordID) != "" {
				t.Fatalf("invalid preview must not emit outcome headers: %v", response.Header())
			}
		})
	}

	// Validation never reaches storage, so nothing exists for the reused keys.
	lookup := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/k", "")
	if lookup.Code != http.StatusNotFound {
		t.Fatalf("validation wrote a record, status = %d", lookup.Code)
	}
}

func TestPreviewOmittedSnapshotDefaultsToNull(t *testing.T) {
	handler := newAPIRouter(t)

	body := `{"idempotency_key":"k-null","request_fingerprint":"fp","expires_at":"2099-01-01T00:00:00Z"}`
	if response := doJSON(t, handler, http.MethodPost, previewPath, body); response.Code != http.StatusOK ||
		response.Body.String() != `{"outcome":"created"}` {
		t.Fatalf("omitted snapshot preview = %d %s", response.Code, response.Body.String())
	}

	// Seed the key with a null snapshot through the real submit, then confirm the replay carries
	// null exactly as the stored record does.
	submit := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", body)
	if submit.Code != http.StatusOK {
		t.Fatalf("seed status = %d body = %s", submit.Code, submit.Body.String())
	}
	replayed := doJSON(t, handler, http.MethodPost, previewPath, body)
	if replayed.Code != http.StatusOK || !strings.Contains(replayed.Body.String(), `"response_snapshot":null`) {
		t.Fatalf("null snapshot replay = %d %s", replayed.Code, replayed.Body.String())
	}
}

func TestPreviewStorageUnavailable(t *testing.T) {
	st, err := store.Open(t.TempDir() + "/service.db")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	handler := NewRouter(st)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	response := doJSON(t, handler, http.MethodPost, previewPath, submitBody("k", "fp", `{}`, ""))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body = %s", response.Code, response.Body.String())
	}
	if !strings.HasPrefix(response.Body.String(), `{"error":{"code":"storage_unavailable",`) {
		t.Fatalf("body = %s", response.Body.String())
	}
	if response.Header().Get(headerOutcome) != "" || response.Header().Get(headerRecordID) != "" {
		t.Fatalf("failed preview must not emit outcome headers: %v", response.Header())
	}
}

func TestPreviewObservationDoesNotBlockLaterSubmit(t *testing.T) {
	handler := newAPIRouter(t)

	first := doJSON(t, handler, http.MethodPost, previewPath, submitBody("k1", "fp1", `{"price":100}`, ""))
	if first.Body.String() != `{"outcome":"created"}` {
		t.Fatalf("first preview = %s", first.Body.String())
	}
	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{"price":100}`, ""))
	if created.Code != http.StatusOK {
		t.Fatalf("submit status = %d body = %s", created.Code, created.Body.String())
	}
	if outcome := created.Header().Get(headerOutcome); outcome != "created" {
		t.Fatalf("submit outcome after preview = %q, want created", outcome)
	}
	second := doJSON(t, handler, http.MethodPost, previewPath, submitBody("k1", "fp1", `{"price":100}`, ""))
	if outcome, id := previewOutcome(t, second); outcome != "replayed" {
		t.Fatalf("second preview outcome = %q id = %q", outcome, id)
	}
}
