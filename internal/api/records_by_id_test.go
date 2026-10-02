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

func TestSubmitOutcomeHeaders(t *testing.T) {
	handler := newAPIRouter(t)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{"price":100}`, ""))
	if created.Code != http.StatusOK {
		t.Fatalf("create status = %d body = %s", created.Code, created.Body.String())
	}
	if got := created.Header().Get("Idempotency-Outcome"); got != "created" {
		t.Fatalf("create outcome header = %q, want created", got)
	}
	var parsed struct {
		Record struct {
			ID string `json:"id"`
		} `json:"record"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	if got := created.Header().Get("Idempotency-Record-ID"); got != parsed.Record.ID {
		t.Fatalf("create record-id header = %q, want %s", got, parsed.Record.ID)
	}

	replayed := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{"price":999}`, ""))
	if replayed.Code != http.StatusOK {
		t.Fatalf("replay status = %d", replayed.Code)
	}
	if got := replayed.Header().Get("Idempotency-Outcome"); got != "replayed" {
		t.Fatalf("replay outcome header = %q, want replayed", got)
	}
	if got := replayed.Header().Get("Idempotency-Record-ID"); got != parsed.Record.ID {
		t.Fatalf("replay record-id header = %q, want %s", got, parsed.Record.ID)
	}
	if replayed.Body.String() != created.Body.String() {
		t.Fatalf("replay body = %s, want first body %s", replayed.Body.String(), created.Body.String())
	}

	conflict := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp-other", `{}`, ""))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d", conflict.Code)
	}
	if got := conflict.Header().Get("Idempotency-Outcome"); got != "" {
		t.Fatalf("conflict leaked outcome header = %q", got)
	}
	if got := conflict.Header().Get("Idempotency-Record-ID"); got != "" {
		t.Fatalf("conflict leaked record-id header = %q", got)
	}

	invalid := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", `{"request_fingerprint":"fp"}`)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid status = %d", invalid.Code)
	}
	if got := invalid.Header().Get("Idempotency-Outcome"); got != "" {
		t.Fatalf("invalid request leaked outcome header = %q", got)
	}
	if got := invalid.Header().Get("Idempotency-Record-ID"); got != "" {
		t.Fatalf("invalid request leaked record-id header = %q", got)
	}
}

func TestGetRecordByIDActive(t *testing.T) {
	handler := newAPIRouter(t)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("by-id-key", "fp1", `{"price":100}`, ""))
	var parsed struct {
		Record struct {
			ID string `json:"id"`
		} `json:"record"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode: %v", err)
	}

	fetched := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records-by-id/"+parsed.Record.ID, "")
	if fetched.Code != http.StatusOK {
		t.Fatalf("get by id status = %d body = %s", fetched.Code, fetched.Body.String())
	}
	if fetched.Body.String() != created.Body.String() {
		t.Fatalf("get by id body = %s, want %s", fetched.Body.String(), created.Body.String())
	}
	if !strings.Contains(fetched.Body.String(), `"status":"active"`) {
		t.Fatalf("active record rendered non-active: %s", fetched.Body.String())
	}
}

func TestGetRecordByIDReachesExpiredGenerationWithVerbatimSnapshot(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "service.db")
	st, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	now := time.Now().UTC()

	expiredID, err := store.NewRecordID()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}
	// Snapshot carries insignificant whitespace; the by-id read must replay the stored bytes.
	expired := store.Record{
		ID:                 expiredID,
		IdempotencyKey:     "archived",
		RequestFingerprint: "fp1",
		ResponseSnapshot:   []byte(`{ "kept" : true }`),
		CreatedAt:          now.Add(-2 * time.Hour),
		ExpiresAt:          now.Add(-time.Hour),
	}
	if _, _, err := st.PutRecord(context.Background(), expired, now); err != nil {
		t.Fatalf("seed expired row: %v", err)
	}

	handler := NewRouter(st)
	fetched := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records-by-id/"+expiredID, "")
	if fetched.Code != http.StatusOK {
		t.Fatalf("expired get by id status = %d body = %s", fetched.Code, fetched.Body.String())
	}
	body := fetched.Body.String()
	if !strings.Contains(body, `"id":"`+expiredID+`"`) || !strings.Contains(body, `"status":"expired"`) {
		t.Fatalf("expired record shape wrong: %s", body)
	}
	if !strings.Contains(body, `"response_snapshot":{ "kept" : true }`) {
		t.Fatalf("snapshot bytes were not preserved: %s", body)
	}

	// The read is strictly read-only: key lookup and list still hide the row, history still has it.
	byKey := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/archived", "")
	if byKey.Code != http.StatusNotFound {
		t.Fatalf("expired snapshot leaked through key lookup: %d %s", byKey.Code, byKey.Body.String())
	}
	list := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?key=archived", "")
	if list.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("expired snapshot leaked through list: %s", list.Body.String())
	}
	history := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=archived", "")
	if history.Code != http.StatusOK || !strings.Contains(history.Body.String(), expiredID) {
		t.Fatalf("history lost the expired row: %d %s", history.Code, history.Body.String())
	}
}

func TestGetRecordByIDMalformedAndUnknown(t *testing.T) {
	handler := newAPIRouter(t)

	wellFormedUnknown := "rec_0123456789abcdef0123456789abcdef"
	malformed := []string{
		"rec_",
		"rec_0123456789abcdef0123456789abcde",
		"rec_0123456789abcdef0123456789abcdef0",
		"REC_0123456789abcdef0123456789abcdef",
		"rec_0123456789abcdef0123456789ABCDEF",
		"rec_0123456789abcdef0123456789abcdeg",
		"0123456789abcdef0123456789abcdef",
		"rec_zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
	}
	for _, id := range malformed {
		response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records-by-id/"+id, "")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("id %q status = %d, want 400", id, response.Code)
		}
		if !strings.HasPrefix(response.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
			t.Fatalf("id %q body = %s", id, response.Body.String())
		}
	}

	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records-by-id/"+wellFormedUnknown, "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown id status = %d, want 404", response.Code)
	}
	if !strings.HasPrefix(response.Body.String(), `{"error":{"code":"not_found",`) {
		t.Fatalf("unknown id body = %s", response.Body.String())
	}
}

func TestGetRecordByIDStorageUnavailable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "service.db")
	st, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	handler := NewRouter(st)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A malformed id is rejected before touching storage; a well-formed one surfaces only 503.
	request := httptest.NewRequest(http.MethodGet, "/v1/idempotency/records-by-id/rec_0123456789abcdef0123456789abcdef", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body = %s", recorder.Code, recorder.Body.String())
	}
	if !strings.HasPrefix(recorder.Body.String(), `{"error":{"code":"storage_unavailable",`) {
		t.Fatalf("body = %s", recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "sql") {
		t.Fatalf("body leaks internal detail: %s", recorder.Body.String())
	}
}
