package api

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

func decodeEvents(t *testing.T, body string) ([]struct {
	ID                         string `json:"id"`
	IdempotencyKey             string `json:"idempotency_key"`
	ObservedRequestFingerprint string `json:"observed_request_fingerprint"`
	ExistingRecordID           string `json:"existing_record_id"`
	ExistingRequestFingerprint string `json:"existing_request_fingerprint"`
	CreatedAt                  string `json:"created_at"`
}, string) {
	t.Helper()
	var decoded struct {
		Events []struct {
			ID                         string `json:"id"`
			IdempotencyKey             string `json:"idempotency_key"`
			ObservedRequestFingerprint string `json:"observed_request_fingerprint"`
			ExistingRecordID           string `json:"existing_record_id"`
			ExistingRequestFingerprint string `json:"existing_request_fingerprint"`
			CreatedAt                  string `json:"created_at"`
		} `json:"events"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return decoded.Events, decoded.NextCursor
}

func createConflict(t *testing.T, handler http.Handler, key, observedFingerprint string) (recordID string, conflictBody string) {
	t.Helper()
	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records",
		submitBody(key, "original-fp", `{"secret":42}`, ""))
	if created.Code != http.StatusOK {
		t.Fatalf("seed create: %d %s", created.Code, created.Body.String())
	}
	var parsed struct {
		Record struct {
			ID string `json:"id"`
		} `json:"record"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode seed: %v", err)
	}
	conflict := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records",
		submitBody(key, observedFingerprint, `{"other":1}`, ""))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("seed conflict: %d %s", conflict.Code, conflict.Body.String())
	}
	return parsed.Record.ID, conflict.Body.String()
}

func TestConflictsEmptyBeforeAnyConflict(t *testing.T) {
	handler := newAPIRouter(t)

	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts", "")
	if response.Code != http.StatusOK || response.Body.String() != `{"events":[],"next_cursor":""}` {
		t.Fatalf("empty conflicts = %d %s", response.Code, response.Body.String())
	}
}

func TestConflictEventCreatedWithFixedShapeAndOrder(t *testing.T) {
	handler := newAPIRouter(t)

	recordID, conflictBody := createConflict(t, handler, "k1", "intruder-fp")

	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts", "")
	if response.Code != http.StatusOK {
		t.Fatalf("list = %d %s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	wantPrefix := `{"events":[{"id":"con_`
	if !strings.HasPrefix(body, wantPrefix) {
		t.Fatalf("event envelope shape: %s", body)
	}
	events, nextCursor := decodeEvents(t, body)
	if len(events) != 1 || nextCursor != "" {
		t.Fatalf("events = %d cursor = %q: %s", len(events), nextCursor, body)
	}
	event := events[0]
	if !strings.HasPrefix(event.ID, "con_") || len(event.ID) != len("con_")+32 {
		t.Fatalf("event id shape: %q", event.ID)
	}
	for _, ch := range event.ID[len("con_"):] {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			t.Fatalf("event id must be lowercase hex: %q", event.ID)
		}
	}
	if event.IdempotencyKey != "k1" ||
		event.ObservedRequestFingerprint != "intruder-fp" ||
		event.ExistingRecordID != recordID ||
		event.ExistingRequestFingerprint != "original-fp" {
		t.Fatalf("event fields mismatch: %+v", event)
	}
	if _, err := time.Parse(time.RFC3339Nano, event.CreatedAt); err != nil {
		t.Fatalf("created_at not RFC 3339: %q (%v)", event.CreatedAt, err)
	}
	// Events must never expose response snapshots, and the 409 body itself stays byte-stable.
	if strings.Contains(body, "secret") {
		t.Fatalf("conflict audit leaked snapshot bytes: %s", body)
	}
	if !strings.Contains(conflictBody, `"record_id":"`+recordID+`"`) ||
		!strings.Contains(conflictBody, `"request_fingerprint":"original-fp"`) {
		t.Fatalf("409 body identity changed: %s", conflictBody)
	}
}

func TestOnlyConflictsProduceEvents(t *testing.T) {
	handler := newAPIRouter(t)

	doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{}`, ""))
	doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{}`, ""))
	// Validation failures produce neither records nor events.
	doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", `{"idempotency_key":"k1"}`)
	// Queries never write events.
	doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/k1", "")
	doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts", "")

	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts", "")
	if response.Body.String() != `{"events":[],"next_cursor":""}` {
		t.Fatalf("create/replay/invalid/query must not emit events: %s", response.Body.String())
	}
}

func TestConflictsListedNewestFirstWithIDTieBreak(t *testing.T) {
	handler := newAPIRouter(t)

	createConflict(t, handler, "k1", "fp-a")
	time.Sleep(2 * time.Millisecond)
	createConflict(t, handler, "k2", "fp-b")
	time.Sleep(2 * time.Millisecond)
	createConflict(t, handler, "k3", "fp-c")

	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?limit=50", "")
	events, _ := decodeEvents(t, response.Body.String())
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3", len(events))
	}
	if events[0].ObservedRequestFingerprint != "fp-c" ||
		events[1].ObservedRequestFingerprint != "fp-b" ||
		events[2].ObservedRequestFingerprint != "fp-a" {
		t.Fatalf("events must be newest first: %+v", events)
	}
}

func TestConflictReturns503WhenEventCannotBeSaved(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "service.db")
	st, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	handler := NewRouter(st)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records",
		submitBody("k1", "fp1", `{}`, ""))
	if created.Code != http.StatusOK {
		t.Fatalf("seed: %d %s", created.Code, created.Body.String())
	}

	// With storage gone the conflict is identified by semantics but the event cannot be saved;
	// the client must see 503 storage_unavailable rather than a 409 without an audit row.
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records",
		submitBody("k1", "fp-different", `{}`, ""))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503, body = %s", response.Code, response.Body.String())
	}
	if !strings.HasPrefix(response.Body.String(), `{"error":{"code":"storage_unavailable",`) {
		t.Fatalf("body = %s", response.Body.String())
	}
	if response.Header().Get("Idempotency-Outcome") != "" || response.Header().Get("Idempotency-Record-ID") != "" {
		t.Fatalf("503 must not carry idempotency headers: %v", response.Header())
	}
}
