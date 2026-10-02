package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

func conflictsRouterWithStore(t *testing.T) (*store.Store, http.Handler) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st, NewRouter(st)
}

type conflictEventJSON struct {
	ID                         string `json:"id"`
	IdempotencyKey             string `json:"idempotency_key"`
	ObservedRequestFingerprint string `json:"observed_request_fingerprint"`
	ExistingRecordID           string `json:"existing_record_id"`
	ExistingRequestFingerprint string `json:"existing_request_fingerprint"`
	CreatedAt                  string `json:"created_at"`
}

func decodeConflictPage(t *testing.T, body string) struct {
	Events     []conflictEventJSON `json:"events"`
	NextCursor string              `json:"next_cursor"`
} {
	t.Helper()
	var decoded struct {
		Events     []conflictEventJSON `json:"events"`
		NextCursor string              `json:"next_cursor"`
	}
	if err := json.Unmarshal([]byte(body), &decoded); err != nil {
		t.Fatalf("decode conflicts page: %v body=%s", err, body)
	}
	return decoded
}

func TestSubmitConflictPersistsAuditEvent(t *testing.T) {
	handler := newAPIRouter(t)

	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{"price":100}`, ""))
	var createdParsed struct {
		Record struct {
			ID string `json:"id"`
		} `json:"record"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &createdParsed); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// A same-fingerprint replay emits nothing.
	if replay := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp1", `{"price":999}`, "")); replay.Code != http.StatusOK {
		t.Fatalf("replay status = %d", replay.Code)
	}

	conflict := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp2", `{"price":1}`, ""))
	if conflict.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body = %s", conflict.Code, conflict.Body.String())
	}
	if conflict.Header().Get("Idempotency-Outcome") != "" || conflict.Header().Get("Idempotency-Record-ID") != "" {
		t.Fatalf("409 must not carry idempotency headers: %+v", conflict.Header())
	}

	events := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts", "")
	if events.Code != http.StatusOK {
		t.Fatalf("list conflicts = %d %s", events.Code, events.Body.String())
	}
	body := events.Body.String()
	if !strings.HasPrefix(body, `{"events":[{"id":"con_`) {
		t.Fatalf("envelope shape: %s", body)
	}
	if strings.Contains(body, "response_snapshot") || strings.Contains(body, "price") {
		t.Fatalf("conflict events must not expose response snapshots: %s", body)
	}
	decoded := decodeConflictPage(t, body)
	if len(decoded.Events) != 1 || decoded.NextCursor != "" {
		t.Fatalf("page = %+v", decoded)
	}
	event := decoded.Events[0]
	if event.IdempotencyKey != "k1" ||
		event.ObservedRequestFingerprint != "fp2" ||
		event.ExistingRecordID != createdParsed.Record.ID ||
		event.ExistingRequestFingerprint != "fp1" {
		t.Fatalf("event fields wrong: %+v", event)
	}
	if !store.IsConflictID(event.ID) {
		t.Fatalf("event id shape: %q", event.ID)
	}
	if _, err := time.Parse(time.RFC3339Nano, event.CreatedAt); err != nil {
		t.Fatalf("created_at is not RFC 3339: %q (%v)", event.CreatedAt, err)
	}
	wantFields := []string{
		`"id":"` + event.ID + `"`,
		`"idempotency_key":"k1"`,
		`"observed_request_fingerprint":"fp2"`,
		`"existing_record_id":"` + createdParsed.Record.ID + `"`,
		`"existing_request_fingerprint":"fp1"`,
		`"created_at":"` + event.CreatedAt + `"`,
	}
	position := 0
	for _, field := range wantFields {
		idx := strings.Index(body, field)
		if idx < position {
			t.Fatalf("field order wrong around %q: %s", field, body)
		}
		position = idx
	}

	// Every losing request leaves its own event; repeats with distinct fingerprints accumulate.
	if second := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k1", "fp3", `{}`, "")); second.Code != http.StatusConflict {
		t.Fatalf("second conflict status = %d", second.Code)
	}
	page := decodeConflictPage(t, doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?key=k1", "").Body.String())
	if len(page.Events) != 2 {
		t.Fatalf("events = %d, want 2", len(page.Events))
	}
	if page.Events[0].ObservedRequestFingerprint != "fp3" || page.Events[1].ObservedRequestFingerprint != "fp2" {
		t.Fatalf("events not newest first: %+v", page.Events)
	}
}

func TestConflictEventsOnlyProducedByFingerprintConflicts(t *testing.T) {
	handler := newAPIRouter(t)

	// Validation failures produce no events.
	for _, body := range []string{
		`{"idempotency_key":"","request_fingerprint":"fp","response_snapshot":{},"expires_at":"2099-01-01T00:00:00Z"}`,
		`not json`,
	} {
		if response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", body); response.Code != http.StatusBadRequest {
			t.Fatalf("validation status = %d body = %s", response.Code, response.Body.String())
		}
	}

	// First create, replay, and an expired-then-resubmit sequence produce no events either.
	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("renew", "fp", `{}`, ""))
	if created.Code != http.StatusOK {
		t.Fatalf("create = %d", created.Code)
	}
	if replay := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("renew", "fp", `{}`, "")); replay.Code != http.StatusOK {
		t.Fatalf("replay = %d", replay.Code)
	}

	// GETs never create events.
	doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts", "")
	doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/renew", "")
	doJSON(t, handler, http.MethodGet, "/v1/idempotency/records", "")
	doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=renew", "")

	empty := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts", "")
	if empty.Code != http.StatusOK || empty.Body.String() != `{"events":[],"next_cursor":""}` {
		t.Fatalf("empty conflicts page = %d %s", empty.Code, empty.Body.String())
	}
}

func seedConflictEventsHTTP(t *testing.T, st *store.Store, now time.Time) []store.ConflictEvent {
	t.Helper()
	ctx := context.Background()
	const recA = "rec_00000000000000000000000000000001"
	const recB = "rec_00000000000000000000000000000002"
	specs := []struct {
		key, observed, existingID, existingFP string
		created                               time.Time
	}{
		{"k", "fp-x", recA, "fp-a", now.Add(-4 * time.Minute)},
		{"k", "fp-y", recA, "fp-a", now.Add(-3 * time.Minute)},
		{"other", "fp-x", recB, "fp-c", now.Add(-2 * time.Minute)},
		{"k", "fp-y", recB, "fp-c", now.Add(-time.Minute)},
		{"k", "fp-z", recB, "fp-c", now},
	}
	events := make([]store.ConflictEvent, 0, len(specs))
	for _, spec := range specs {
		id, err := store.NewConflictID()
		if err != nil {
			t.Fatalf("id: %v", err)
		}
		event := store.ConflictEvent{
			ID:                         id,
			IdempotencyKey:             spec.key,
			ObservedRequestFingerprint: spec.observed,
			ExistingRecordID:           spec.existingID,
			ExistingRequestFingerprint: spec.existingFP,
			CreatedAt:                  spec.created,
		}
		if _, err := st.SeedConflictEventForTest(ctx, event); err != nil {
			t.Fatalf("seed: %v", err)
		}
		events = append(events, event)
	}
	return events
}

func TestConflictListFiltersAreExact(t *testing.T) {
	st, handler := conflictsRouterWithStore(t)
	now := time.Now().UTC()
	seeded := seedConflictEventsHTTP(t, st, now)

	byKey := decodeConflictPage(t, doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?key=k", "").Body.String())
	if len(byKey.Events) != 4 {
		t.Fatalf("key filter = %+v", byKey.Events)
	}
	for _, event := range byKey.Events {
		if event.IdempotencyKey != "k" {
			t.Fatalf("key filter leaked: %+v", event)
		}
	}

	byObserved := decodeConflictPage(t, doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?observed_request_fingerprint=fp-y", "").Body.String())
	if len(byObserved.Events) != 2 {
		t.Fatalf("observed filter = %+v", byObserved.Events)
	}
	for _, event := range byObserved.Events {
		if event.ObservedRequestFingerprint != "fp-y" {
			t.Fatalf("observed filter leaked: %+v", event)
		}
	}

	whitespace := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?key=k%20", "")
	if whitespace.Code != http.StatusOK || whitespace.Body.String() != `{"events":[],"next_cursor":""}` {
		t.Fatalf("whitespace key must match literally: %d %s", whitespace.Code, whitespace.Body.String())
	}

	byRecord := decodeConflictPage(t, doJSON(t, handler, http.MethodGet,
		"/v1/idempotency/conflicts?existing_record_id="+seeded[0].ExistingRecordID, "").Body.String())
	if len(byRecord.Events) != 2 {
		t.Fatalf("existing_record_id filter = %+v", byRecord.Events)
	}
	for _, event := range byRecord.Events {
		if event.ExistingRecordID != seeded[0].ExistingRecordID {
			t.Fatalf("existing_record_id filter leaked: %+v", event)
		}
	}

	combined := decodeConflictPage(t, doJSON(t, handler, http.MethodGet,
		"/v1/idempotency/conflicts?key=k&observed_request_fingerprint=fp-y&existing_record_id="+seeded[3].ExistingRecordID, "").Body.String())
	if len(combined.Events) != 1 || combined.Events[0].ID != seeded[3].ID {
		t.Fatalf("combined filter = %+v", combined.Events)
	}
}

func TestConflictListPagination(t *testing.T) {
	st, handler := conflictsRouterWithStore(t)
	now := time.Now().UTC()
	seeded := seedConflictEventsHTTP(t, st, now)

	page1 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?key=k&limit=2", "")
	if page1.Code != http.StatusOK {
		t.Fatalf("page1 = %d %s", page1.Code, page1.Body.String())
	}
	first := decodeConflictPage(t, page1.Body.String())
	if len(first.Events) != 2 || first.NextCursor == "" {
		t.Fatalf("page1 = %+v", first)
	}
	if first.Events[0].ID != seeded[4].ID || first.Events[1].ID != seeded[3].ID {
		t.Fatalf("page1 order = %+v", first.Events)
	}

	page2 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?key=k&limit=2&cursor="+first.NextCursor, "")
	second := decodeConflictPage(t, page2.Body.String())
	if len(second.Events) != 2 || second.NextCursor != "" {
		t.Fatalf("page2 = %+v, want the final page with an empty cursor", second)
	}
	if second.Events[0].ID != seeded[1].ID || second.Events[1].ID != seeded[0].ID {
		t.Fatalf("page2 order = %+v", second.Events)
	}

	// Reusing the final cursor returns an empty page instead of repeating rows.
	page3 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?key=k&limit=2&cursor="+
		encodeCursorForTest(t, second.Events[1].CreatedAt, second.Events[1].ID), "")
	third := decodeConflictPage(t, page3.Body.String())
	if len(third.Events) != 0 || third.NextCursor != "" {
		t.Fatalf("page3 = %+v, want an empty final page", third)
	}
}

func TestConflictListCursorAndLimitValidation(t *testing.T) {
	st, handler := conflictsRouterWithStore(t)
	now := time.Now().UTC()
	seeded := seedConflictEventsHTTP(t, st, now)

	cases := []struct {
		name   string
		target string
		code   string
	}{
		{"garbage cursor", "/v1/idempotency/conflicts?cursor=!!!not-base64", "invalid_cursor"},
		{"json missing fields", "/v1/idempotency/conflicts?cursor=" + url.QueryEscape(base64EncodeJSON(`{}`)), "invalid_cursor"},
		{"malformed id", "/v1/idempotency/conflicts?cursor=" + url.QueryEscape(base64EncodeJSON(`{"created_at":"2026-10-02T10:00:00Z","id":"bogus"}`)), "invalid_cursor"},
		{"rec id instead of con", "/v1/idempotency/conflicts?cursor=" + url.QueryEscape(base64EncodeJSON(`{"created_at":"2026-10-02T10:00:00Z","id":"rec_00000000000000000000000000000001"}`)), "invalid_cursor"},
		{"unknown id", "/v1/idempotency/conflicts?cursor=" + url.QueryEscape(base64EncodeJSON(`{"created_at":"2026-10-02T10:00:00Z","id":"con_00000000000000000000000000000099"}`)), "invalid_cursor"},
		{"cursor excluded by filter", "/v1/idempotency/conflicts?key=other&cursor=" +
			url.QueryEscape(base64EncodeJSON(`{"created_at":"`+seeded[0].CreatedAt.Format(time.RFC3339Nano)+`","id":"`+seeded[0].ID+`"}`)), "invalid_cursor"},
		{"zero limit", "/v1/idempotency/conflicts?limit=0", "invalid_conflict_query"},
		{"over limit", "/v1/idempotency/conflicts?limit=101", "invalid_conflict_query"},
		{"decimal point", "/v1/idempotency/conflicts?limit=1.5", "invalid_conflict_query"},
		{"negative", "/v1/idempotency/conflicts?limit=-3", "invalid_conflict_query"},
		{"not numeric", "/v1/idempotency/conflicts?limit=abc", "invalid_conflict_query"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := doJSON(t, handler, http.MethodGet, tc.target, "")
			if response.Code != http.StatusBadRequest ||
				!strings.HasPrefix(response.Body.String(), `{"error":{"code":"`+tc.code+`",`) {
				t.Fatalf("%s = %d %s, want 400 %s", tc.name, response.Code, response.Body.String(), tc.code)
			}
			body := response.Body.String()
			for _, leaked := range []string{"SELECT", "INSERT", "sqlite", ".go", "goroutine"} {
				if strings.Contains(body, leaked) {
					t.Fatalf("error message leaked %q: %s", leaked, body)
				}
			}
		})
	}
}

func TestConflictListStorageUnavailable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "service.db")
	st, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	handler := NewRouter(st)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts", "")
	if response.Code != http.StatusServiceUnavailable ||
		!strings.HasPrefix(response.Body.String(), `{"error":{"code":"storage_unavailable",`) {
		t.Fatalf("storage failure = %d %s", response.Code, response.Body.String())
	}
}

func TestConflictEndpointDoesNotLeakThroughRecordEndpoints(t *testing.T) {
	handler := newAPIRouter(t)
	doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k", "fp1", `{"secret":true}`, ""))
	doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k", "fp2", `{}`, ""))

	for _, target := range []string{
		"/v1/idempotency/records/k",
		"/v1/idempotency/records",
		"/v1/idempotency/records-by-id/none",
		"/v1/idempotency/history?key=k",
	} {
		response := doJSON(t, handler, http.MethodGet, target, "")
		if strings.Contains(response.Body.String(), "con_") ||
			strings.Contains(response.Body.String(), "observed_request_fingerprint") {
			t.Fatalf("%s leaked conflict data: %s", target, response.Body.String())
		}
	}
}

func encodeCursorForTest(t *testing.T, createdAt string, id string) string {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		t.Fatalf("parse cursor time %q: %v", createdAt, err)
	}
	return pageCursor{CreatedAt: parsed, ID: id}.encode()
}

func TestSubmitConflictReturns503WhenEventStorageUnavailable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "service.db")
	st, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	handler := NewRouter(st)

	// Establish the active winner while storage is healthy.
	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k", "fp1", `{}`, ""))
	if created.Code != http.StatusOK {
		t.Fatalf("create = %d %s", created.Code, created.Body.String())
	}

	// Closing the store makes the conflict transaction (and its event insert) fail; the caller must
	// see 503 storage_unavailable, never 409.
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	conflict := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("k", "fp2", `{}`, ""))
	if conflict.Code != http.StatusServiceUnavailable ||
		!strings.HasPrefix(conflict.Body.String(), `{"error":{"code":"storage_unavailable",`) {
		t.Fatalf("conflict with failed event storage = %d %s, want 503 storage_unavailable",
			conflict.Code, conflict.Body.String())
	}
}
