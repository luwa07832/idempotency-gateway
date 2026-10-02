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

// seedExpiredRow inserts one expired historical row directly through the store, bypassing the
// POST contract (which rejects past expires_at). putNow must be later than the row's expiry so
// same-key generations are stored as independent rows.
func seedExpiredRow(t *testing.T, st *store.Store, key, fingerprint, snapshot string, createdAt time.Time, ttl time.Duration, putNow time.Time) store.Record {
	t.Helper()
	id, err := store.NewRecordID()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}
	record := store.Record{
		ID:                 id,
		IdempotencyKey:     key,
		RequestFingerprint: fingerprint,
		ResponseSnapshot:   []byte(snapshot),
		CreatedAt:          createdAt,
		ExpiresAt:          createdAt.Add(ttl),
	}
	if _, _, err := st.PutRecord(context.Background(), record, putNow); err != nil {
		t.Fatalf("seed expired row: %v", err)
	}
	return record
}

func newHistoryRouter(t *testing.T) (http.Handler, *store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return NewRouter(st), st
}

func TestHistoryRequiresKey(t *testing.T) {
	handler, _ := newHistoryRouter(t)
	cases := []string{
		"/v1/idempotency/history",
		"/v1/idempotency/history?key=",
		"/v1/idempotency/history?key=%20%20",
	}
	for _, target := range cases {
		response := doJSON(t, handler, http.MethodGet, target, "")
		if response.Code != http.StatusBadRequest ||
			!strings.HasPrefix(response.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
			t.Fatalf("%s = %d %s", target, response.Code, response.Body.String())
		}
	}
}

func TestHistoryUnknownKeyReturnsEmptyPage(t *testing.T) {
	handler, _ := newHistoryRouter(t)
	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=never-seen", "")
	if response.Code != http.StatusOK || response.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("unknown key = %d %s", response.Code, response.Body.String())
	}
}

func TestHistoryReturnsExpiredGenerationsWithFixedShape(t *testing.T) {
	handler, st := newHistoryRouter(t)
	now := time.Now().UTC()

	first := seedExpiredRow(t, st, "order-1", "fp1", `{"price":100}`, now.Add(-3*time.Hour), time.Hour, now)
	second := seedExpiredRow(t, st, "order-1", "fp2", ` [ "a" , 2 ] `, now.Add(-90*time.Minute), time.Hour, now)
	active := store.Record{
		ID:                 func() string { id, _ := store.NewRecordID(); return id }(),
		IdempotencyKey:     "order-1",
		RequestFingerprint: "fp3",
		ResponseSnapshot:   []byte(`{"price":300}`),
		CreatedAt:          now.Add(-time.Minute),
		ExpiresAt:          now.Add(time.Hour),
	}
	if _, _, err := st.PutRecord(context.Background(), active, now); err != nil {
		t.Fatalf("seed active row: %v", err)
	}
	seedExpiredRow(t, st, "other-key", "fp1", `{"other":true}`, now.Add(-2*time.Hour), time.Hour, now)

	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=order-1", "")
	if response.Code != http.StatusOK {
		t.Fatalf("history status = %d body = %s", response.Code, response.Body.String())
	}
	body := response.Body.String()

	var parsed struct {
		Records []struct {
			ID                 string          `json:"id"`
			IdempotencyKey     string          `json:"idempotency_key"`
			Status             string          `json:"status"`
			RequestFingerprint string          `json:"request_fingerprint"`
			ResponseSnapshot   json.RawMessage `json:"response_snapshot"`
			CreatedAt          string          `json:"created_at"`
			ExpiresAt          string          `json:"expires_at"`
		} `json:"records"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(parsed.Records) != 2 {
		t.Fatalf("records = %d, want 2 expired generations (active hidden, other key hidden): %s", len(parsed.Records), body)
	}
	if parsed.Records[0].ID != second.ID || parsed.Records[1].ID != first.ID {
		t.Fatalf("order = %s then %s, want second generation first", parsed.Records[0].ID, parsed.Records[1].ID)
	}
	for _, record := range parsed.Records {
		if record.Status != "expired" || record.IdempotencyKey != "order-1" {
			t.Fatalf("record shape wrong: %+v", record)
		}
		if _, err := time.Parse(time.RFC3339, record.CreatedAt); err != nil {
			t.Fatalf("created_at not RFC 3339: %s", record.CreatedAt)
		}
		if _, err := time.Parse(time.RFC3339, record.ExpiresAt); err != nil {
			t.Fatalf("expires_at not RFC 3339: %s", record.ExpiresAt)
		}
	}
	// The first committed JSON bytes are preserved verbatim, including odd whitespace. The check
	// runs against the raw response body: json.Unmarshal trims outer whitespace around a value.
	if !strings.Contains(body, `"response_snapshot": [ "a" , 2 ] `) {
		t.Fatalf("snapshot bytes not verbatim: %s", body)
	}
	if !strings.Contains(body, `"response_snapshot":{"price":100}`) {
		t.Fatalf("first snapshot not verbatim: %s", body)
	}
	// Fixed field order: id, idempotency_key, status, request_fingerprint, response_snapshot,
	// created_at, expires_at.
	recordPrefix := `{"id":"` + second.ID + `","idempotency_key":"order-1","status":"expired","request_fingerprint":"fp2","response_snapshot":`
	if !strings.Contains(body, recordPrefix) {
		t.Fatalf("fixed field order not respected: %s", body)
	}
	if parsed.NextCursor != "" || !strings.HasSuffix(body, `,"next_cursor":""}`) {
		t.Fatalf("next cursor should be empty: %s", body)
	}

	// The active-only endpoints keep hiding expired snapshots.
	got := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/order-1", "")
	if got.Code != http.StatusOK || strings.Contains(got.Body.String(), `"price":100`) {
		t.Fatalf("GET by key leaked expired snapshot: %s", got.Body.String())
	}
	listed := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?key=order-1", "")
	if strings.Count(listed.Body.String(), `"idempotency_key"`) != 1 || strings.Contains(listed.Body.String(), `"expired"`) {
		t.Fatalf("list leaked history: %s", listed.Body.String())
	}
}

func TestHistoryFiltersExactFingerprintAndExpiryBounds(t *testing.T) {
	handler, st := newHistoryRouter(t)
	now := time.Now().UTC()

	type rowSpec struct {
		fingerprint string
		created     time.Duration
		ttl         time.Duration
	}
	specs := []rowSpec{
		{"alpha", -4 * time.Hour, 30 * time.Minute},
		{"alpha", -3 * time.Hour, time.Hour},
		{"ALPHA", -2 * time.Hour, 2 * time.Hour},
		{"beta", -90 * time.Minute, 90 * time.Minute},
	}
	for _, spec := range specs {
		created := now.Add(spec.created)
		seedExpiredRow(t, st, "k1", spec.fingerprint, `"s"`, created, spec.ttl, now)
	}

	match := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k1&request_fingerprint=alpha", "")
	if count := strings.Count(match.Body.String(), `"request_fingerprint"`); count != 2 {
		t.Fatalf("exact fingerprint match count = %d, body = %s", count, match.Body.String())
	}
	upper := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k1&request_fingerprint=ALPHA", "")
	if strings.Count(upper.Body.String(), `"idempotency_key"`) != 1 {
		t.Fatalf("case must be significant: %s", upper.Body.String())
	}
	prefix := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k1&request_fingerprint=alp", "")
	if prefix.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("prefix must not match: %s", prefix.Body.String())
	}
	whitespace := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k1&request_fingerprint=%20alpha%20", "")
	if whitespace.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("whitespace must be matched verbatim: %s", whitespace.Body.String())
	}

	// Expiry timestamps: h1 -3h30m, h2 -2h, h3 now, h4 now.
	midway := now.Add(-105 * time.Minute).Format(time.RFC3339Nano)
	before := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k1&expires_before="+url.QueryEscape(midway), "")
	if count := strings.Count(before.Body.String(), `"idempotency_key"`); count != 2 {
		t.Fatalf("expires_before strictly earlier = %d: %s", count, before.Body.String())
	}
	// Boundary equal to h2's expiry must exclude h2 itself.
	atH2 := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	beforeEqual := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k1&expires_before="+url.QueryEscape(atH2), "")
	if strings.Count(beforeEqual.Body.String(), `"idempotency_key"`) != 1 {
		t.Fatalf("expires_before equality exclusion wrong: %s", beforeEqual.Body.String())
	}
	after := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k1&expires_after="+url.QueryEscape(atH2), "")
	if strings.Count(after.Body.String(), `"idempotency_key"`) != 2 {
		t.Fatalf("expires_after strictly later = %s", after.Body.String())
	}
}

func TestHistoryValidatesLimitAndTimes(t *testing.T) {
	handler, _ := newHistoryRouter(t)
	cases := []string{
		"/v1/idempotency/history?key=k&limit=0",
		"/v1/idempotency/history?key=k&limit=101",
		"/v1/idempotency/history?key=k&limit=1.5",
		"/v1/idempotency/history?key=k&limit=abc",
		"/v1/idempotency/history?key=k&expires_before=not-a-time",
		"/v1/idempotency/history?key=k&expires_after=2026-13-99T00:00:00Z",
	}
	for _, target := range cases {
		response := doJSON(t, handler, http.MethodGet, target, "")
		if response.Code != http.StatusBadRequest ||
			!strings.HasPrefix(response.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
			t.Fatalf("%s = %d %s", target, response.Code, response.Body.String())
		}
	}
}

func TestHistoryInvalidCursor(t *testing.T) {
	handler, _ := newHistoryRouter(t)
	// {"id":"rec_x"}
	tampered := "eyJpZCI6InJlY194In0"
	cases := []string{
		"/v1/idempotency/history?key=k&cursor=not-base64!!",
		"/v1/idempotency/history?key=k&cursor=" + tampered,
		"/v1/idempotency/history?key=k&cursor=eyJjcmVhdGVkX2F0IjoiMjAyNi0xMC0wMVQxMDowMDowMFoifQ==",
	}
	for _, target := range cases {
		response := doJSON(t, handler, http.MethodGet, target, "")
		if response.Code != http.StatusBadRequest ||
			!strings.HasPrefix(response.Body.String(), `{"error":{"code":"invalid_cursor",`) ||
			strings.Contains(response.Body.String(), "SELECT") ||
			strings.Contains(response.Body.String(), "/") {
			t.Fatalf("%s = %d %s", target, response.Code, response.Body.String())
		}
	}
}

func TestHistoryPaginationStableAcrossConcurrentChanges(t *testing.T) {
	handler, st := newHistoryRouter(t)
	now := time.Now().UTC()

	var seeded []store.Record
	for i := 0; i < 3; i++ {
		created := now.Add(time.Duration(-3+i) * time.Hour)
		seeded = append(seeded, seedExpiredRow(t, st, "k1", "fp", `{"i":`+string(rune('0'+i))+`}`, created, time.Hour, now))
	}

	page1 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k1&limit=2", "")
	var parsed1 struct {
		Records []struct {
			ID string `json:"id"`
		} `json:"records"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(page1.Body.Bytes(), &parsed1); err != nil {
		t.Fatalf("decode page1: %v", err)
	}
	if len(parsed1.Records) != 2 || parsed1.Records[0].ID != seeded[2].ID || parsed1.Records[1].ID != seeded[1].ID || parsed1.NextCursor == "" {
		t.Fatalf("page1 = %+v", parsed1)
	}

	// While the client holds the cursor a new still-active generation is inserted. It sorts above
	// the cursor but is not expired, so it cannot duplicate or shift the already-served pages.
	lateRecord := store.Record{
		ID:                 func() string { id, _ := store.NewRecordID(); return id }(),
		IdempotencyKey:     "k1",
		RequestFingerprint: "fp-late",
		ResponseSnapshot:   []byte(`{"late":true}`),
		CreatedAt:          now.Add(time.Minute),
		ExpiresAt:          now.Add(2 * time.Hour),
	}
	if _, _, err := st.PutRecord(context.Background(), lateRecord, now.Add(time.Minute)); err != nil {
		t.Fatalf("insert late row: %v", err)
	}

	page2 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k1&limit=2&cursor="+parsed1.NextCursor, "")
	var parsed2 struct {
		Records []struct {
			ID string `json:"id"`
		} `json:"records"`
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(page2.Body.Bytes(), &parsed2); err != nil {
		t.Fatalf("decode page2: %v", err)
	}
	if len(parsed2.Records) != 1 || parsed2.Records[0].ID != seeded[0].ID || parsed2.NextCursor != "" {
		t.Fatalf("page2 = %+v, want only the earliest seeded row", parsed2)
	}
	for _, row := range parsed1.Records {
		for _, row2 := range parsed2.Records {
			if row.ID == row2.ID {
				t.Fatalf("page duplicated id %s", row.ID)
			}
		}
	}
}

func TestHistoryStorageUnavailable(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "service.db")
	st, err := store.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	handler := NewRouter(st)
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k1", "")
	if response.Code != http.StatusServiceUnavailable ||
		!strings.HasPrefix(response.Body.String(), `{"error":{"code":"storage_unavailable",`) {
		t.Fatalf("closed store = %d %s", response.Code, response.Body.String())
	}
}

func TestHistoryAcrossInstancesSharesRows(t *testing.T) {
	routers, path := newSharedRouters(t, 2)
	st, err := store.Open(path)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	t.Cleanup(func() { st.Close() })

	now := time.Now().UTC()
	seedExpiredRow(t, st, "shared", "fp", `{"v":1}`, now.Add(-2*time.Hour), time.Hour, now)

	fromInstance0 := doJSON(t, routers[0], http.MethodGet, "/v1/idempotency/history?key=shared", "")
	fromInstance1 := doJSON(t, routers[1], http.MethodGet, "/v1/idempotency/history?key=shared", "")
	if fromInstance0.Code != http.StatusOK || fromInstance0.Body.String() != fromInstance1.Body.String() {
		t.Fatalf("instances disagree: %s vs %s", fromInstance0.Body.String(), fromInstance1.Body.String())
	}
	if strings.Count(fromInstance1.Body.String(), `"idempotency_key"`) != 1 {
		t.Fatalf("shared history not visible across instances: %s", fromInstance1.Body.String())
	}
}
