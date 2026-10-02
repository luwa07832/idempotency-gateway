package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

func historyRouterWithStore(t *testing.T) (*store.Store, http.Handler) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st, NewRouter(st)
}

func seedHistoryHTTPRows(t *testing.T, st *store.Store, now time.Time) []store.Record {
	t.Helper()
	ctx := context.Background()
	specs := []struct {
		id          string
		key         string
		fingerprint string
		snapshot    string
		created     time.Duration
		expires     time.Duration
	}{
		{"00000000000000000000000000000001", "k", "fp1", `{"v":"first" }`, -3 * time.Hour, -2 * time.Hour},
		{"00000000000000000000000000000002", "k", "fp2", `{"v":"second"}`, -2 * time.Hour, -time.Hour},
		{"00000000000000000000000000000003", "other", "fp2", `{"v":"other"}`, -90 * time.Minute, -30 * time.Minute},
		{"00000000000000000000000000000004", "k", "fp2", `{"v":"active"}`, time.Minute, time.Hour},
	}
	records := make([]store.Record, 0, len(specs))
	for _, spec := range specs {
		createdAt := now.Add(spec.created)
		expiresAt := now.Add(spec.expires)
		record := store.Record{
			ID:                 "rec_" + spec.id,
			IdempotencyKey:     spec.key,
			RequestFingerprint: spec.fingerprint,
			ResponseSnapshot:   []byte(spec.snapshot),
			CreatedAt:          createdAt,
			ExpiresAt:          expiresAt,
		}
		// Insert each generation at its own created_at: by then every older generation of the key
		// has expired (the oldest expires exactly when the next one is created), while the future
		// active row has not been written yet.
		if _, _, err := st.PutRecord(ctx, record, createdAt); err != nil {
			t.Fatalf("seed %s: %v", spec.id, err)
		}
		records = append(records, record)
	}
	return records
}

func base64EncodeJSON(raw string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func TestHistoryRequiresKey(t *testing.T) {
	handler := newAPIRouter(t)

	for _, target := range []string{
		"/v1/idempotency/history",
		"/v1/idempotency/history?key=",
		"/v1/idempotency/history?key=%20%20",
	} {
		response := doJSON(t, handler, http.MethodGet, target, "")
		if response.Code != http.StatusBadRequest ||
			!strings.HasPrefix(response.Body.String(), `{"error":{"code":"invalid_idempotency_record",`) {
			t.Fatalf("%s = %d %s", target, response.Code, response.Body.String())
		}
	}
}

func TestHistoryUnknownKeyIsEmptyPage(t *testing.T) {
	handler := newAPIRouter(t)

	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=missing", "")
	if response.Code != http.StatusOK || response.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("unknown key = %d %s", response.Code, response.Body.String())
	}
}

func TestHistoryReturnsExpiredGenerationsWithFixedShape(t *testing.T) {
	st, handler := historyRouterWithStore(t)
	now := time.Now().UTC()
	seeded := seedHistoryHTTPRows(t, st, now)

	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k&limit=50", "")
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", response.Code, response.Body.String())
	}
	body := response.Body.String()
	if !strings.HasPrefix(body, `{"records":[{"id":"rec_`) {
		t.Fatalf("envelope shape: %s", body)
	}
	if strings.Contains(body, "active") || !strings.Contains(body, `"status":"expired"`) {
		t.Fatalf("history records must carry status expired: %s", body)
	}
	// The first-generation snapshot keeps its original JSON bytes (including the odd whitespace).
	if !strings.Contains(body, `"response_snapshot":{"v":"first" }`) {
		t.Fatalf("first snapshot bytes were not preserved: %s", body)
	}

	var decoded struct {
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
	if err := json.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded.Records) != 2 {
		t.Fatalf("records = %d, want the two expired generations only: %s", len(decoded.Records), body)
	}
	if decoded.Records[0].ID != seeded[1].ID || decoded.Records[1].ID != seeded[0].ID {
		t.Fatalf("records must be newest first: %+v", decoded.Records)
	}
	if decoded.NextCursor != "" {
		t.Fatalf("final page cursor = %q, want empty", decoded.NextCursor)
	}
	for _, record := range decoded.Records {
		if record.IdempotencyKey != "k" || record.Status != "expired" {
			t.Fatalf("record leaked another key or wrong status: %+v", record)
		}
		parsedCreated, err := time.Parse(time.RFC3339, record.CreatedAt)
		if err != nil {
			t.Fatalf("created_at %q is not RFC 3339: %v", record.CreatedAt, err)
		}
		if parsedCreated.Location() != time.UTC {
			t.Fatalf("created_at %q is not UTC", record.CreatedAt)
		}
	}
}

func TestHistoryPaginationKeysetAndValidation(t *testing.T) {
	st, handler := historyRouterWithStore(t)
	now := time.Now().UTC()
	seeded := seedHistoryHTTPRows(t, st, now)

	page1 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k&limit=1", "")
	var first struct {
		Records    []struct{ ID string } `json:"records"`
		NextCursor string                `json:"next_cursor"`
	}
	if err := json.Unmarshal(page1.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode page1: %v", err)
	}
	if len(first.Records) != 1 || first.Records[0].ID != seeded[1].ID || first.NextCursor == "" {
		t.Fatalf("page1 = %s", page1.Body.String())
	}

	page2 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k&limit=1&cursor="+url.QueryEscape(first.NextCursor), "")
	var second struct {
		Records    []struct{ ID string } `json:"records"`
		NextCursor string                `json:"next_cursor"`
	}
	if err := json.Unmarshal(page2.Body.Bytes(), &second); err != nil {
		t.Fatalf("decode page2: %v", err)
	}
	if len(second.Records) != 1 || second.Records[0].ID != seeded[0].ID || second.NextCursor != "" {
		t.Fatalf("page2 = %s", page2.Body.String())
	}

	cases := []struct {
		name   string
		target string
		code   string
	}{
		{"garbage cursor", "/v1/idempotency/history?key=k&cursor=!!!not-base64", "invalid_cursor"},
		{"json missing fields", "/v1/idempotency/history?key=k&cursor=" + url.QueryEscape(base64EncodeJSON(`{}`)), "invalid_cursor"},
		{"malformed id", "/v1/idempotency/history?key=k&cursor=" + url.QueryEscape(base64EncodeJSON(`{"created_at":"2026-10-02T10:00:00Z","id":"bogus"}`)), "invalid_cursor"},
		{"unknown id", "/v1/idempotency/history?key=k&cursor=" + url.QueryEscape(base64EncodeJSON(`{"created_at":"2026-10-02T10:00:00Z","id":"rec_00000000000000000000000000000099"}`)), "invalid_cursor"},
		{"cursor from another key", "/v1/idempotency/history?key=k&cursor=" + url.QueryEscape(base64EncodeJSON(`{"created_at":"`+seeded[2].CreatedAt.Format(time.RFC3339Nano)+`","id":"`+seeded[2].ID+`"}`)), "invalid_cursor"},
		{"bad limit", "/v1/idempotency/history?key=k&limit=101", "invalid_idempotency_record"},
		{"zero limit", "/v1/idempotency/history?key=k&limit=0", "invalid_idempotency_record"},
		{"non-decimal limit", "/v1/idempotency/history?key=k&limit=1.5", "invalid_idempotency_record"},
		{"bad time", "/v1/idempotency/history?key=k&expires_before=nope", "invalid_idempotency_record"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := doJSON(t, handler, http.MethodGet, tc.target, "")
			wantStatus := http.StatusBadRequest
			if response.Code != wantStatus ||
				!strings.HasPrefix(response.Body.String(), `{"error":{"code":"`+tc.code+`",`) {
				t.Fatalf("%s = %d %s, want %d code %s", tc.name, response.Code, response.Body.String(), wantStatus, tc.code)
			}
			body := response.Body.String()
			for _, leaked := range []string{"SELECT", "INSERT", "sqlite", ".go", "/", "goroutine"} {
				if strings.Contains(body, leaked) {
					t.Fatalf("error message leaked %q: %s", leaked, body)
				}
			}
		})
	}
}

func TestHistoryFiltersDoNotLeakAcrossKeysOrActiveRows(t *testing.T) {
	st, handler := historyRouterWithStore(t)
	now := time.Now().UTC()
	seedHistoryHTTPRows(t, st, now)

	byFingerprint := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k&request_fingerprint=fp2&limit=50", "")
	if strings.Count(byFingerprint.Body.String(), `"id"`) != 1 ||
		!strings.Contains(byFingerprint.Body.String(), `"v":"second"`) ||
		strings.Contains(byFingerprint.Body.String(), `"v":"active"`) {
		t.Fatalf("fingerprint filter leaked rows: %s", byFingerprint.Body.String())
	}

	whitespace := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k&request_fingerprint=fp1%20", "")
	if whitespace.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("whitespace fingerprint must match literally: %s", whitespace.Body.String())
	}

	boundary := now.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	strictBefore := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k&expires_before="+url.QueryEscape(boundary), "")
	if strictBefore.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("expires_before must be strict: %s", strictBefore.Body.String())
	}

	strictAfter := doJSON(t, handler, http.MethodGet, "/v1/idempotency/history?key=k&expires_after="+url.QueryEscape(boundary), "")
	if strings.Count(strictAfter.Body.String(), `"id"`) != 1 || !strings.Contains(strictAfter.Body.String(), `"v":"second"`) {
		t.Fatalf("expires_after must be strict: %s", strictAfter.Body.String())
	}

	// The active endpoints still hide expired snapshots entirely.
	getRecord := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records/k", "")
	if getRecord.Code != http.StatusOK || !strings.Contains(getRecord.Body.String(), `"v":"active"`) ||
		strings.Contains(getRecord.Body.String(), "first") {
		t.Fatalf("active lookup changed semantics: %d %s", getRecord.Code, getRecord.Body.String())
	}
	list := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?key=k", "")
	if strings.Count(list.Body.String(), `"id"`) != 1 || !strings.Contains(list.Body.String(), `"v":"active"`) {
		t.Fatalf("active listing leaked history: %s", list.Body.String())
	}
}
