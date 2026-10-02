package api

import (
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
)

func TestConflictsFiltersExact(t *testing.T) {
	handler := newAPIRouter(t)
	recordIDK1, _ := createConflict(t, handler, "k1", "fp-a")
	recordIDK2, _ := createConflict(t, handler, "k2", "fp-b")
	createConflict(t, handler, "k1", "fpA")

	byKey := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?key=k1", "")
	events, _ := decodeEvents(t, byKey.Body.String())
	if len(events) != 2 {
		t.Fatalf("key filter events = %d: %s", len(events), byKey.Body.String())
	}
	for _, event := range events {
		if event.IdempotencyKey != "k1" {
			t.Fatalf("key filter leaked: %+v", event)
		}
	}

	caseSensitive := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?observed_request_fingerprint=fp-a", "")
	events, _ = decodeEvents(t, caseSensitive.Body.String())
	if len(events) != 1 || events[0].ObservedRequestFingerprint != "fp-a" {
		t.Fatalf("observed exact filter: %s", caseSensitive.Body.String())
	}
	upper := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?observed_request_fingerprint=fpA", "")
	events, _ = decodeEvents(t, upper.Body.String())
	if len(events) != 1 || events[0].ObservedRequestFingerprint != "fpA" {
		t.Fatalf("observed filter must be case-sensitive: %s", upper.Body.String())
	}
	whitespace := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?observed_request_fingerprint=fp-a%20", "")
	if whitespace.Body.String() != `{"events":[],"next_cursor":""}` {
		t.Fatalf("whitespace observed value must match literally: %s", whitespace.Body.String())
	}

	byRecord := doJSON(t, handler, http.MethodGet,
		"/v1/idempotency/conflicts?existing_record_id="+url.QueryEscape(recordIDK2), "")
	events, _ = decodeEvents(t, byRecord.Body.String())
	if len(events) != 1 || events[0].ExistingRecordID != recordIDK2 {
		t.Fatalf("existing_record_id filter: %s", byRecord.Body.String())
	}

	combined := doJSON(t, handler, http.MethodGet,
		"/v1/idempotency/conflicts?key=k1&observed_request_fingerprint=fp-a&existing_record_id="+url.QueryEscape(recordIDK1), "")
	events, _ = decodeEvents(t, combined.Body.String())
	if len(events) != 1 {
		t.Fatalf("combined filter events = %d: %s", len(events), combined.Body.String())
	}
}

func TestConflictsPagination(t *testing.T) {
	handler := newAPIRouter(t)
	const total = 3
	for i := 0; i < total; i++ {
		createConflict(t, handler, "k1", "fp-intruder")
	}

	page1 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?key=k1&limit=1", "")
	first, cursor := decodeEvents(t, page1.Body.String())
	if len(first) != 1 || cursor == "" {
		t.Fatalf("page1 = %s", page1.Body.String())
	}
	page2 := doJSON(t, handler, http.MethodGet,
		"/v1/idempotency/conflicts?key=k1&limit=1&cursor="+url.QueryEscape(cursor), "")
	second, cursor2 := decodeEvents(t, page2.Body.String())
	if len(second) != 1 || second[0].ID == first[0].ID || cursor2 == "" {
		t.Fatalf("page2 = %s", page2.Body.String())
	}
	page3 := doJSON(t, handler, http.MethodGet,
		"/v1/idempotency/conflicts?key=k1&limit=1&cursor="+url.QueryEscape(cursor2), "")
	third, cursor3 := decodeEvents(t, page3.Body.String())
	if len(third) != 1 || cursor3 != "" || third[0].ID == first[0].ID || third[0].ID == second[0].ID {
		t.Fatalf("page3 = %s", page3.Body.String())
	}
}

func TestConflictsRejectsBadQueryParams(t *testing.T) {
	handler := newAPIRouter(t)
	cases := []struct {
		name   string
		target string
		code   string
	}{
		{"limit zero", "/v1/idempotency/conflicts?limit=0", "invalid_conflict_query"},
		{"limit over 100", "/v1/idempotency/conflicts?limit=101", "invalid_conflict_query"},
		{"limit negative", "/v1/idempotency/conflicts?limit=-1", "invalid_conflict_query"},
		{"limit decimal", "/v1/idempotency/conflicts?limit=1.5", "invalid_conflict_query"},
		{"limit text", "/v1/idempotency/conflicts?limit=abc", "invalid_conflict_query"},
		{"garbage cursor", "/v1/idempotency/conflicts?cursor=!!!not-base64", "invalid_cursor"},
		{"cursor missing fields", "/v1/idempotency/conflicts?cursor=" + url.QueryEscape(base64EncodeJSON(`{}`)), "invalid_cursor"},
		{"cursor bad id shape", "/v1/idempotency/conflicts?cursor=" +
			url.QueryEscape(base64EncodeJSON(`{"created_at":"2026-10-02T10:00:00Z","id":"bogus"}`)), "invalid_cursor"},
		{"cursor unknown event", "/v1/idempotency/conflicts?cursor=" +
			url.QueryEscape(base64EncodeJSON(`{"created_at":"2026-10-02T10:00:00Z","id":"con_00000000000000000000000000000099"}`)), "invalid_cursor"},
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
					t.Fatalf("error leaked %q: %s", leaked, body)
				}
			}
		})
	}
}

func TestConflictsCursorFilterMismatchIsInvalidCursor(t *testing.T) {
	handler := newAPIRouter(t)
	_, _ = createConflict(t, handler, "k1", "fp-a")
	recordID, _ := createConflict(t, handler, "k1", "fp-c")

	list := doJSON(t, handler, http.MethodGet, "/v1/idempotency/conflicts?key=k1&limit=1", "")
	_, cursor := decodeEvents(t, list.Body.String())
	if cursor == "" {
		t.Fatalf("expected a cursor: %s", list.Body.String())
	}

	// Replaying a k1 cursor under a different filter must not silently return a reshaped page.
	mismatch := doJSON(t, handler, http.MethodGet,
		"/v1/idempotency/conflicts?key=other&cursor="+url.QueryEscape(cursor), "")
	if mismatch.Code != http.StatusBadRequest ||
		!strings.HasPrefix(mismatch.Body.String(), `{"error":{"code":"invalid_cursor",`) {
		t.Fatalf("filter-mismatched cursor = %d %s", mismatch.Code, mismatch.Body.String())
	}
	byRecord := doJSON(t, handler, http.MethodGet,
		"/v1/idempotency/conflicts?existing_record_id=rec_00000000000000000000000000000099&cursor="+
			url.QueryEscape(cursor), "")
	if byRecord.Code != http.StatusBadRequest ||
		!strings.HasPrefix(byRecord.Body.String(), `{"error":{"code":"invalid_cursor",`) {
		t.Fatalf("record-filter cursor = %d %s", byRecord.Code, byRecord.Body.String())
	}
	_ = recordID
}

func TestConcurrentConflictsEachEmitOneEvent(t *testing.T) {
	const instances = 8
	routers, _ := newSharedRouters(t, instances)

	// Seed the winning record through the first instance.
	seed := doJSON(t, routers[0], http.MethodPost, "/v1/idempotency/records",
		submitBody("conflict-audit-key", "fp-winner", `{"price":100}`, ""))
	if seed.Code != http.StatusOK {
		t.Fatalf("seed: %d %s", seed.Code, seed.Body.String())
	}

	type result struct {
		status int
		body   string
	}
	results := make([]result, instances)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, router := range routers {
		wg.Add(1)
		go func(i int, router http.Handler) {
			defer wg.Done()
			<-start
			results[i] = func() result {
				recorder := doJSON(t, router, http.MethodPost, "/v1/idempotency/records",
					submitBodyWithFingerprint("conflict-audit-key", "fp-loser-"+string(rune('a'+i))))
				return result{status: recorder.Code, body: recorder.Body.String()}
			}()
		}(i, router)
	}
	close(start)
	wg.Wait()

	conflicts := 0
	for _, result := range results {
		if result.status != http.StatusConflict {
			t.Fatalf("status = %d, want 409: %s", result.status, result.body)
		}
		conflicts++
	}
	if conflicts != instances {
		t.Fatalf("conflicts = %d, want %d", conflicts, instances)
	}

	list := doJSON(t, routers[0], http.MethodGet, "/v1/idempotency/conflicts?key=conflict-audit-key&limit=100", "")
	if list.Code != http.StatusOK {
		t.Fatalf("list = %d %s", list.Code, list.Body.String())
	}
	events, _ := decodeEvents(t, list.Body.String())
	if len(events) != instances {
		t.Fatalf("events = %d, want one per 409 request (%d): %s", len(events), instances, list.Body.String())
	}
	seen := map[string]bool{}
	for _, event := range events {
		if seen[event.ID] {
			t.Fatalf("duplicate event id: %s", event.ID)
		}
		seen[event.ID] = true
		if event.IdempotencyKey != "conflict-audit-key" || event.ExistingRequestFingerprint != "fp-winner" {
			t.Fatalf("event identity mismatch: %+v", event)
		}
	}
}
