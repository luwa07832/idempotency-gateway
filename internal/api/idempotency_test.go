package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
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

func TestListFiltersByRequestFingerprint(t *testing.T) {
	handler := newAPIRouter(t)
	seeds := []struct {
		key         string
		fingerprint string
	}{
		{"shared-a", "alpha"},
		{"shared-b", "alpha"},
		{"upper", "ALPHA"},
		{"other", "beta"},
	}
	for _, seed := range seeds {
		response := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody(seed.key, seed.fingerprint, `{}`, ""))
		if response.Code != http.StatusOK {
			t.Fatalf("seed %s: %d %s", seed.key, response.Code, response.Body.String())
		}
	}

	decodeKeys := func(body string) ([]string, string) {
		var parsed struct {
			Records []struct {
				IdempotencyKey string `json:"idempotency_key"`
			} `json:"records"`
			NextCursor string `json:"next_cursor"`
		}
		if err := json.Unmarshal([]byte(body), &parsed); err != nil {
			t.Fatalf("decode list body %q: %v", body, err)
		}
		keys := make([]string, len(parsed.Records))
		for i := range parsed.Records {
			keys[i] = parsed.Records[i].IdempotencyKey
		}
		return keys, parsed.NextCursor
	}

	matched, cursor := decodeKeys(doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?request_fingerprint=alpha", "").Body.String())
	if len(matched) != 2 || matched[0] != "shared-b" || matched[1] != "shared-a" || cursor != "" {
		t.Fatalf("fingerprint filter = %v cursor=%q, want shared-b, shared-a with no next cursor", matched, cursor)
	}

	if keys, _ := decodeKeys(doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?request_fingerprint=ALPHA", "").Body.String()); len(keys) != 1 || keys[0] != "upper" {
		t.Fatalf("fingerprint filter must be case-sensitive: %v", keys)
	}

	if body := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?request_fingerprint=alp", "").Body.String(); body != `{"records":[],"next_cursor":""}` {
		t.Fatalf("fingerprint filter must not prefix-match: %s", body)
	}

	if body := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?request_fingerprint=%20alpha%20", "").Body.String(); body != `{"records":[],"next_cursor":""}` {
		t.Fatalf("fingerprint filter must keep whitespace verbatim: %s", body)
	}

	emptyParam := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?request_fingerprint=", "")
	if keys, _ := decodeKeys(emptyParam.Body.String()); len(keys) != 4 {
		t.Fatalf("empty fingerprint must behave like the baseline: %v", keys)
	}

	combined := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?key=shared-a&request_fingerprint=alpha", "")
	if strings.Count(combined.Body.String(), `"idempotency_key"`) != 1 || !strings.Contains(combined.Body.String(), "shared-a") {
		t.Fatalf("fingerprint+key filter: %s", combined.Body.String())
	}

	page1 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?request_fingerprint=alpha&limit=1", "")
	page1Keys, next := decodeKeys(page1.Body.String())
	if len(page1Keys) != 1 || page1Keys[0] != "shared-b" || next == "" {
		t.Fatalf("fingerprint page1 = %v cursor=%q", page1Keys, next)
	}
	page2 := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?request_fingerprint=alpha&limit=1&cursor="+url.QueryEscape(next), "")
	page2Keys, page2Cursor := decodeKeys(page2.Body.String())
	if len(page2Keys) != 1 || page2Keys[0] != "shared-a" || page2Cursor != "" {
		t.Fatalf("fingerprint page2 = %v cursor=%q", page2Keys, page2Cursor)
	}

	expired := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?status=expired&request_fingerprint=alpha", "")
	if expired.Code != http.StatusOK || expired.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("expired status with fingerprint = %d %s", expired.Code, expired.Body.String())
	}

	invalidCases := []struct {
		target string
		code   string
	}{
		{"/v1/idempotency/records?request_fingerprint=alpha&limit=0", "invalid_idempotency_record"},
		{"/v1/idempotency/records?request_fingerprint=alpha&status=weird", "invalid_idempotency_record"},
		{"/v1/idempotency/records?request_fingerprint=alpha&expires_before=not-a-time", "invalid_idempotency_record"},
		{"/v1/idempotency/records?request_fingerprint=alpha&cursor=not-base64!!", "invalid_cursor"},
	}
	for _, tc := range invalidCases {
		response := doJSON(t, handler, http.MethodGet, tc.target, "")
		if response.Code != http.StatusBadRequest ||
			!strings.HasPrefix(response.Body.String(), `{"error":{"code":"`+tc.code+`",`) {
			t.Fatalf("%s = %d %s", tc.target, response.Code, response.Body.String())
		}
	}

	rawFingerprint := `fp space&eq=1`
	created := doJSON(t, handler, http.MethodPost, "/v1/idempotency/records", submitBody("special", rawFingerprint, `{}`, ""))
	if created.Code != http.StatusOK {
		t.Fatalf("seed special fingerprint: %d %s", created.Code, created.Body.String())
	}
	special := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?request_fingerprint="+url.QueryEscape(rawFingerprint), "")
	if strings.Count(special.Body.String(), `"idempotency_key"`) != 1 || !strings.Contains(special.Body.String(), "special") {
		t.Fatalf("special-character fingerprint filter: %s", special.Body.String())
	}
}

func TestListFingerprintHidesExpiredRows(t *testing.T) {
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
	expired := store.Record{
		ID:                 expiredID,
		IdempotencyKey:     "gone",
		RequestFingerprint: "alpha",
		ResponseSnapshot:   []byte(`"secret"`),
		CreatedAt:          now.Add(-2 * time.Hour),
		ExpiresAt:          now.Add(-time.Hour),
	}
	if _, _, err := st.PutRecord(context.Background(), expired, now); err != nil {
		t.Fatalf("seed expired row: %v", err)
	}

	handler := NewRouter(st)
	response := doJSON(t, handler, http.MethodGet, "/v1/idempotency/records?request_fingerprint=alpha", "")
	if response.Code != http.StatusOK || response.Body.String() != `{"records":[],"next_cursor":""}` {
		t.Fatalf("expired snapshot leaked through fingerprint filter: %d %s", response.Code, response.Body.String())
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
