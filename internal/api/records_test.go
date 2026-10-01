package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

type apiHarness struct {
	t      *testing.T
	router http.Handler
}

func newAPIHarness(t *testing.T) *apiHarness {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "service.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return &apiHarness{t: t, router: NewRouter(st)}
}

func (h *apiHarness) do(method, target string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			h.t.Fatalf("marshal: %v", err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	request := httptest.NewRequest(method, target, reader)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	h.router.ServeHTTP(recorder, request)
	return recorder
}

func validSubmitBody() map[string]any {
	return map[string]any{
		"idempotency_key":     "order-1",
		"request_fingerprint": "fp-1",
		"response_snapshot":   map[string]any{"status": "ok", "amount": 42},
		"expires_at":          time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
	}
}

func decodeBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode %q: %v", recorder.Body.String(), err)
	}
	return body
}

func postRecord(t *testing.T, h *apiHarness, body map[string]any) (int, map[string]any) {
	t.Helper()
	recorder := h.do(http.MethodPost, "/v1/idempotency/records", body)
	return recorder.Code, decodeBody(t, recorder)
}

func TestSubmitCreatesAndReplaysFirstSnapshot(t *testing.T) {
	h := newAPIHarness(t)

	statusCode, body := postRecord(t, h, validSubmitBody())
	if statusCode != http.StatusCreated {
		t.Fatalf("create status = %d body = %v", statusCode, body)
	}
	first := body["record"].(map[string]any)
	if first["status"] != "stored" || first["idempotency_key"] != "order-1" || first["request_fingerprint"] != "fp-1" {
		t.Fatalf("unexpected first record: %v", first)
	}
	snapshot := first["response_snapshot"].(map[string]any)
	if snapshot["status"] != "ok" {
		t.Fatalf("snapshot = %v", snapshot)
	}
	recordID := first["record_id"].(string)
	if recordID == "" {
		t.Fatal("record_id missing")
	}

	repeat := validSubmitBody()
	repeat["response_snapshot"] = map[string]any{"status": "overwritten"}
	statusCode, body = postRecord(t, h, repeat)
	if statusCode != http.StatusOK {
		t.Fatalf("replay status = %d body = %v", statusCode, body)
	}
	replayed := body["record"].(map[string]any)
	if replayed["record_id"] != recordID {
		t.Fatalf("replay record id = %v, want %s", replayed["record_id"], recordID)
	}
	if replayed["response_snapshot"].(map[string]any)["status"] != "ok" {
		t.Fatalf("first snapshot was overwritten: %v", replayed["response_snapshot"])
	}
	if len(replayed) != len(first) {
		t.Fatalf("replay field set differs: %v vs %v", replayed, first)
	}

	recorder := h.do(http.MethodGet, "/v1/idempotency/records/order-1", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("get status = %d", recorder.Code)
	}
	got := decodeBody(t, recorder)["record"].(map[string]any)
	if got["record_id"] != recordID || got["request_fingerprint"] != "fp-1" {
		t.Fatalf("get returned %v", got)
	}
}

func TestSubmitRejectsFingerprintConflictWithoutMutation(t *testing.T) {
	h := newAPIHarness(t)
	_, first := postRecord(t, h, validSubmitBody())
	firstID := first["record"].(map[string]any)["record_id"].(string)

	conflict := validSubmitBody()
	conflict["request_fingerprint"] = "fp-2"
	recorder := h.do(http.MethodPost, "/v1/idempotency/records", conflict)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("conflict status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	wantBody := `{"error":{"code":"idempotency_fingerprint_conflict","message":"a record for this idempotency key already exists with a different request fingerprint","existing_record_id":"` + firstID + `","existing_request_fingerprint":"fp-1"}}`
	if recorder.Body.String() != wantBody {
		t.Fatalf("conflict body = %s\nwant %s", recorder.Body.String(), wantBody)
	}
	errBody := decodeBody(t, recorder)["error"].(map[string]any)
	if errBody["code"] != "idempotency_fingerprint_conflict" {
		t.Fatalf("code = %v", errBody["code"])
	}
	if errBody["existing_record_id"] != firstID || errBody["existing_request_fingerprint"] != "fp-1" {
		t.Fatalf("conflict body = %v", errBody)
	}

	recorder = h.do(http.MethodGet, "/v1/idempotency/records/order-1", nil)
	got := decodeBody(t, recorder)["record"].(map[string]any)
	if got["record_id"] != firstID || got["request_fingerprint"] != "fp-1" {
		t.Fatalf("original record changed: %v", got)
	}
}

func TestSubmitValidatesParameters(t *testing.T) {
	h := newAPIHarness(t)
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"missing key", func(b map[string]any) { delete(b, "idempotency_key") }},
		{"blank key", func(b map[string]any) { b["idempotency_key"] = "   " }},
		{"missing fingerprint", func(b map[string]any) { delete(b, "request_fingerprint") }},
		{"blank fingerprint", func(b map[string]any) { b["request_fingerprint"] = "" }},
		{"missing expiry", func(b map[string]any) { delete(b, "expires_at") }},
		{"expiry in past", func(b map[string]any) {
			b["expires_at"] = time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
		}},
		{"malformed expiry", func(b map[string]any) { b["expires_at"] = "not-a-time" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := validSubmitBody()
			tc.mutate(body)
			recorder := h.do(http.MethodPost, "/v1/idempotency/records", body)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d body = %s", recorder.Code, recorder.Body.String())
			}
			errBody := decodeBody(t, recorder)["error"].(map[string]any)
			if errBody["code"] != "invalid_idempotency_record" {
				t.Fatalf("code = %v", errBody["code"])
			}
		})
	}

	recorder := h.do(http.MethodGet, "/v1/idempotency/records", nil)
	listBody := decodeBody(t, recorder)
	if len(listBody["records"].([]any)) != 0 {
		t.Fatalf("invalid writes created records: %v", listBody)
	}
}

func TestGetMissingAndExpiredReturnNotFound(t *testing.T) {
	h := newAPIHarness(t)

	recorder := h.do(http.MethodGet, "/v1/idempotency/records/unknown", nil)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("missing status = %d", recorder.Code)
	}
	if decodeBody(t, recorder)["error"].(map[string]any)["code"] != "not_found" {
		t.Fatalf("missing code = %s", recorder.Body.String())
	}

	expired := validSubmitBody()
	expired["idempotency_key"] = "gone"
	expired["expires_at"] = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339)
	recorder = h.do(http.MethodPost, "/v1/idempotency/records", expired)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expired submit status = %d", recorder.Code)
	}

	body := validSubmitBody()
	body["idempotency_key"] = "almost-gone"
	body["expires_at"] = time.Now().UTC().Add(100 * time.Millisecond).Format(time.RFC3339Nano)
	recorder = h.do(http.MethodPost, "/v1/idempotency/records", body)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("short-lived submit status = %d body = %s", recorder.Code, recorder.Body.String())
	}
	time.Sleep(250 * time.Millisecond)
	recorder = h.do(http.MethodGet, "/v1/idempotency/records/almost-gone", nil)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expired get status = %d", recorder.Code)
	}
}

func TestListFiltersPaginatesAndValidatesCursor(t *testing.T) {
	h := newAPIHarness(t)
	for _, key := range []string{"a", "b", "c"} {
		body := validSubmitBody()
		body["idempotency_key"] = key
		recorder := h.do(http.MethodPost, "/v1/idempotency/records", body)
		if recorder.Code != http.StatusCreated {
			t.Fatalf("seed %s: %d %s", key, recorder.Code, recorder.Body.String())
		}
		time.Sleep(5 * time.Millisecond)
	}

	recorder := h.do(http.MethodGet, "/v1/idempotency/records?limit=2", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("list status = %d", recorder.Code)
	}
	page1 := decodeBody(t, recorder)
	records1 := page1["records"].([]any)
	if len(records1) != 2 {
		t.Fatalf("page 1 len = %d", len(records1))
	}
	if records1[0].(map[string]any)["idempotency_key"] != "c" ||
		records1[1].(map[string]any)["idempotency_key"] != "b" {
		t.Fatalf("page 1 order = %v", records1)
	}
	cursor := page1["next_cursor"].(string)
	if cursor == "" {
		t.Fatal("next_cursor missing")
	}

	recorder = h.do(http.MethodGet, "/v1/idempotency/records?limit=2&cursor="+cursor, nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("page 2 status = %d", recorder.Code)
	}
	page2 := decodeBody(t, recorder)
	records2 := page2["records"].([]any)
	if len(records2) != 1 || records2[0].(map[string]any)["idempotency_key"] != "a" {
		t.Fatalf("page 2 = %v", records2)
	}
	if page2["next_cursor"] != "" {
		t.Fatalf("final page cursor = %v", page2["next_cursor"])
	}

	recorder = h.do(http.MethodGet, "/v1/idempotency/records?cursor=not-a-cursor", nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("bad cursor status = %d", recorder.Code)
	}
	if decodeBody(t, recorder)["error"].(map[string]any)["code"] != "invalid_cursor" {
		t.Fatalf("bad cursor body = %s", recorder.Body.String())
	}

	recorder = h.do(http.MethodGet, "/v1/idempotency/records?status=stored&limit=1", nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("filtered list status = %d", recorder.Code)
	}
	if len(decodeBody(t, recorder)["records"].([]any)) != 1 {
		t.Fatalf("status filter failed: %s", recorder.Body.String())
	}

	recorder = h.do(http.MethodGet, "/v1/idempotency/records?status=other", nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("bad status = %d", recorder.Code)
	}
	if decodeBody(t, recorder)["error"].(map[string]any)["code"] != "invalid_idempotency_record" {
		t.Fatalf("bad status body = %s", recorder.Body.String())
	}
}

func TestSameInputYieldsIdenticalReplay(t *testing.T) {
	h := newAPIHarness(t)
	first := h.do(http.MethodPost, "/v1/idempotency/records", validSubmitBody())
	second := h.do(http.MethodPost, "/v1/idempotency/records", validSubmitBody())
	third := h.do(http.MethodPost, "/v1/idempotency/records", validSubmitBody())
	if second.Body.String() != third.Body.String() {
		t.Fatalf("replays differ:\n%s\n%s", second.Body.String(), third.Body.String())
	}
	firstBody := decodeBody(t, first)["record"].(map[string]any)
	replayBody := decodeBody(t, second)["record"].(map[string]any)
	for _, field := range []string{"record_id", "status", "idempotency_key", "request_fingerprint", "response_snapshot", "created_at", "expires_at"} {
		if !reflect.DeepEqual(firstBody[field], replayBody[field]) {
			t.Fatalf("field %s differs: %v vs %v", field, firstBody[field], replayBody[field])
		}
	}
}
