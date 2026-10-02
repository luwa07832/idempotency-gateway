package store

import (
	"context"
	"testing"
	"time"
)

func TestRecordByIDReturnsAnyGeneration(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	active := makeRecord(t, "k1", "fp1", `{"v":"active"}`, now, now.Add(time.Hour))
	if _, outcome, err := st.PutRecord(ctx, active, now); err != nil || outcome != PutCreated {
		t.Fatalf("seed active: outcome=%d err=%v", outcome, err)
	}
	expired := makeRecord(t, "k2", "fp2", `{"v":"expired"}`, now.Add(-2*time.Hour), now.Add(-time.Hour))
	if saved, _, err := st.PutRecord(ctx, expired, now); err != nil {
		t.Fatalf("seed expired: %v", err)
	} else {
		expired = *saved
	}

	gotActive, err := st.RecordByID(ctx, active.ID)
	if err != nil || gotActive == nil {
		t.Fatalf("active lookup: record=%v err=%v", gotActive, err)
	}
	if gotActive.ID != active.ID || string(gotActive.ResponseSnapshot) != `{"v":"active"}` {
		t.Fatalf("active record mismatch: %+v", gotActive)
	}

	gotExpired, err := st.RecordByID(ctx, expired.ID)
	if err != nil || gotExpired == nil {
		t.Fatalf("expired lookup: record=%v err=%v", gotExpired, err)
	}
	if gotExpired.ID != expired.ID || !gotExpired.ExpiresAt.Equal(expired.ExpiresAt) {
		t.Fatalf("expired record mismatch: %+v", gotExpired)
	}

	missing, err := st.RecordByID(ctx, "rec_0123456789abcdef0123456789abcdef")
	if err != nil || missing != nil {
		t.Fatalf("unknown id: record=%v err=%v", missing, err)
	}
}

func TestIsRecordIDShape(t *testing.T) {
	cases := map[string]bool{
		"rec_0123456789abcdef0123456789abcdef":  true,
		"rec_0000000000000000000000000000000a":  true,
		"rec_0123456789abcdef0123456789abcde":   false,
		"rec_0123456789abcdef0123456789abcdef0": false,
		"REC_0123456789abcdef0123456789abcdef":  false,
		"rec_0123456789ABCDEF0123456789abcdef":  false,
		"0123456789abcdef0123456789abcdef":      false,
		"rec_zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz":  false,
		"":                                      false,
	}
	for id, want := range cases {
		if got := IsRecordID(id); got != want {
			t.Errorf("IsRecordID(%q) = %v, want %v", id, got, want)
		}
	}
}
