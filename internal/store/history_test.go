package store

import (
	"context"
	"testing"
	"time"
)

func seedHistoryRows(t *testing.T, st *Store, now time.Time) []Record {
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
		{"00000000000000000000000000000001", "gen", "fp1", `{"v":1}`, -5 * time.Hour, -4 * time.Hour},
		{"00000000000000000000000000000002", "gen", "fp2", `{"v":2}`, -3 * time.Hour, -2 * time.Hour},
		{"00000000000000000000000000000003", "gen", "fp2", `{"v":3}`, -time.Hour, -time.Minute},
		{"00000000000000000000000000000004", "gen", "fp2", `{"v":4}`, time.Minute, time.Hour},
		{"00000000000000000000000000000005", "other", "fp2", `{"v":5}`, -6 * time.Hour, -time.Minute},
		{"00000000000000000000000000000006", "gen", "FP2", `{"v":6}`, -90 * time.Minute, -75 * time.Minute},
	}
	records := make([]Record, 0, len(specs))
	for _, spec := range specs {
		createdAt := now.Add(spec.created)
		expiresAt := now.Add(spec.expires)
		record := Record{
			ID:                 "rec_" + spec.id,
			IdempotencyKey:     spec.key,
			RequestFingerprint: spec.fingerprint,
			ResponseSnapshot:   []byte(spec.snapshot),
			CreatedAt:          createdAt,
			ExpiresAt:          expiresAt,
		}
		// These specs describe non-chronological generations (including a future active row that
		// coexists with older expired ones), which the check-then-insert write path cannot build.
		// The history query is read-only over the same rows, so seed them directly.
		if _, err := st.db.ExecContext(ctx, `
INSERT INTO idempotency_records
	(id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`,
			record.ID, record.IdempotencyKey, record.RequestFingerprint,
			string(record.ResponseSnapshot), createdAt.UnixNano(), expiresAt.UnixNano()); err != nil {
			t.Fatalf("insert %s: %v", spec.id, err)
		}
		records = append(records, record)
	}
	return records
}

func TestListHistoryReturnsOnlyExpiredRowsForKeyNewestFirst(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	seeded := seedHistoryRows(t, st, now)

	rows, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "gen", Limit: 50}, now)
	if err != nil {
		t.Fatalf("list history: %v", err)
	}
	// gen has four expired generations plus one active row; id05 belongs to another key, so
	// exactly the four expired rows return, newest created first.
	if len(rows) != 4 {
		t.Fatalf("rows = %v, want 4 expired generations", rows)
	}
	wantIDs := []string{seeded[2].ID, seeded[5].ID, seeded[1].ID, seeded[0].ID}
	for i, want := range wantIDs {
		if rows[i].ID != want {
			t.Fatalf("row %d = %s, want %s; rows = %v", i, rows[i].ID, want, rows)
		}
	}
	if rows[0].IdempotencyKey != "gen" || string(rows[0].ResponseSnapshot) != `{"v":3}` {
		t.Fatalf("newest expired row identity lost: %+v", rows[0])
	}
	if rows[3].CreatedAt.After(rows[2].CreatedAt) || rows[2].CreatedAt.After(rows[1].CreatedAt) || rows[1].CreatedAt.After(rows[0].CreatedAt) {
		t.Fatalf("history not ordered newest first: %v", rows)
	}
}

func TestListHistoryFiltersAreExactAndExclusive(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	seedHistoryRows(t, st, now)

	byFingerprint, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:     "gen",
		RequestFingerprint: "fp2",
		Limit:              50,
	}, now)
	if err != nil || len(byFingerprint) != 2 {
		t.Fatalf("fingerprint filter rows=%v err=%v", byFingerprint, err)
	}
	if string(byFingerprint[0].ResponseSnapshot) != `{"v":3}` || string(byFingerprint[1].ResponseSnapshot) != `{"v":2}` {
		t.Fatalf("fingerprint filter ordered rows = %v", byFingerprint)
	}

	caseSensitive, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:     "gen",
		RequestFingerprint: "FP2",
		Limit:              50,
	}, now)
	if err != nil || len(caseSensitive) != 1 || string(caseSensitive[0].ResponseSnapshot) != `{"v":6}` {
		t.Fatalf("fingerprint must match character-for-character: rows=%v err=%v", caseSensitive, err)
	}

	prefix, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:     "gen",
		RequestFingerprint: "fp",
		Limit:              50,
	}, now)
	if err != nil || len(prefix) != 0 {
		t.Fatalf("fingerprint must not prefix-match: rows=%v err=%v", prefix, err)
	}

	// expires_at strictly before 2026-10-02T10:00:00Z excludes the row expiring exactly then.
	boundary := time.Date(2026, 10, 2, 10, 0, 0, 0, time.UTC)
	before, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey: "gen",
		ExpiresBefore:  &boundary,
		Limit:          50,
	}, now)
	if err != nil || len(before) != 1 || string(before[0].ResponseSnapshot) != `{"v":1}` {
		t.Fatalf("expires_before must be strict: rows=%v err=%v", before, err)
	}

	// expires_at strictly after the same boundary excludes the 08:00 row while keeping the rows
	// expiring at 10:00 and 11:45.
	after, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey: "gen",
		ExpiresAfter:   &boundary,
		Limit:          50,
	}, now)
	if err != nil || len(after) != 2 {
		t.Fatalf("expires_after must be strict: rows=%v err=%v", after, err)
	}

	// A row expiring exactly at the query instant is expired.
	expiringNow := time.Date(2026, 10, 2, 11, 45, 0, 0, time.UTC)
	equal, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "gen", Limit: 50}, expiringNow)
	if err != nil || len(equal) != 3 ||
		string(equal[0].ResponseSnapshot) != `{"v":6}` ||
		string(equal[1].ResponseSnapshot) != `{"v":2}` ||
		string(equal[2].ResponseSnapshot) != `{"v":1}` {
		t.Fatalf("expires_at == now must count as expired: rows=%v err=%v", equal, err)
	}
}

func TestListHistoryPaginationStableAndCursorValidated(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	seeded := seedHistoryRows(t, st, now)

	page1, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "gen", Limit: 2}, now)
	if err != nil || len(page1) != 2 {
		t.Fatalf("page1 rows=%v err=%v", page1, err)
	}
	page2, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "gen",
		Limit:           2,
		CursorCreatedAt: page1[1].CreatedAt,
		CursorID:        page1[1].ID,
	}, now)
	if err != nil || len(page2) != 2 || page2[0].ID != seeded[1].ID || page2[1].ID != seeded[0].ID {
		t.Fatalf("page2 rows=%v err=%v", page2, err)
	}
	page3, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "gen",
		Limit:           2,
		CursorCreatedAt: page2[1].CreatedAt,
		CursorID:        page2[1].ID,
	}, now)
	if err != nil || len(page3) != 0 {
		t.Fatalf("page3 rows=%v err=%v, want the end of history", page3, err)
	}

	seen := map[string]bool{}
	for _, row := range append(append(page1, page2...), page3...) {
		if seen[row.ID] {
			t.Fatalf("cursor pagination repeated %s", row.ID)
		}
		seen[row.ID] = true
	}

	// A cursor aimed at a different key references no matching row.
	if _, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "other",
		Limit:           2,
		CursorCreatedAt: page1[1].CreatedAt,
		CursorID:        page1[1].ID,
	}, now); err != ErrCursorTarget {
		t.Fatalf("cross-key cursor err = %v, want ErrCursorTarget", err)
	}

	// A cursor aimed at the still-active gen row cannot anchor an expired page.
	if _, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "gen",
		Limit:           2,
		CursorCreatedAt: seeded[3].CreatedAt,
		CursorID:        seeded[3].ID,
	}, now); err != ErrCursorTarget {
		t.Fatalf("active-row cursor err = %v, want ErrCursorTarget", err)
	}

	// A cursor whose id lacks the fixed shape is rejected before touching the data.
	if _, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "gen",
		Limit:           2,
		CursorCreatedAt: page1[1].CreatedAt,
		CursorID:        "not-an-id",
	}, now); err != ErrInvalidID {
		t.Fatalf("malformed cursor id err = %v, want ErrInvalidID", err)
	}

	// A well-shaped id that does not exist is an abnormal target.
	if _, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "gen",
		Limit:           2,
		CursorCreatedAt: page1[1].CreatedAt,
		CursorID:        "rec_" + "00000000000000000000000000000000",
	}, now); err != ErrCursorTarget {
		t.Fatalf("missing cursor row err = %v, want ErrCursorTarget", err)
	}
}

func TestListHistoryTieBreaksByIDDESC(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	created := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	now := created.Add(2 * time.Hour)

	// The write path never holds two active rows per key, so two expired generations sharing one
	// created_at are seeded with a direct insert.
	lowerID := "rec_" + "11111111111111111111111111111111"
	higherID := "rec_" + "22222222222222222222222222222222"
	for _, spec := range []struct{ id, snapshot string }{
		{lowerID, `"a"`},
		{higherID, `"b"`},
	} {
		if _, err := st.db.ExecContext(ctx, `
INSERT INTO idempotency_records
	(id, idempotency_key, request_fingerprint, response_snapshot, created_at_ns, expires_at_ns)
VALUES (?, ?, ?, ?, ?, ?)`,
			spec.id, "tie", "fp", spec.snapshot, created.UnixNano(), created.Add(time.Hour).UnixNano()); err != nil {
			t.Fatalf("insert tie row: %v", err)
		}
	}

	page1, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "tie", Limit: 1}, now)
	if err != nil || len(page1) != 1 {
		t.Fatalf("tie page1 rows=%v err=%v", page1, err)
	}
	if page1[0].ID != higherID {
		t.Fatalf("tie breaker must order id DESC: got %s want %s", page1[0].ID, higherID)
	}
	page2, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "tie",
		Limit:           1,
		CursorCreatedAt: page1[0].CreatedAt,
		CursorID:        page1[0].ID,
	}, now)
	if err != nil || len(page2) != 1 || page2[0].ID != lowerID {
		t.Fatalf("tie page2 rows=%v err=%v", page2, err)
	}
}
