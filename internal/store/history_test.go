package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// seedHistoryRow inserts one row. putNow is the effective time for PutRecord's active-record
// check; pass a time later than every earlier generation's expiry so overlapping generations for
// the same key are all inserted as independent historical rows.
func seedHistoryRow(t *testing.T, st *Store, key, fingerprint, snapshot string, createdAt, expiresAt, putNow time.Time) Record {
	t.Helper()
	record := makeRecord(t, key, fingerprint, snapshot, createdAt, expiresAt)
	if _, _, err := st.PutRecord(context.Background(), record, putNow); err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
	return record
}

func TestListHistoryReturnsOnlyExpiredGenerationsForKey(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	now := base.Add(5 * time.Hour)

	oldest := seedHistoryRow(t, st, "k1", "fp1", `{"v":1}`, base, base.Add(time.Hour), base)
	middle := seedHistoryRow(t, st, "k1", "fp2", `{"v":2}`, base.Add(2*time.Hour), base.Add(3*time.Hour), base.Add(2*time.Hour))
	current := makeRecord(t, "k1", "fp3", `{"v":3}`, base.Add(4*time.Hour), base.Add(6*time.Hour))
	if _, _, err := st.PutRecord(ctx, current, base.Add(4*time.Hour)); err != nil {
		t.Fatalf("put active row: %v", err)
	}
	seedHistoryRow(t, st, "k2", "fp1", `{"v":9}`, base.Add(time.Minute), base.Add(2*time.Hour), now)

	rows, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "k1", Limit: 10}, now)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("history len = %d, want 2 expired generations (active row hidden)", len(rows))
	}
	if rows[0].ID != middle.ID || rows[1].ID != oldest.ID {
		t.Fatalf("history order = [%s %s], want middle then oldest", rows[0].ID, rows[1].ID)
	}
	for _, row := range rows {
		if row.ExpiresAt.After(now) {
			t.Fatalf("history returned a row not yet expired: %+v", row)
		}
	}
	if string(rows[0].ResponseSnapshot) != `{"v":2}` || string(rows[1].ResponseSnapshot) != `{"v":1}` {
		t.Fatalf("first snapshots not preserved byte-for-byte: %s %s", rows[0].ResponseSnapshot, rows[1].ResponseSnapshot)
	}

	// An unknown key yields an empty page rather than an error.
	unknown, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "missing", Limit: 10}, now)
	if err != nil || len(unknown) != 0 {
		t.Fatalf("unknown key: rows=%v err=%v", unknown, err)
	}

	// A row expiring exactly at now is expired and must be visible.
	atBoundary, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "k1", Limit: 10}, middle.ExpiresAt)
	if err != nil || len(atBoundary) != 2 || atBoundary[0].ID != middle.ID || atBoundary[1].ID != oldest.ID {
		t.Fatalf("expiry boundary rows=%v err=%v", atBoundary, err)
	}
}

func TestListHistoryFiltersAndPaginates(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	specs := []struct {
		name        string
		fingerprint string
		created     time.Duration
		expires     time.Duration
	}{
		{"h1", "alpha", 0, 30 * time.Minute},
		{"h2", "alpha", time.Minute, time.Hour},
		{"h3", "ALPHA", 2 * time.Minute, 2 * time.Hour},
		{"h4", "beta", 3 * time.Minute, 90 * time.Minute},
	}
	now := base.Add(3 * time.Hour)
	inserted := make(map[string]Record)
	for _, spec := range specs {
		created := base.Add(spec.created)
		inserted[spec.name] = seedHistoryRow(t, st, "k1", spec.fingerprint, `"snap"`, created, created.Add(spec.expires), now)
	}

	exact, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "k1", RequestFingerprint: "alpha", Limit: 10}, now)
	if err != nil || len(exact) != 2 || exact[0].ID != inserted["h2"].ID || exact[1].ID != inserted["h1"].ID {
		t.Fatalf("exact fingerprint rows=%v err=%v", exact, err)
	}
	prefix, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "k1", RequestFingerprint: "alp", Limit: 10}, now)
	if err != nil || len(prefix) != 0 {
		t.Fatalf("fingerprint must match exactly: rows=%v err=%v", prefix, err)
	}
	emptyFingerprint, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "k1", RequestFingerprint: "", Limit: 10}, now)
	if err != nil || len(emptyFingerprint) != 4 {
		t.Fatalf("empty fingerprint must not filter: rows=%v err=%v", len(emptyFingerprint), err)
	}

	boundary := base.Add(90 * time.Minute)
	before, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "k1", ExpiresBefore: &boundary, Limit: 10}, now)
	if err != nil || len(before) != 2 {
		t.Fatalf("expires_before must be strictly before: rows=%v err=%v", before, err)
	}
	for _, row := range before {
		if !row.ExpiresAt.Before(boundary) {
			t.Fatalf("expires_before leaked %+v", row)
		}
	}
	afterBoundary := base.Add(2 * time.Hour)
	after, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "k1", ExpiresAfter: &afterBoundary, Limit: 10}, now)
	if err != nil || len(after) != 1 || after[0].ID != inserted["h3"].ID {
		t.Fatalf("expires_after must be strictly after: rows=%v err=%v", after, err)
	}

	page1, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "k1", Limit: 2}, now)
	if err != nil || len(page1) != 2 || page1[0].ID != inserted["h4"].ID || page1[1].ID != inserted["h3"].ID {
		t.Fatalf("page1 rows=%v err=%v", page1, err)
	}
	page2, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "k1",
		Limit:           2,
		CursorCreatedAt: page1[1].CreatedAt,
		CursorID:        page1[1].ID,
	}, now)
	if err != nil || len(page2) != 2 {
		t.Fatalf("page2 rows=%v err=%v", page2, err)
	}
	for _, seen := range page1 {
		for _, row := range page2 {
			if row.ID == seen.ID {
				t.Fatalf("pagination duplicated row %s", row.ID)
			}
		}
	}
	if page2[0].ID != inserted["h2"].ID || page2[1].ID != inserted["h1"].ID {
		t.Fatalf("page2 order = %+v", page2)
	}
	page3, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "k1",
		Limit:           2,
		CursorCreatedAt: page2[1].CreatedAt,
		CursorID:        page2[1].ID,
	}, now)
	if err != nil || len(page3) != 0 {
		t.Fatalf("page3 rows=%v err=%v", page3, err)
	}
}

func TestListHistoryCursorStableAcrossInsertsAndExpiries(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	now := base.Add(3 * time.Hour)
	seeded := make([]Record, 3)
	for i := range seeded {
		created := base.Add(time.Duration(i) * time.Minute)
		seeded[i] = seedHistoryRow(t, st, "k1", "fp", `{}`, created, created.Add(time.Hour), now)
	}
	ids := []string{seeded[2].ID, seeded[1].ID, seeded[0].ID}
	page1, err := st.ListHistoryRecords(ctx, HistoryFilter{IdempotencyKey: "k1", Limit: 1}, now)
	if err != nil || len(page1) != 1 {
		t.Fatalf("page1 rows=%v err=%v", page1, err)
	}

	// While paging: a new generation is inserted and another row expires. Neither must shift the
	// already-served page or resurrect a duplicate.
	newRow := makeRecord(t, "k1", "fp", `{"late":true}`, now.Add(time.Minute), now.Add(2*time.Hour))
	if _, _, err := st.PutRecord(ctx, newRow, now.Add(time.Minute)); err != nil {
		t.Fatalf("insert during paging: %v", err)
	}
	page2Now := now.Add(90 * time.Minute)
	page2, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "k1",
		Limit:           1,
		CursorCreatedAt: page1[0].CreatedAt,
		CursorID:        page1[0].ID,
	}, page2Now)
	if err != nil || len(page2) != 1 || page2[0].ID == page1[0].ID {
		t.Fatalf("page2 overlapped page1 after changes: rows=%v err=%v", page2, err)
	}
	if page1[0].ID != ids[0] || page2[0].ID != ids[1] {
		t.Fatalf("pages shifted: page1=%s page2=%s want %s %s", page1[0].ID, page2[0].ID, ids[0], ids[1])
	}
}

func TestListHistoryRejectsAbnormalCursor(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	cases := []struct {
		name      string
		createdAt time.Time
		id        string
	}{
		{"malformed id", now, "rec_x"},
		{"wrong prefix", now, "xyz_" + "0123456789abcdef0123456789abcdef"},
		{"uppercase hex", now, "rec_0123456789ABCDEF0123456789abcdef"},
		{"short hex", now, "rec_0123"},
		{"zero time", time.Time{}, "rec_" + "0123456789abcdef0123456789abcdef"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := st.ListHistoryRecords(ctx, HistoryFilter{
				IdempotencyKey:  "k1",
				CursorCreatedAt: tc.createdAt,
				CursorID:        tc.id,
				Limit:           10,
			}, now)
			if !errors.Is(err, ErrInvalidID) {
				t.Fatalf("err = %v, want ErrInvalidID", err)
			}
		})
	}

	// A well-shaped cursor that points at a record which does not exist is not malformed: the
	// keyset predicate simply returns the remaining rows.
	ghost, err := st.ListHistoryRecords(ctx, HistoryFilter{
		IdempotencyKey:  "k1",
		CursorCreatedAt: now,
		CursorID:        "rec_" + "0123456789abcdef0123456789abcdef",
		Limit:           10,
	}, now)
	if err != nil || len(ghost) != 0 {
		t.Fatalf("ghost cursor rows=%v err=%v", ghost, err)
	}
}

func TestValidRecordIDShape(t *testing.T) {
	valid, err := NewRecordID()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}
	if !ValidRecordID(valid) {
		t.Fatalf("generated id %q rejected", valid)
	}
	for _, invalid := range []string{
		"", "rec_", "rec_x", "rec_0123456789abcdef0123456789abcde",
		"rec_0123456789abcdef0123456789abcdef0",
		"rec_0123456789ABCDEF0123456789abcdef",
		"xyz_0123456789abcdef0123456789abcdef",
	} {
		if ValidRecordID(invalid) {
			t.Fatalf("invalid id %q accepted", invalid)
		}
	}
}
