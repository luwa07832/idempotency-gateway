package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

const cursorVersion = "v1"

type cursorPayload struct {
	Version     string `json:"v"`
	CreatedNano int64  `json:"created_nano"`
	RecordID    string `json:"record_id"`
}

func encodeCursor(position store.Cursor) (string, error) {
	payload, err := json.Marshal(cursorPayload{
		Version:     cursorVersion,
		CreatedNano: position.CreatedAt.UnixNano(),
		RecordID:    position.RecordID,
	})
	if err != nil {
		return "", err
	}
	return cursorVersion + "." + base64.RawURLEncoding.EncodeToString(payload), nil
}

func decodeCursor(encoded string) (store.Cursor, error) {
	version, data, ok := strings.Cut(encoded, ".")
	if !ok || version != cursorVersion {
		return store.Cursor{}, errors.New("cursor version mismatch")
	}
	raw, err := base64.RawURLEncoding.DecodeString(data)
	if err != nil {
		return store.Cursor{}, err
	}
	var payload cursorPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		return store.Cursor{}, err
	}
	if payload.Version != cursorVersion || payload.RecordID == "" || payload.CreatedNano <= 0 {
		return store.Cursor{}, errors.New("cursor payload invalid")
	}
	return store.Cursor{CreatedAt: time.Unix(0, payload.CreatedNano).UTC(), RecordID: payload.RecordID}, nil
}
