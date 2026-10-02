package api

import (
	"bytes"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/luwa07832/idempotency-gateway/internal/store"
)

// marshalRecordObject renders one record with the fixed field order while embedding
// response_snapshot's stored bytes verbatim. encoding/json compacts every nested JSON value
// (including json.RawMessage) during marshaling, so the snapshot is concatenated in place rather
// than passed through an encoder; it is guaranteed to be valid JSON at submission time.
func marshalRecordObject(record recordResponse) ([]byte, error) {
	id, err := json.Marshal(record.ID)
	if err != nil {
		return nil, err
	}
	key, err := json.Marshal(record.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	status, err := json.Marshal(record.Status)
	if err != nil {
		return nil, err
	}
	fingerprint, err := json.Marshal(record.RequestFingerprint)
	if err != nil {
		return nil, err
	}
	createdAt, err := json.Marshal(record.CreatedAt)
	if err != nil {
		return nil, err
	}
	expiresAt, err := json.Marshal(record.ExpiresAt)
	if err != nil {
		return nil, err
	}
	snapshot := record.ResponseSnapshot
	if len(snapshot) == 0 {
		snapshot = []byte("null")
	}

	var body bytes.Buffer
	body.Grow(len(id) + len(key) + len(status) + len(fingerprint) + len(createdAt) + len(expiresAt) + len(snapshot) + 128)
	body.WriteString(`{"id":`)
	body.Write(id)
	body.WriteString(`,"idempotency_key":`)
	body.Write(key)
	body.WriteString(`,"status":`)
	body.Write(status)
	body.WriteString(`,"request_fingerprint":`)
	body.Write(fingerprint)
	body.WriteString(`,"response_snapshot":`)
	body.Write(snapshot)
	body.WriteString(`,"created_at":`)
	body.Write(createdAt)
	body.WriteString(`,"expires_at":`)
	body.Write(expiresAt)
	body.WriteByte('}')
	return body.Bytes(), nil
}

// writeRecordEnvelope writes {"record":{...}} with the snapshot bytes untouched.
func writeRecordEnvelope(c *gin.Context, record recordResponse) {
	object, err := marshalRecordObject(record)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	body := make([]byte, 0, len(object)+11)
	body = append(body, `{"record":`...)
	body = append(body, object...)
	body = append(body, '}')
	c.Data(http.StatusOK, "application/json; charset=utf-8", body)
}

// writePreviewCreated writes the preview observation for a key with no active record. Expired
// historical rows are not replayed, so this is the missing-key and all-expired-key result alike.
func writePreviewCreated(c *gin.Context) {
	c.Data(http.StatusOK, "application/json; charset=utf-8", []byte(`{"outcome":"created"}`))
}

// writePreviewReplayed writes {"outcome":"replayed","record":{...}} with the stored record in
// its fixed field order and the first response_snapshot embedded byte-for-byte.
func writePreviewReplayed(c *gin.Context, record recordResponse) {
	object, err := marshalRecordObject(record)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	body := make([]byte, 0, len(object)+23)
	body = append(body, `{"outcome":"replayed","record":`...)
	body = append(body, object...)
	body = append(body, '}')
	c.Data(http.StatusOK, "application/json; charset=utf-8", body)
}

// writePreviewConflict writes {"outcome":"conflict","record_id":"...","request_fingerprint":"..."}
// from the existing active record only; its snapshot is never part of the conflict observation.
func writePreviewConflict(c *gin.Context, record *store.Record) {
	recordID, err := json.Marshal(record.ID)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	fingerprint, err := json.Marshal(record.RequestFingerprint)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}
	var body bytes.Buffer
	body.Grow(len(recordID) + len(fingerprint) + 48)
	body.WriteString(`{"outcome":"conflict","record_id":`)
	body.Write(recordID)
	body.WriteString(`,"request_fingerprint":`)
	body.Write(fingerprint)
	body.WriteByte('}')
	c.Data(http.StatusOK, "application/json; charset=utf-8", body.Bytes())
}

// writeListResponse writes {"records":[...],"next_cursor":...} with every snapshot untouched.
func writeListResponse(c *gin.Context, records []recordResponse, nextCursor string) {
	encodedCursor, err := json.Marshal(nextCursor)
	if err != nil {
		writeStorageUnavailable(c)
		return
	}

	var body bytes.Buffer
	body.WriteString(`{"records":[`)
	for i := range records {
		if i > 0 {
			body.WriteByte(',')
		}
		object, err := marshalRecordObject(records[i])
		if err != nil {
			writeStorageUnavailable(c)
			return
		}
		body.Write(object)
	}
	body.WriteString(`],"next_cursor":`)
	body.Write(encodedCursor)
	body.WriteByte('}')
	c.Data(http.StatusOK, "application/json; charset=utf-8", body.Bytes())
}
