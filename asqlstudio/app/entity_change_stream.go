package studioapp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

type entityChangeStreamStartResponse struct {
	Status    string `json:"status"`
	StreamID  string `json:"stream_id"`
	EventName string `json:"event_name"`
	SQL       string `json:"sql"`
}

type entityChangeStreamEvent struct {
	Kind     string                 `json:"kind"`
	StreamID string                 `json:"stream_id,omitempty"`
	Row      map[string]interface{} `json:"row,omitempty"`
	Error    string                 `json:"error,omitempty"`
	SQL      string                 `json:"sql,omitempty"`
}

func quoteStreamIdentifier(value string) string {
	if value == "" {
		return value
	}
	if isSimpleIdentifier(value) {
		return value
	}
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteStreamLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}

func isSimpleIdentifier(value string) bool {
	for idx, r := range value {
		if idx == 0 {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_') {
				return false
			}
			continue
		}
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_') {
			return false
		}
	}
	return value != ""
}

func buildEntityChangeStreamSQL(req entityChangeStreamStartRequest, follow bool) (string, error) {
	domain := strings.TrimSpace(req.Domain)
	entity := strings.TrimSpace(req.EntityName)
	if domain == "" {
		return "", fmt.Errorf("domain is required")
	}
	if entity == "" {
		return "", fmt.Errorf("entity_name is required")
	}
	parts := []string{
		"TAIL ENTITY CHANGES",
		quoteStreamIdentifier(domain) + "." + quoteStreamIdentifier(entity),
	}
	if rootPK := strings.TrimSpace(req.RootPK); rootPK != "" {
		parts = append(parts, "FOR", quoteStreamLiteral(rootPK))
	}
	if req.FromLSN > 0 {
		parts = append(parts, "FROM LSN", fmt.Sprintf("%d", req.FromLSN))
	}
	if req.ToLSN > 0 {
		parts = append(parts, "TO LSN", fmt.Sprintf("%d", req.ToLSN))
	}
	if req.Limit > 0 {
		parts = append(parts, "LIMIT", fmt.Sprintf("%d", req.Limit))
	}
	if follow {
		parts = append(parts, "FOLLOW")
	}
	return strings.Join(parts, " "), nil
}

func (a *App) storeEntityChangeStream(streamID string, cancel context.CancelFunc) {
	a.streamMu.Lock()
	a.streamCancels[streamID] = cancel
	a.streamMu.Unlock()
}

func (a *App) deleteEntityChangeStream(streamID string) {
	a.streamMu.Lock()
	delete(a.streamCancels, streamID)
	a.streamMu.Unlock()
}

func (a *App) takeEntityChangeStream(streamID string) context.CancelFunc {
	a.streamMu.Lock()
	defer a.streamMu.Unlock()
	cancel := a.streamCancels[streamID]
	delete(a.streamCancels, streamID)
	return cancel
}

func (a *App) stopAllEntityChangeStreams() {
	a.streamMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(a.streamCancels))
	for streamID, cancel := range a.streamCancels {
		cancels = append(cancels, cancel)
		delete(a.streamCancels, streamID)
	}
	a.streamMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (a *App) StartEntityChangeStream(req entityChangeStreamStartRequest) (map[string]interface{}, error) {
	if a.engine == nil {
		return nil, fmt.Errorf("engine is not configured")
	}
	if a.ctx == nil {
		return nil, fmt.Errorf("application context is not ready")
	}
	sql, err := buildEntityChangeStreamSQL(req, true)
	if err != nil {
		return nil, err
	}
	streamID := randomID()
	eventName := "entity-change-stream:" + streamID
	ctx, cancel := context.WithCancel(a.reqCtx0())
	a.storeEntityChangeStream(streamID, cancel)

	go func() {
		defer a.deleteEntityChangeStream(streamID)
		runtime.EventsEmit(a.ctx, eventName, entityChangeStreamEvent{Kind: "started", StreamID: streamID, SQL: sql})
		err := a.engine.streamQueryWithDomains(ctx, []string{req.Domain}, sql, func(row map[string]interface{}) error {
			runtime.EventsEmit(a.ctx, eventName, entityChangeStreamEvent{Kind: "row", StreamID: streamID, Row: row})
			return nil
		})
		if err != nil {
			if errors.Is(err, context.Canceled) {
				runtime.EventsEmit(a.ctx, eventName, entityChangeStreamEvent{Kind: "stopped", StreamID: streamID})
				return
			}
			runtime.EventsEmit(a.ctx, eventName, entityChangeStreamEvent{Kind: "error", StreamID: streamID, Error: err.Error()})
			return
		}
		runtime.EventsEmit(a.ctx, eventName, entityChangeStreamEvent{Kind: "end", StreamID: streamID})
	}()

	return structToMap(entityChangeStreamStartResponse{
		Status:    "started",
		StreamID:  streamID,
		EventName: eventName,
		SQL:       sql,
	})
}

func (a *App) StopEntityChangeStream(req entityChangeStreamStopRequest) (map[string]interface{}, error) {
	streamID := strings.TrimSpace(req.StreamID)
	if streamID == "" {
		return nil, fmt.Errorf("stream_id is required")
	}
	cancel := a.takeEntityChangeStream(streamID)
	if cancel == nil {
		return map[string]interface{}{"status": "not_found", "stream_id": streamID}, nil
	}
	cancel()
	return map[string]interface{}{"status": "stopped", "stream_id": streamID}, nil
}
