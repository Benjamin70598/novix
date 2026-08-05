// Package hub implements novixd's hub mode: the cloud-side sink that
// receives synced events and aggregates from sites and serves the fleet
// view. The hub never rewrites history; it interleaves per-site append-only
// streams keyed by (site, topic, site_seq).
//
// Idempotency is the core contract: sites push with at-least-once
// semantics, so every ingest endpoint tolerates replays. Events dedupe on
// (site, topic, site_seq); aggregates upsert on (site, topic, window_start).
//
// API:
//
//	POST /v1/sync/{site}/events      batch of raw events for one topic
//	POST /v1/sync/{site}/aggregates  batch of closed windows for one topic
//	GET  /v1/fleet/status            per-site, per-topic sync state
//	GET  /v1/fleet/events?site=&topic=&after=&limit=   inspection
package hub

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Hub is the cloud-side sink. It owns its own SQLite handle (hub tables are
// disjoint from the site log tables, so hub and agent can share a file in
// tests without colliding).
type Hub struct {
	db *sql.DB
}

// Open initializes hub storage on an existing database handle.
func Open(db *sql.DB) (*Hub, error) {
	schema := `
CREATE TABLE IF NOT EXISTS hub_events (
	site     TEXT    NOT NULL,
	topic    TEXT    NOT NULL,
	site_seq INTEGER NOT NULL,
	ts       INTEGER NOT NULL,
	key      TEXT    NOT NULL DEFAULT '',
	payload  BLOB    NOT NULL,
	class    TEXT    NOT NULL DEFAULT '',
	recv_ts  INTEGER NOT NULL,
	PRIMARY KEY (site, topic, site_seq)
);
CREATE TABLE IF NOT EXISTS hub_aggregates (
	site         TEXT    NOT NULL,
	topic        TEXT    NOT NULL,
	window_start INTEGER NOT NULL,
	window_end   INTEGER NOT NULL,
	count        INTEGER NOT NULL,
	stats        TEXT    NOT NULL,
	recv_ts      INTEGER NOT NULL,
	PRIMARY KEY (site, topic, window_start)
);`
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("hub schema: %w", err)
	}
	return &Hub{db: db}, nil
}

// SyncEvent is the wire form of one raw event pushed by a site.
type SyncEvent struct {
	SiteSeq int64           `json:"site_seq"`
	TS      int64           `json:"ts_unix_ms"`
	Key     string          `json:"key,omitempty"`
	Payload json.RawMessage `json:"payload"`
	Class   string          `json:"class,omitempty"`
}

type eventBatch struct {
	Topic  string      `json:"topic"`
	Events []SyncEvent `json:"events"`
}

// AggWindowRec is the wire form of one closed aggregation window.
type AggWindowRec struct {
	WindowStart int64           `json:"window_start_ms"`
	WindowEnd   int64           `json:"window_end_ms"`
	Count       int64           `json:"count"`
	Stats       json.RawMessage `json:"stats"`
}

type aggBatch struct {
	Topic   string         `json:"topic"`
	Windows []AggWindowRec `json:"windows"`
}

// Handler returns the hub's HTTP handler.
func (h *Hub) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sync/{site}/events", h.handleEvents)
	mux.HandleFunc("POST /v1/sync/{site}/aggregates", h.handleAggregates)
	mux.HandleFunc("GET /v1/fleet/status", h.handleStatus)
	mux.HandleFunc("GET /v1/fleet/events", h.handleInspect)
	return mux
}

func (h *Hub) handleEvents(w http.ResponseWriter, r *http.Request) {
	site := r.PathValue("site")
	var b eventBatch
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil || b.Topic == "" {
		httpErr(w, 400, "bad batch")
		return
	}
	tx, err := h.db.Begin()
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	accepted := 0
	for _, e := range b.Events {
		res, err := tx.Exec(
			`INSERT OR IGNORE INTO hub_events (site, topic, site_seq, ts, key, payload, class, recv_ts)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			site, b.Topic, e.SiteSeq, e.TS, e.Key, []byte(e.Payload), e.Class, now)
		if err != nil {
			httpErr(w, 500, err.Error())
			return
		}
		if n, _ := res.RowsAffected(); n > 0 {
			accepted++
		}
	}
	var hwm int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(site_seq), 0) FROM hub_events WHERE site = ? AND topic = ?`,
		site, b.Topic).Scan(&hwm); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	if err := tx.Commit(); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	writeJSON(w, map[string]int64{"accepted": int64(accepted), "high_water": hwm})
}

func (h *Hub) handleAggregates(w http.ResponseWriter, r *http.Request) {
	site := r.PathValue("site")
	var b aggBatch
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil || b.Topic == "" {
		httpErr(w, 400, "bad batch")
		return
	}
	tx, err := h.db.Begin()
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer tx.Rollback()
	now := time.Now().UnixMilli()
	for _, win := range b.Windows {
		if _, err := tx.Exec(
			`INSERT INTO hub_aggregates (site, topic, window_start, window_end, count, stats, recv_ts)
			 VALUES (?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT (site, topic, window_start) DO UPDATE SET
			   window_end = excluded.window_end, count = excluded.count,
			   stats = excluded.stats, recv_ts = excluded.recv_ts`,
			site, b.Topic, win.WindowStart, win.WindowEnd, win.Count, string(win.Stats), now); err != nil {
			httpErr(w, 500, err.Error())
			return
		}
	}
	var hwmEnd int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(window_end), 0) FROM hub_aggregates WHERE site = ? AND topic = ?`,
		site, b.Topic).Scan(&hwmEnd); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	if err := tx.Commit(); err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	writeJSON(w, map[string]int64{"high_water_window_end": hwmEnd})
}

type topicStatus struct {
	Events     int64 `json:"events"`
	MaxSeq     int64 `json:"max_seq"`
	AggWindows int64 `json:"agg_windows,omitempty"`
	AggCount   int64 `json:"agg_source_events,omitempty"`
	LastRecvMS int64 `json:"last_recv_ms"`
}

func (h *Hub) handleStatus(w http.ResponseWriter, r *http.Request) {
	out := map[string]map[string]*topicStatus{}
	rows, err := h.db.Query(
		`SELECT site, topic, COUNT(*), MAX(site_seq), MAX(recv_ts) FROM hub_events GROUP BY site, topic`)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	for rows.Next() {
		var site, topic string
		var st topicStatus
		if err := rows.Scan(&site, &topic, &st.Events, &st.MaxSeq, &st.LastRecvMS); err != nil {
			rows.Close()
			httpErr(w, 500, err.Error())
			return
		}
		if out[site] == nil {
			out[site] = map[string]*topicStatus{}
		}
		s := st
		out[site][topic] = &s
	}
	rows.Close()

	rows, err = h.db.Query(
		`SELECT site, topic, COUNT(*), SUM(count), MAX(recv_ts) FROM hub_aggregates GROUP BY site, topic`)
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	for rows.Next() {
		var site, topic string
		var wins, srcCount, last int64
		if err := rows.Scan(&site, &topic, &wins, &srcCount, &last); err != nil {
			rows.Close()
			httpErr(w, 500, err.Error())
			return
		}
		if out[site] == nil {
			out[site] = map[string]*topicStatus{}
		}
		st := out[site][topic]
		if st == nil {
			st = &topicStatus{}
			out[site][topic] = st
		}
		st.AggWindows, st.AggCount = wins, srcCount
		if last > st.LastRecvMS {
			st.LastRecvMS = last
		}
	}
	rows.Close()
	writeJSON(w, map[string]any{"sites": out})
}

func (h *Hub) handleInspect(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	rows, err := h.db.Query(
		`SELECT site_seq, ts, key, payload, class FROM hub_events
		  WHERE site = ? AND topic = ? AND site_seq > ?
		  ORDER BY site_seq LIMIT ?`,
		q.Get("site"), q.Get("topic"), atoi(q.Get("after")), max(1, atoi(q.Get("limit"))))
	if err != nil {
		httpErr(w, 500, err.Error())
		return
	}
	defer rows.Close()
	var evs []SyncEvent
	for rows.Next() {
		var e SyncEvent
		var payload []byte
		if err := rows.Scan(&e.SiteSeq, &e.TS, &e.Key, &payload, &e.Class); err != nil {
			httpErr(w, 500, err.Error())
			return
		}
		e.Payload = payload
		evs = append(evs, e)
	}
	writeJSON(w, map[string]any{"events": evs})
}

func atoi(s string) int64 {
	var n int64
	fmt.Sscanf(s, "%d", &n)
	return n
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func httpErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
