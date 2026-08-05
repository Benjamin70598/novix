// Package log implements the Novix site log: an append-only, per-topic,
// crash-safe event log backed by SQLite.
//
// Design notes (see DECISIONS.md 005, 006):
//   - SQLite in WAL mode. synchronous=NORMAL is safe against process death
//     (kill -9): committed transactions live in the WAL and survive. Full
//     power-loss durability would use synchronous=FULL at a write-latency
//     cost; revisit with a design partner workload.
//   - site_seq is a per-topic monotonic integer assigned inside the append
//     transaction. Single writer per site makes total order per topic trivial.
//   - class is stamped at write time by the policy engine (M2). For M1 the
//     caller passes it through (empty string is fine).
//   - Delivery is at-least-once end to end. A producer retrying after a lost
//     ack may create a duplicate business event with a new site_seq. Dedupe
//     by key is an M2+ concern; the log itself never duplicates a site_seq.
package log

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Event is one record in the site log.
type Event struct {
	Topic   string `json:"topic"`
	SiteSeq int64  `json:"site_seq"`
	TS      int64  `json:"ts_unix_ms"`
	Key     string `json:"key,omitempty"`
	Payload []byte `json:"payload"`
	Class   string `json:"class,omitempty"`
}

// Log is a handle to the site log. Safe for concurrent use.
type Log struct {
	db *sql.DB

	mu     sync.Mutex
	notify map[string]chan struct{} // per-topic broadcast: closed on append
}

// Open opens (creating if needed) the site log at path.
func Open(path string) (*Log, error) {
	// _busy_timeout guards against transient SQLITE_BUSY under WAL.
	db, err := sql.Open(driverName, path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// The log serializes writes itself; a single connection avoids
	// SQLITE_BUSY entirely and keeps reasoning simple at M1 scale.
	db.SetMaxOpenConns(1)

	pragmas := []string{
		`PRAGMA journal_mode=WAL;`,
		`PRAGMA synchronous=NORMAL;`,
		`PRAGMA busy_timeout=5000;`,
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("pragma %q: %w", p, err)
		}
	}

	schema := `
CREATE TABLE IF NOT EXISTS events (
	topic    TEXT    NOT NULL,
	site_seq INTEGER NOT NULL,
	ts       INTEGER NOT NULL,
	key      TEXT    NOT NULL DEFAULT '',
	payload  BLOB    NOT NULL,
	class    TEXT    NOT NULL DEFAULT '',
	PRIMARY KEY (topic, site_seq)
);
CREATE TABLE IF NOT EXISTS offsets (
	consumer      TEXT    NOT NULL,
	topic         TEXT    NOT NULL,
	committed_seq INTEGER NOT NULL,
	PRIMARY KEY (consumer, topic)
);`
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}

	return &Log{db: db, notify: make(map[string]chan struct{})}, nil
}

// Close closes the underlying database.
func (l *Log) Close() error { return l.db.Close() }

// Append writes one event to topic and returns its assigned site_seq.
// The event is durably committed (WAL) before Append returns; an ack built
// on this return value is honest against kill -9.
func (l *Log) Append(topic, key string, payload []byte, class string) (int64, error) {
	if topic == "" {
		return 0, errors.New("empty topic")
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	tx, err := l.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var next int64
	if err := tx.QueryRow(
		`SELECT COALESCE(MAX(site_seq), 0) + 1 FROM events WHERE topic = ?`, topic,
	).Scan(&next); err != nil {
		return 0, err
	}
	if _, err := tx.Exec(
		`INSERT INTO events (topic, site_seq, ts, key, payload, class) VALUES (?, ?, ?, ?, ?, ?)`,
		topic, next, time.Now().UnixMilli(), key, payload, class,
	); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}

	// Wake long-pollers on this topic.
	if ch, ok := l.notify[topic]; ok {
		close(ch)
		delete(l.notify, topic)
	}
	return next, nil
}

// Read returns up to limit events on topic with site_seq > after, in order.
func (l *Log) Read(topic string, after int64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
	rows, err := l.db.Query(
		`SELECT topic, site_seq, ts, key, payload, class
		   FROM events WHERE topic = ? AND site_seq > ?
		   ORDER BY site_seq ASC LIMIT ?`, topic, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.Topic, &e.SiteSeq, &e.TS, &e.Key, &e.Payload, &e.Class); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MaxSeq returns the highest site_seq on topic (0 if empty).
func (l *Log) MaxSeq(topic string) (int64, error) {
	var max int64
	err := l.db.QueryRow(
		`SELECT COALESCE(MAX(site_seq), 0) FROM events WHERE topic = ?`, topic,
	).Scan(&max)
	return max, err
}

// Topics returns every topic present in the log with its max site_seq.
func (l *Log) Topics() (map[string]int64, error) {
	rows, err := l.db.Query(`SELECT topic, MAX(site_seq) FROM events GROUP BY topic`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var t string
		var m int64
		if err := rows.Scan(&t, &m); err != nil {
			return nil, err
		}
		out[t] = m
	}
	return out, rows.Err()
}

// WaitCh returns a channel that is closed on the next append to topic.
// Callers select on it together with their own timeout.
func (l *Log) WaitCh(topic string) <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	ch, ok := l.notify[topic]
	if !ok {
		ch = make(chan struct{})
		l.notify[topic] = ch
	}
	return ch
}

// CommitOffset durably records consumer's committed position on topic.
func (l *Log) CommitOffset(consumer, topic string, seq int64) error {
	_, err := l.db.Exec(
		`INSERT INTO offsets (consumer, topic, committed_seq) VALUES (?, ?, ?)
		 ON CONFLICT (consumer, topic) DO UPDATE SET committed_seq = excluded.committed_seq`,
		consumer, topic, seq)
	return err
}

// Offset returns consumer's committed position on topic (0 if none).
func (l *Log) Offset(consumer, topic string) (int64, error) {
	var seq int64
	err := l.db.QueryRow(
		`SELECT committed_seq FROM offsets WHERE consumer = ? AND topic = ?`,
		consumer, topic).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return seq, err
}
