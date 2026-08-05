package log

// M2 additions: durable sync-loop state and bandwidth meters. These live in
// the same SQLite file as the event log so a site restart resumes sync
// exactly where it stopped, and the bandwidth numbers survive crashes too.

import "database/sql"

const m2Schema = `
CREATE TABLE IF NOT EXISTS sync_state (
	topic      TEXT PRIMARY KEY,
	synced_seq INTEGER NOT NULL DEFAULT 0,
	agg_end    INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS meters (
	name  TEXT PRIMARY KEY,
	value INTEGER NOT NULL DEFAULT 0
);`

func (l *Log) ensureM2Schema() error {
	_, err := l.db.Exec(m2Schema)
	return err
}

// SyncedSeq returns the last site_seq confirmed synced to the hub for topic.
func (l *Log) SyncedSeq(topic string) (int64, error) {
	var seq int64
	err := l.db.QueryRow(`SELECT synced_seq FROM sync_state WHERE topic = ?`, topic).Scan(&seq)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return seq, err
}

// SetSyncedSeq durably records sync progress for topic.
func (l *Log) SetSyncedSeq(topic string, seq int64) error {
	_, err := l.db.Exec(
		`INSERT INTO sync_state (topic, synced_seq) VALUES (?, ?)
		 ON CONFLICT (topic) DO UPDATE SET synced_seq = excluded.synced_seq`, topic, seq)
	return err
}

// AggEnd returns the end timestamp (ms) of the last aggregation window
// confirmed synced for topic. 0 means no window synced yet.
func (l *Log) AggEnd(topic string) (int64, error) {
	var end int64
	err := l.db.QueryRow(`SELECT agg_end FROM sync_state WHERE topic = ?`, topic).Scan(&end)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	return end, err
}

// SetAggEnd durably records aggregation progress for topic.
func (l *Log) SetAggEnd(topic string, end int64) error {
	_, err := l.db.Exec(
		`INSERT INTO sync_state (topic, agg_end) VALUES (?, ?)
		 ON CONFLICT (topic) DO UPDATE SET agg_end = excluded.agg_end`, topic, end)
	return err
}

// ReadByTS returns events on topic with fromTS <= ts < toTS in seq order.
func (l *Log) ReadByTS(topic string, fromTS, toTS int64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 50000 {
		limit = 10000
	}
	rows, err := l.db.Query(
		`SELECT topic, site_seq, ts, key, payload, class FROM events
		  WHERE topic = ? AND ts >= ? AND ts < ?
		  ORDER BY site_seq ASC LIMIT ?`, topic, fromTS, toTS, limit)
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

// MinTS returns the earliest event timestamp on topic (0 if empty).
func (l *Log) MinTS(topic string) (int64, error) {
	var ts sql.NullInt64
	err := l.db.QueryRow(`SELECT MIN(ts) FROM events WHERE topic = ?`, topic).Scan(&ts)
	return ts.Int64, err
}

// MeterAdd atomically adds delta to a named counter.
func (l *Log) MeterAdd(name string, delta int64) error {
	_, err := l.db.Exec(
		`INSERT INTO meters (name, value) VALUES (?, ?)
		 ON CONFLICT (name) DO UPDATE SET value = value + excluded.value`, name, delta)
	return err
}

// Meters returns all counters.
func (l *Log) Meters() (map[string]int64, error) {
	rows, err := l.db.Query(`SELECT name, value FROM meters`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var name string
		var v int64
		if err := rows.Scan(&name, &v); err != nil {
			return nil, err
		}
		out[name] = v
	}
	return out, rows.Err()
}
