// Package sync implements the Novix site-to-hub sync loop: the component
// that turns declared policy into wire behavior.
//
// Each pass walks topics in priority order (lower number first) and, per
// the topic's class:
//
//	all / immediate   push raw events after the durable high-water mark
//	aggregates_D      push only closed windows of computed statistics;
//	                  raw events never leave the site
//	none              nothing leaves; the naive meter still counts it
//
// Two meters make the pitch measurable, both persisted in the site log:
//
//	sync_actual_bytes  bytes actually sent to the hub
//	sync_naive_bytes   bytes a replicate-everything design would have sent
//	                   (every raw event in wire format)
//
// The loop is partition-tolerant by construction: any error aborts the pass,
// progress marks are only advanced after hub acknowledgment, and the next
// tick retries. A site that reconnects after hours drains its backlog in
// priority order because every pass is priority-ordered.
package sync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync/atomic"
	"time"

	nlog "github.com/Benjamin70598/novix/internal/log"
	"github.com/Benjamin70598/novix/internal/policy"
)

const (
	batchLimit = 500
	aggGrace   = 2 * time.Second // window must end this long ago to be closed
)

// Syncer pushes one site's log to the hub according to policy.
type Syncer struct {
	Log    *nlog.Log
	Policy *policy.Policy
	Client *http.Client

	kick        chan struct{}
	lastOK      atomic.Int64 // unix ms of last successful hub exchange
	lastErr     atomic.Value // string
	capBPS      int64
	now         func() time.Time
}

// New builds a Syncer. Policy must have been validated by policy.Load.
func New(l *nlog.Log, p *policy.Policy) *Syncer {
	cap, _ := p.CapBytesPerSec()
	return &Syncer{
		Log:    l,
		Policy: p,
		Client: &http.Client{Timeout: 10 * time.Second},
		kick:   make(chan struct{}, 1),
		capBPS: cap,
		now:    time.Now,
	}
}

// Kick requests an immediate sync pass (used for immediate-class appends).
func (s *Syncer) Kick() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Run loops until ctx is done: tick, kick, or backoff after errors.
func (s *Syncer) Run(ctx context.Context, tick time.Duration) {
	if tick <= 0 {
		tick = 500 * time.Millisecond
	}
	backoff := tick
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		case <-s.kick:
		}
		if err := s.RunOnce(); err != nil {
			s.lastErr.Store(err.Error())
			if backoff < 5*time.Second {
				backoff *= 2
			}
		} else {
			backoff = tick
		}
	}
}

// RunOnce performs a single full sync pass over all topics.
func (s *Syncer) RunOnce() error {
	topics, err := s.topicsByPriority()
	if err != nil {
		return err
	}
	for _, t := range topics {
		tp := s.Policy.Topic(t)
		switch {
		case tp.Sync == "none":
			err = s.passNone(t)
		case tp.Sync == "all" || tp.Sync == "immediate":
			err = s.passRaw(t)
		default: // aggregates_D, validated at load
			err = s.passAggregates(t, tp.Sync)
		}
		if err != nil {
			return fmt.Errorf("topic %s: %w", t, err)
		}
	}
	return nil
}

// topicsByPriority is the union of topics present in the log and topics
// declared in policy, sorted by priority then name.
func (s *Syncer) topicsByPriority() ([]string, error) {
	present, err := s.Log.Topics()
	if err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for t := range present {
		set[t] = true
	}
	for t := range s.Policy.Topics {
		set[t] = true
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool {
		pi, pj := s.Policy.Topic(out[i]).Priority, s.Policy.Topic(out[j]).Priority
		if pi != pj {
			return pi < pj
		}
		return out[i] < out[j]
	})
	return out, nil
}

// passRaw pushes raw events after the synced high-water mark.
func (s *Syncer) passRaw(topic string) error {
	for {
		hwm, err := s.Log.SyncedSeq(topic)
		if err != nil {
			return err
		}
		evs, err := s.Log.Read(topic, hwm, batchLimit)
		if err != nil {
			return err
		}
		if len(evs) == 0 {
			return nil
		}
		wire := make([]wireEvent, len(evs))
		var naive int64
		for i, e := range evs {
			wire[i] = wireEvent{SiteSeq: e.SiteSeq, TS: e.TS, Key: e.Key, Payload: e.Payload, Class: e.Class}
			naive += wireSize(wire[i])
		}
		body, _ := json.Marshal(map[string]any{"topic": topic, "events": wire})
		if err := s.post("/v1/sync/"+s.Policy.Site+"/events", body); err != nil {
			return err
		}
		s.Log.MeterAdd("sync_actual_bytes", int64(len(body)))
		s.Log.MeterAdd("sync_naive_bytes", naive)
		s.Log.MeterAdd("sync_events_raw_sent", int64(len(evs)))
		if err := s.Log.SetSyncedSeq(topic, evs[len(evs)-1].SiteSeq); err != nil {
			return err
		}
		if len(evs) < batchLimit {
			return nil
		}
	}
}

// passNone advances past unsynced events, charging only the naive meter:
// this is data a replicate-everything design would have shipped.
func (s *Syncer) passNone(topic string) error {
	hwm, err := s.Log.SyncedSeq(topic)
	if err != nil {
		return err
	}
	evs, err := s.Log.Read(topic, hwm, batchLimit*10)
	if err != nil || len(evs) == 0 {
		return err
	}
	var naive int64
	for _, e := range evs {
		naive += wireSize(wireEvent{SiteSeq: e.SiteSeq, TS: e.TS, Key: e.Key, Payload: e.Payload, Class: e.Class})
	}
	s.Log.MeterAdd("sync_naive_bytes", naive)
	return s.Log.SetSyncedSeq(topic, evs[len(evs)-1].SiteSeq)
}

// passAggregates computes closed windows and pushes statistics only.
func (s *Syncer) passAggregates(topic, mode string) error {
	window, err := policy.AggWindow(mode)
	if err != nil {
		return err
	}
	winMS := window.Milliseconds()

	from, err := s.Log.AggEnd(topic)
	if err != nil {
		return err
	}
	if from == 0 {
		minTS, err := s.Log.MinTS(topic)
		if err != nil {
			return err
		}
		if minTS == 0 {
			return nil // topic empty
		}
		from = (minTS / winMS) * winMS
	}
	cutoff := s.now().Add(-aggGrace).UnixMilli()
	lastClosed := (cutoff / winMS) * winMS // windows ending at or before this are closed
	if from >= lastClosed {
		return nil
	}

	evs, err := s.Log.ReadByTS(topic, from, lastClosed, 0)
	if err != nil {
		return err
	}
	if len(evs) == 0 {
		// Nothing in the closed range; advance so we do not rescan forever.
		return s.Log.SetAggEnd(topic, lastClosed)
	}

	wins := map[int64]*aggWindow{}
	var naive int64
	var maxSeq int64
	for _, e := range evs {
		ws := (e.TS / winMS) * winMS
		w, ok := wins[ws]
		if !ok {
			w = newAggWindow(ws, ws+winMS)
			wins[ws] = w
		}
		w.add(e.Payload)
		naive += wireSize(wireEvent{SiteSeq: e.SiteSeq, TS: e.TS, Key: e.Key, Payload: e.Payload, Class: e.Class})
		if e.SiteSeq > maxSeq {
			maxSeq = e.SiteSeq
		}
	}

	ordered := make([]*aggWindow, 0, len(wins))
	for _, w := range wins {
		ordered = append(ordered, w)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].start < ordered[j].start })

	recs := make([]map[string]any, len(ordered))
	for i, w := range ordered {
		recs[i] = w.record()
	}
	body, _ := json.Marshal(map[string]any{"topic": topic, "windows": recs})
	if err := s.post("/v1/sync/"+s.Policy.Site+"/aggregates", body); err != nil {
		return err
	}
	s.Log.MeterAdd("sync_actual_bytes", int64(len(body)))
	s.Log.MeterAdd("sync_naive_bytes", naive)
	s.Log.MeterAdd("sync_events_aggregated", int64(len(evs)))
	s.Log.MeterAdd("sync_agg_windows_sent", int64(len(ordered)))
	// Also advance the raw hwm so a later policy change to sync:all does not
	// re-ship history that was already represented by aggregates.
	if err := s.Log.SetSyncedSeq(topic, maxSeq); err != nil {
		return err
	}
	return s.Log.SetAggEnd(topic, lastClosed)
}

func (s *Syncer) post(path string, body []byte) error {
	resp, err := s.Client.Post(s.Policy.Hub+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("hub returned %d", resp.StatusCode)
	}
	s.lastOK.Store(time.Now().UnixMilli())
	if s.capBPS > 0 {
		time.Sleep(time.Duration(float64(len(body)) / float64(s.capBPS) * float64(time.Second)))
	}
	return nil
}

// Status reports sync health and the bandwidth meters for /v1/status.
func (s *Syncer) Status() map[string]any {
	meters, _ := s.Log.Meters()
	out := map[string]any{
		"hub":             s.Policy.Hub,
		"last_ok_ms":      s.lastOK.Load(),
		"meters":          meters,
	}
	if actual, naive := meters["sync_actual_bytes"], meters["sync_naive_bytes"]; actual > 0 && naive > 0 {
		out["reduction_factor"] = math.Round(float64(naive)/float64(actual)*10) / 10
	}
	if e, ok := s.lastErr.Load().(string); ok && e != "" {
		out["last_error"] = e
	}
	return out
}

type wireEvent struct {
	SiteSeq int64           `json:"site_seq"`
	TS      int64           `json:"ts_unix_ms"`
	Key     string          `json:"key,omitempty"`
	Payload json.RawMessage `json:"payload"`
	Class   string          `json:"class,omitempty"`
}

func wireSize(e wireEvent) int64 {
	b, _ := json.Marshal(e)
	return int64(len(b))
}

// aggWindow accumulates per-field numeric statistics for one window.
type aggWindow struct {
	start, end int64
	count      int64
	fields     map[string]*fieldStats
}

type fieldStats struct {
	min, max, sum float64
	n             int64
}

func newAggWindow(start, end int64) *aggWindow {
	return &aggWindow{start: start, end: end, fields: map[string]*fieldStats{}}
}

func (w *aggWindow) add(payload []byte) {
	w.count++
	var doc map[string]any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return
	}
	for k, v := range doc {
		f, ok := v.(float64)
		if !ok {
			continue
		}
		st, ok := w.fields[k]
		if !ok {
			st = &fieldStats{min: f, max: f}
			w.fields[k] = st
		}
		if f < st.min {
			st.min = f
		}
		if f > st.max {
			st.max = f
		}
		st.sum += f
		st.n++
	}
}

func (w *aggWindow) record() map[string]any {
	stats := map[string]any{}
	for k, st := range w.fields {
		stats[k] = map[string]float64{
			"min": st.min,
			"max": st.max,
			"avg": math.Round(st.sum/float64(st.n)*1000) / 1000,
		}
	}
	return map[string]any{
		"window_start_ms": w.start,
		"window_end_ms":   w.end,
		"count":           w.count,
		"stats":           stats,
	}
}
