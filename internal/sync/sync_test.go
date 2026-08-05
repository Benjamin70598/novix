package sync

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Benjamin70598/novix/internal/hub"
	nlog "github.com/Benjamin70598/novix/internal/log"
	"github.com/Benjamin70598/novix/internal/policy"
)

const testPolicy = `
site: store-a
hub: %s
topics:
  equipment_faults: { sync: immediate, priority: 0 }
  orders:           { sync: all, priority: 1 }
  fryer_telemetry:  { sync: aggregates_10s, priority: 2 }
  debug_dumps:      { sync: none, priority: 9 }
`

type fixture struct {
	log    *nlog.Log
	hubDB  *sql.DB
	hubSrv *httptest.Server
	syncer *Syncer
	now    time.Time
}

func setup(t *testing.T) *fixture {
	t.Helper()
	dir := t.TempDir()

	l, err := nlog.Open(filepath.Join(dir, "site.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })

	hubLog, err := nlog.Open(filepath.Join(dir, "hub.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { hubLog.Close() })
	h, err := hub.Open(hubLog.DB())
	if err != nil {
		t.Fatal(err)
	}
	hs := httptest.NewServer(h.Handler())
	t.Cleanup(hs.Close)

	polPath := filepath.Join(dir, "novix.yaml")
	writeFile(t, polPath, fmt.Sprintf(testPolicy, hs.URL))
	pol, err := policy.Load(polPath)
	if err != nil {
		t.Fatal(err)
	}

	f := &fixture{log: l, hubDB: hubLog.DB(), hubSrv: hs, syncer: New(l, pol)}
	f.now = time.Now()
	f.syncer.now = func() time.Time { return f.now }
	return f
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := writeAll(path, content); err != nil {
		t.Fatal(err)
	}
}

// append writes an event with a timestamp under test control by writing
// through the log and then rewriting ts directly (single-writer test only).
func (f *fixture) append(t *testing.T, topic string, payload string, ts time.Time) {
	t.Helper()
	seq, err := f.log.Append(topic, "", []byte(payload), f.syncer.Policy.Class(topic))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.log.DB().Exec(
		`UPDATE events SET ts = ? WHERE topic = ? AND site_seq = ?`,
		ts.UnixMilli(), topic, seq); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) hubEventCount(t *testing.T, topic string) int64 {
	t.Helper()
	var n int64
	if err := f.hubDB.QueryRow(
		`SELECT COUNT(*) FROM hub_events WHERE site = 'store-a' AND topic = ?`, topic).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func (f *fixture) hubAggWindows(t *testing.T, topic string) (wins, srcEvents int64) {
	t.Helper()
	if err := f.hubDB.QueryRow(
		`SELECT COUNT(*), COALESCE(SUM(count),0) FROM hub_aggregates WHERE site = 'store-a' AND topic = ?`,
		topic).Scan(&wins, &srcEvents); err != nil {
		t.Fatal(err)
	}
	return
}

func TestSyncConvergenceAndAggregation(t *testing.T) {
	f := setup(t)
	base := f.now.Add(-60 * time.Second)

	// 20 orders (raw), 200 telemetry readings across ~30s (aggregated),
	// 5 debug dumps (never leave), 1 fault (immediate).
	for i := 0; i < 20; i++ {
		f.append(t, "orders", fmt.Sprintf(`{"order_id":"o%03d","amount_cents":599}`, i), base.Add(time.Duration(i)*time.Second))
	}
	for i := 0; i < 200; i++ {
		f.append(t, "fryer_telemetry",
			fmt.Sprintf(`{"device":"fryer-1","temp_c":%0.1f}`, 165.0+float64(i%20)),
			base.Add(time.Duration(i*150)*time.Millisecond))
	}
	for i := 0; i < 5; i++ {
		f.append(t, "debug_dumps", `{"blob":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`, base)
	}
	f.append(t, "equipment_faults", `{"device":"fryer-2","fault":"overtemp"}`, base.Add(30*time.Second))

	if err := f.syncer.RunOnce(); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	if n := f.hubEventCount(t, "orders"); n != 20 {
		t.Fatalf("hub orders = %d, want 20", n)
	}
	if n := f.hubEventCount(t, "equipment_faults"); n != 1 {
		t.Fatalf("hub faults = %d, want 1", n)
	}
	if n := f.hubEventCount(t, "debug_dumps"); n != 0 {
		t.Fatalf("debug_dumps leaked to hub: %d", n)
	}
	if n := f.hubEventCount(t, "fryer_telemetry"); n != 0 {
		t.Fatalf("raw telemetry leaked to hub: %d", n)
	}
	wins, src := f.hubAggWindows(t, "fryer_telemetry")
	if wins < 3 || src != 200 {
		t.Fatalf("telemetry aggregates: %d windows covering %d events, want >=3 windows covering 200", wins, src)
	}

	// Aggregate stats are real: check temp_c min/max sanity on one window.
	var stats string
	if err := f.hubDB.QueryRow(
		`SELECT stats FROM hub_aggregates WHERE site='store-a' AND topic='fryer_telemetry' LIMIT 1`).Scan(&stats); err != nil {
		t.Fatal(err)
	}
	var doc map[string]map[string]float64
	if err := json.Unmarshal([]byte(stats), &doc); err != nil {
		t.Fatalf("stats not JSON: %v", err)
	}
	tc := doc["temp_c"]
	if tc["min"] < 164 || tc["max"] > 186 || tc["avg"] == 0 {
		t.Fatalf("implausible temp stats: %+v", tc)
	}
}

func TestIdempotentResync(t *testing.T) {
	f := setup(t)
	base := f.now.Add(-60 * time.Second)
	for i := 0; i < 10; i++ {
		f.append(t, "orders", fmt.Sprintf(`{"order_id":"o%d"}`, i), base.Add(time.Duration(i)*time.Second))
	}
	if err := f.syncer.RunOnce(); err != nil {
		t.Fatal(err)
	}
	// Simulate lost progress: reset the site's high-water mark and resync.
	if err := f.log.SetSyncedSeq("orders", 0); err != nil {
		t.Fatal(err)
	}
	if err := f.syncer.RunOnce(); err != nil {
		t.Fatal(err)
	}
	if n := f.hubEventCount(t, "orders"); n != 10 {
		t.Fatalf("after replay hub orders = %d, want 10 (dedupe failed)", n)
	}
}

func TestPartitionRecovery(t *testing.T) {
	f := setup(t)
	base := f.now.Add(-60 * time.Second)

	// Hub goes down.
	f.hubSrv.Close()
	for i := 0; i < 10; i++ {
		f.append(t, "orders", fmt.Sprintf(`{"order_id":"p%d"}`, i), base.Add(time.Duration(i)*time.Second))
	}
	if err := f.syncer.RunOnce(); err == nil {
		t.Fatal("expected error while hub is down")
	}

	// Hub comes back at a new address; site policy follows.
	hubLog2, err := nlog.Open(filepath.Join(t.TempDir(), "hub2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer hubLog2.Close()
	h2, err := hub.Open(hubLog2.DB())
	if err != nil {
		t.Fatal(err)
	}
	hs2 := httptest.NewServer(h2.Handler())
	defer hs2.Close()
	f.syncer.Policy.Hub = hs2.URL

	if err := f.syncer.RunOnce(); err != nil {
		t.Fatalf("post-recovery sync: %v", err)
	}
	var n int64
	if err := hubLog2.DB().QueryRow(
		`SELECT COUNT(*) FROM hub_events WHERE topic='orders'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 10 {
		t.Fatalf("backlog drain: hub has %d orders, want 10", n)
	}
}

func TestBandwidthMetersShowReduction(t *testing.T) {
	f := setup(t)
	base := f.now.Add(-120 * time.Second)

	// Telemetry-heavy workload: 2000 readings, 10 orders.
	for i := 0; i < 2000; i++ {
		f.append(t, "fryer_telemetry",
			fmt.Sprintf(`{"device":"fryer-%d","temp_c":%0.1f,"amps":%0.1f}`, i%4+1, 165.0+float64(i%20), 11.5),
			base.Add(time.Duration(i*30)*time.Millisecond))
	}
	for i := 0; i < 10; i++ {
		f.append(t, "orders", fmt.Sprintf(`{"order_id":"o%d","amount_cents":599}`, i), base.Add(time.Duration(i)*time.Second))
	}
	if err := f.syncer.RunOnce(); err != nil {
		t.Fatal(err)
	}
	meters, err := f.log.Meters()
	if err != nil {
		t.Fatal(err)
	}
	actual, naive := meters["sync_actual_bytes"], meters["sync_naive_bytes"]
	if actual == 0 || naive == 0 {
		t.Fatalf("meters empty: %+v", meters)
	}
	reduction := float64(naive) / float64(actual)
	t.Logf("naive=%d actual=%d reduction=%.1fx", naive, actual, reduction)
	if reduction < 10 {
		t.Fatalf("reduction %.1fx below 10x on telemetry-heavy workload", reduction)
	}
	if meters["sync_events_aggregated"] != 2000 {
		t.Fatalf("aggregated count: %+v", meters)
	}
}

func TestPriorityOrder(t *testing.T) {
	f := setup(t)
	topics, err := f.syncer.topicsByPriority()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"equipment_faults", "orders", "fryer_telemetry", "debug_dumps"}
	if len(topics) != len(want) {
		t.Fatalf("topics: %v", topics)
	}
	for i := range want {
		if topics[i] != want[i] {
			t.Fatalf("priority order: %v, want %v", topics, want)
		}
	}
}
