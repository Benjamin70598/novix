package policy

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

const sample = `
site: store-a
hub: http://127.0.0.1:9400
bandwidth_cap: 2mbps

topics:
  orders: { sync: all, priority: 1 }
  equipment_faults: { sync: immediate, priority: 0 }
  fryer_telemetry: { sync: aggregates_10s, priority: 2, retention_local: 72h }
  debug_dumps: { sync: none, priority: 9 }

offline:
  continue: true
  buffer: 72h
`

func write(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "novix.yaml")
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadAndClassify(t *testing.T) {
	p, err := Load(write(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	if p.Site != "store-a" || p.Hub != "http://127.0.0.1:9400" {
		t.Fatalf("site/hub: %+v", p)
	}
	cases := map[string]string{
		"orders":           "all",
		"equipment_faults": "immediate",
		"fryer_telemetry":  "aggregates_10s",
		"debug_dumps":      "none",
		"undeclared_topic": "all",
	}
	for topic, want := range cases {
		if got := p.Class(topic); got != want {
			t.Errorf("Class(%s) = %q, want %q", topic, got, want)
		}
	}
	if p.Topic("undeclared_topic").Priority != 50 {
		t.Errorf("default priority")
	}
}

func TestAggWindow(t *testing.T) {
	d, err := AggWindow("aggregates_10s")
	if err != nil || d != 10*time.Second {
		t.Fatalf("10s: %v %v", d, err)
	}
	d, err = AggWindow("aggregates_5m")
	if err != nil || d != 5*time.Minute {
		t.Fatalf("5m: %v %v", d, err)
	}
	if _, err := AggWindow("aggregates_banana"); err == nil {
		t.Fatal("expected error")
	}
}

func TestBandwidthCap(t *testing.T) {
	p, err := Load(write(t, sample))
	if err != nil {
		t.Fatal(err)
	}
	bps, err := p.CapBytesPerSec()
	if err != nil || bps != 250_000 {
		t.Fatalf("2mbps = %d B/s, err %v", bps, err)
	}
	p2 := &Policy{}
	bps, err = p2.CapBytesPerSec()
	if err != nil || bps != 0 {
		t.Fatalf("empty cap: %d %v", bps, err)
	}
}

func TestRejectsUnknownMode(t *testing.T) {
	_, err := Load(write(t, "topics:\n  x: { sync: sometimes }\n"))
	if err == nil {
		t.Fatal("expected error for unknown sync mode")
	}
}
