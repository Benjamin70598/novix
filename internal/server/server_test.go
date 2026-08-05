package server

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	nlog "github.com/Benjamin70598/novix/internal/log"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	l, err := nlog.Open(filepath.Join(t.TempDir(), "novix.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return httptest.NewServer((&Server{Site: "test", Log: l}).Handler())
}

func TestAppendReadRoundtrip(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	body := `{"key":"o1","payload":{"order_id":"o1","amount_cents":599},"class":"orders"}`
	resp, err := ts.Client().Post(ts.URL+"/v1/topics/orders/events", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	var ack struct {
		SiteSeq int64 `json:"site_seq"`
	}
	json.NewDecoder(resp.Body).Decode(&ack)
	resp.Body.Close()
	if ack.SiteSeq != 1 {
		t.Fatalf("ack seq=%d", ack.SiteSeq)
	}

	resp, err = ts.Client().Get(ts.URL + "/v1/topics/orders/events?after=0")
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		Events []nlog.Event `json:"events"`
		MaxSeq int64        `json:"max_seq"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	resp.Body.Close()
	if len(out.Events) != 1 || out.MaxSeq != 1 {
		t.Fatalf("events=%d max=%d", len(out.Events), out.MaxSeq)
	}
	var payload struct {
		OrderID string `json:"order_id"`
	}
	json.Unmarshal(out.Events[0].Payload, &payload)
	if payload.OrderID != "o1" {
		t.Fatalf("payload roundtrip: %s", out.Events[0].Payload)
	}
}

func TestLongPollWakesOnAppend(t *testing.T) {
	ts := newTestServer(t)
	defer ts.Close()

	done := make(chan int, 1)
	go func() {
		resp, err := ts.Client().Get(ts.URL + "/v1/topics/orders/events?after=0&wait_ms=5000")
		if err != nil {
			done <- -1
			return
		}
		var out struct {
			Events []nlog.Event `json:"events"`
		}
		json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		done <- len(out.Events)
	}()

	time.Sleep(150 * time.Millisecond) // poller is now waiting
	start := time.Now()
	ts.Client().Post(ts.URL+"/v1/topics/orders/events", "application/json",
		bytes.NewBufferString(`{"payload":{"n":1}}`))

	select {
	case n := <-done:
		if n != 1 {
			t.Fatalf("long poll returned %d events", n)
		}
		if time.Since(start) > 2*time.Second {
			t.Fatalf("long poll did not wake promptly")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("long poll never returned")
	}
}
