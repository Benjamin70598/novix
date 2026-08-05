package log

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func openTemp(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "novix.db")
	l, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return l, path
}

func TestAppendAssignsContiguousSeqs(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()

	for i := 1; i <= 100; i++ {
		seq, err := l.Append("orders", fmt.Sprintf("k%d", i), []byte(`{"n":1}`), "")
		if err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
		if seq != int64(i) {
			t.Fatalf("append %d: got seq %d", i, seq)
		}
	}
	evs, err := l.Read("orders", 0, 1000)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(evs) != 100 {
		t.Fatalf("read: got %d events", len(evs))
	}
	for i, e := range evs {
		if e.SiteSeq != int64(i+1) {
			t.Fatalf("event %d has seq %d", i, e.SiteSeq)
		}
	}
}

func TestTopicsAreIndependent(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()

	l.Append("orders", "", []byte(`1`), "")
	l.Append("telemetry", "", []byte(`1`), "")
	l.Append("orders", "", []byte(`2`), "")

	mo, _ := l.MaxSeq("orders")
	mt, _ := l.MaxSeq("telemetry")
	if mo != 2 || mt != 1 {
		t.Fatalf("orders=%d telemetry=%d", mo, mt)
	}
}

func TestPersistsAcrossReopen(t *testing.T) {
	l, path := openTemp(t)
	for i := 0; i < 10; i++ {
		if _, err := l.Append("orders", "", []byte(`x`), ""); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()

	l2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()
	max, _ := l2.MaxSeq("orders")
	if max != 10 {
		t.Fatalf("after reopen max=%d", max)
	}
	seq, err := l2.Append("orders", "", []byte(`y`), "")
	if err != nil || seq != 11 {
		t.Fatalf("append after reopen: seq=%d err=%v", seq, err)
	}
}

func TestConcurrentAppendsStayContiguous(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()

	const goroutines, per = 8, 50
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				if _, err := l.Append("orders", "", []byte(`c`), ""); err != nil {
					t.Errorf("append: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	evs, err := l.Read("orders", 0, 10000)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != goroutines*per {
		t.Fatalf("got %d events, want %d", len(evs), goroutines*per)
	}
	for i, e := range evs {
		if e.SiteSeq != int64(i+1) {
			t.Fatalf("gap or dup at position %d: seq %d", i, e.SiteSeq)
		}
	}
}

func TestOffsets(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()

	got, err := l.Offset("kds", "orders")
	if err != nil || got != 0 {
		t.Fatalf("initial offset: %d %v", got, err)
	}
	if err := l.CommitOffset("kds", "orders", 42); err != nil {
		t.Fatal(err)
	}
	if err := l.CommitOffset("kds", "orders", 99); err != nil {
		t.Fatal(err)
	}
	got, _ = l.Offset("kds", "orders")
	if got != 99 {
		t.Fatalf("offset=%d", got)
	}
}

func TestReadAfter(t *testing.T) {
	l, _ := openTemp(t)
	defer l.Close()
	for i := 0; i < 20; i++ {
		l.Append("orders", "", []byte(`x`), "")
	}
	evs, err := l.Read("orders", 15, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 5 || evs[0].SiteSeq != 16 {
		t.Fatalf("read after 15: len=%d first=%d", len(evs), evs[0].SiteSeq)
	}
}
