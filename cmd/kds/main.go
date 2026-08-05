// kds simulates a kitchen display system: it consumes the orders topic from
// the local novixd, verifies site_seq contiguity (no gaps, no seq
// duplicates), and commits its offset every batch. Restart it any time; it
// resumes from its committed offset.
//
// With -until N it exits 0 once seq N is consumed, or nonzero on any
// contiguity violation. The crash test uses this as the replay verifier.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	stdlog "log"
	"net/http"
	"os"
	"strings"
	"time"
)

type event struct {
	SiteSeq int64           `json:"site_seq"`
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
}

func main() {
	var (
		url   = flag.String("novixd", "http://127.0.0.1:9410", "novixd base URL")
		topic = flag.String("topic", "orders", "topic to consume")
		name  = flag.String("name", "kds", "consumer name for offset tracking")
		until = flag.Int64("until", 0, "exit after consuming this seq (0 = run forever)")
		quiet = flag.Bool("quiet", false, "suppress per-batch output")
	)
	flag.Parse()

	client := &http.Client{Timeout: 35 * time.Second}

	get := func(path string, v any) error {
		resp, err := client.Get(*url + path)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		return json.NewDecoder(resp.Body).Decode(v)
	}

	// Resume from committed offset.
	var off struct {
		Seq int64 `json:"seq"`
	}
	for {
		if err := get("/v1/consumers/"+*name+"/offsets/"+*topic, &off); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	next := off.Seq + 1
	if !*quiet {
		stdlog.Printf("kds resuming at seq %d", next)
	}

	consumed := 0
	for {
		var out struct {
			Events []event `json:"events"`
			MaxSeq int64   `json:"max_seq"`
		}
		path := fmt.Sprintf("/v1/topics/%s/events?after=%d&limit=500&wait_ms=30000", *topic, next-1)
		if err := get(path, &out); err != nil {
			// novixd down (crash window): wait and retry.
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, e := range out.Events {
			if e.SiteSeq != next {
				stdlog.Printf("CONTIGUITY VIOLATION: expected seq %d, got %d", next, e.SiteSeq)
				os.Exit(1)
			}
			next++
			consumed++
		}
		if len(out.Events) > 0 {
			// Commit offset (best effort; replay after crash is fine).
			body := fmt.Sprintf(`{"seq":%d}`, next-1)
			req, _ := http.NewRequest("PUT",
				*url+"/v1/consumers/"+*name+"/offsets/"+*topic,
				strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			if resp, err := client.Do(req); err == nil {
				resp.Body.Close()
			}
			if !*quiet {
				stdlog.Printf("consumed through seq %d (%d this run)", next-1, consumed)
			}
		}
		if *until > 0 && next > *until {
			stdlog.Printf("reached seq %d, contiguity verified, exiting clean", *until)
			return
		}
	}
}

