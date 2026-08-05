// posgen simulates a POS terminal: it generates order events at a fixed rate
// and writes them to the local novixd. It records every ACKED order id to a
// ledger file. The crash test's contract:
//
//	every order id in the ledger MUST exist in the novixd log after any
//	number of kill -9 restarts of novixd.
//
// posgen retries on connection failure (novixd is down mid-crash) with a
// short backoff. If an ack is lost after commit, the retry may write a
// duplicate business event; that is at-least-once, and the verifier counts
// (but does not fail on) such duplicates. See internal/log package docs.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	stdlog "log"
	"math/rand"
	"net/http"
	"os"
	"time"
)

func main() {
	var (
		url    = flag.String("novixd", "http://127.0.0.1:9410", "novixd base URL")
		topic  = flag.String("topic", "orders", "topic to produce to")
		count  = flag.Int("count", 2000, "number of orders to produce")
		rate   = flag.Int("rate", 200, "target orders per second")
		site   = flag.String("site", "store-a", "site id used in order ids")
		ledger = flag.String("ledger", "acked.log", "file recording acked order ids")
	)
	flag.Parse()

	lf, err := os.Create(*ledger)
	if err != nil {
		stdlog.Fatalf("ledger: %v", err)
	}
	w := bufio.NewWriter(lf)
	defer func() {
		w.Flush()
		lf.Close()
	}()

	client := &http.Client{Timeout: 5 * time.Second}
	interval := time.Second / time.Duration(*rate)
	acked := 0

	for i := 1; i <= *count; i++ {
		orderID := fmt.Sprintf("%s-order-%06d", *site, i)
		payload := map[string]any{
			"order_id":     orderID,
			"amount_cents": 400 + rand.Intn(2000),
			"items":        1 + rand.Intn(5),
		}
		body, _ := json.Marshal(map[string]any{
			"key":     orderID,
			"payload": payload,
			"class":   "orders",
		})

		// Retry until acked. novixd being down (crash window) is normal.
		for attempt := 0; ; attempt++ {
			resp, err := client.Post(*url+"/v1/topics/"+*topic+"/events",
				"application/json", bytes.NewReader(body))
			if err == nil && resp.StatusCode == 200 {
				resp.Body.Close()
				break
			}
			if resp != nil {
				resp.Body.Close()
			}
			if attempt == 0 {
				stdlog.Printf("novixd unreachable, retrying (order %s)", orderID)
			}
			time.Sleep(200 * time.Millisecond)
		}

		fmt.Fprintln(w, orderID)
		acked++
		if acked%500 == 0 {
			w.Flush()
			stdlog.Printf("acked %d/%d", acked, *count)
		}
		time.Sleep(interval)
	}
	w.Flush()
	stdlog.Printf("done: %d orders acked, ledger %s", acked, *ledger)
}
