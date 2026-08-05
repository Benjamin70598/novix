// telegen simulates equipment telemetry: N devices emitting readings at a
// fixed aggregate rate. This is the workload that makes the bandwidth meter
// interesting: telemetry is high-volume and policy-aggregated, so almost
// none of it should cross the WAN raw.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	stdlog "log"
	"math/rand"
	"net/http"
	"time"
)

func main() {
	var (
		url     = flag.String("novixd", "http://127.0.0.1:9410", "novixd base URL")
		topic   = flag.String("topic", "fryer_telemetry", "topic to produce to")
		devices = flag.Int("devices", 4, "number of simulated devices")
		rate    = flag.Int("rate", 100, "total readings per second")
		count   = flag.Int("count", 3000, "total readings to produce (0 = forever)")
	)
	flag.Parse()

	client := &http.Client{Timeout: 5 * time.Second}
	interval := time.Second / time.Duration(*rate)
	temps := make([]float64, *devices)
	for i := range temps {
		temps[i] = 165 + rand.Float64()*10
	}

	sent := 0
	for *count == 0 || sent < *count {
		d := sent % *devices
		temps[d] += rand.Float64()*1.6 - 0.8 // gentle random walk
		payload := map[string]any{
			"device":  fmt.Sprintf("fryer-%d", d+1),
			"temp_c":  round1(temps[d]),
			"amps":    round1(11 + rand.Float64()*3),
		}
		body, _ := json.Marshal(map[string]any{"payload": payload})

		for {
			resp, err := client.Post(*url+"/v1/topics/"+*topic+"/events",
				"application/json", bytes.NewReader(body))
			if err == nil && resp.StatusCode == 200 {
				resp.Body.Close()
				break
			}
			if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(200 * time.Millisecond)
		}
		sent++
		if sent%1000 == 0 {
			stdlog.Printf("sent %d readings", sent)
		}
		time.Sleep(interval)
	}
	stdlog.Printf("done: %d readings", sent)
}

func round1(f float64) float64 { return float64(int(f*10)) / 10 }
