// novixd is the Novix runtime: one binary, two modes.
//
//	agent (default)  runs at a site: durable local log, localhost API for
//	                 apps, policy engine, and the sync loop pushing to the
//	                 hub. Fully functional with zero connectivity.
//	hub              runs centrally: idempotent sink for site sync,
//	                 fleet view.
//
// There is deliberately no graceful shutdown handling: the durability story
// must hold under kill -9, so we do not rely on cleanup ever running.
package main

import (
	"context"
	"flag"
	stdlog "log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/Benjamin70598/novix/internal/hub"
	nlog "github.com/Benjamin70598/novix/internal/log"
	"github.com/Benjamin70598/novix/internal/policy"
	"github.com/Benjamin70598/novix/internal/server"
	nsync "github.com/Benjamin70598/novix/internal/sync"
)

func main() {
	var (
		mode       = flag.String("mode", "agent", "agent or hub")
		site       = flag.String("site", "store-a", "site identifier (agent mode)")
		dataDir    = flag.String("data-dir", "./data", "directory for the database")
		listen     = flag.String("listen", "", "listen address (default 127.0.0.1:9410 agent, 127.0.0.1:9400 hub)")
		policyPath = flag.String("policy", "", "path to novix.yaml (agent mode; optional)")
	)
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		stdlog.Fatalf("data dir: %v", err)
	}
	dbPath := filepath.Join(*dataDir, "novix.db")
	l, err := nlog.Open(dbPath)
	if err != nil {
		stdlog.Fatalf("open log: %v", err)
	}

	switch *mode {
	case "hub":
		addr := *listen
		if addr == "" {
			addr = "127.0.0.1:9400"
		}
		h, err := hub.Open(l.DB())
		if err != nil {
			stdlog.Fatalf("hub: %v", err)
		}
		stdlog.Printf("novixd hub data=%s listening on %s", dbPath, addr)
		stdlog.Fatal(http.ListenAndServe(addr, h.Handler()))

	case "agent":
		addr := *listen
		if addr == "" {
			addr = "127.0.0.1:9410"
		}
		srv := &server.Server{Site: *site, Log: l}

		if *policyPath != "" {
			pol, err := policy.Load(*policyPath)
			if err != nil {
				stdlog.Fatalf("policy: %v", err)
			}
			if pol.Site != "" {
				srv.Site = pol.Site
			}
			syncer := nsync.New(l, pol)
			srv.Classify = pol.Class
			srv.AfterAppend = func(topic, class string) {
				if class == "immediate" {
					syncer.Kick()
				}
			}
			srv.StatusExtra = syncer.Status
			if pol.Hub != "" {
				go syncer.Run(context.Background(), 0)
				stdlog.Printf("sync loop -> %s", pol.Hub)
			}
			stdlog.Printf("policy loaded from %s (%d topics declared)", *policyPath, len(pol.Topics))
		}

		stdlog.Printf("novixd agent site=%s data=%s listening on %s", srv.Site, dbPath, addr)
		stdlog.Fatal(http.ListenAndServe(addr, srv.Handler()))

	default:
		stdlog.Fatalf("unknown mode %q (agent or hub)", *mode)
	}
}
