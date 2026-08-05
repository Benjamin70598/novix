// novixd is the Novix site agent. Milestone 1: local durable log + local
// pub/sub API on localhost. Policy engine and sync loop arrive in M2.
//
// There is deliberately no graceful shutdown handling: the durability story
// must hold under kill -9, so we do not rely on cleanup ever running.
package main

import (
	"flag"
	stdlog "log"
	"net/http"
	"os"
	"path/filepath"

	nlog "github.com/Benjamin70598/novix/internal/log"
	"github.com/Benjamin70598/novix/internal/server"
)

func main() {
	var (
		site    = flag.String("site", "store-a", "site identifier")
		dataDir = flag.String("data-dir", "./data", "directory for the site log")
		listen  = flag.String("listen", "127.0.0.1:9410", "local API listen address")
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

	srv := &server.Server{Site: *site, Log: l}
	stdlog.Printf("novixd site=%s data=%s listening on %s", *site, dbPath, *listen)
	if err := http.ListenAndServe(*listen, srv.Handler()); err != nil {
		stdlog.Fatal(err)
	}
}
