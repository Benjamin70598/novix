// Package server exposes the site log to local applications over HTTP on
// localhost. Apps talk to this and nothing else; the WAN is novixd's problem
// (sync loop arrives in M2).
//
// API (all JSON):
//
//	POST /v1/topics/{topic}/events
//	    {"key": "...", "payload": <any JSON>, "class": "..."}
//	 -> {"site_seq": N}            ack means durably committed
//
//	GET  /v1/topics/{topic}/events?after=N&limit=M&wait_ms=T
//	 -> {"events": [...], "max_seq": N'}
//	    Long-polls up to wait_ms if no events are available.
//
//	PUT  /v1/consumers/{name}/offsets/{topic}   {"seq": N}
//	GET  /v1/consumers/{name}/offsets/{topic} -> {"seq": N}
//
//	GET  /v1/status -> {"site": "...", "topics": {"orders": 123, ...}}
package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	nlog "github.com/Benjamin70598/novix/internal/log"
)

// Server serves the local API for one site.
type Server struct {
	Site string
	Log  *nlog.Log

	// Classify, when set, is the policy engine: it returns the class to
	// stamp on an event written to a topic. The client-supplied class is
	// ignored when a policy is loaded; policy is authoritative
	// (DECISIONS.md 006).
	Classify func(topic string) string
	// AfterAppend, when set, is called after a durable append (used to
	// kick the sync loop for immediate-class events).
	AfterAppend func(topic, class string)
	// StatusExtra, when set, contributes extra fields (sync state,
	// bandwidth meters) to /v1/status.
	StatusExtra func() map[string]any
}

// Handler returns the http.Handler for the local API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/topics/{topic}/events", s.handleAppend)
	mux.HandleFunc("GET /v1/topics/{topic}/events", s.handleRead)
	mux.HandleFunc("PUT /v1/consumers/{name}/offsets/{topic}", s.handleCommitOffset)
	mux.HandleFunc("GET /v1/consumers/{name}/offsets/{topic}", s.handleGetOffset)
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	return mux
}

type appendReq struct {
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
	Class   string          `json:"class"`
}

func (s *Server) handleAppend(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	var req appendReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpErr(w, http.StatusBadRequest, "bad json: "+err.Error())
		return
	}
	if len(req.Payload) == 0 {
		httpErr(w, http.StatusBadRequest, "missing payload")
		return
	}
	class := req.Class
	if s.Classify != nil {
		class = s.Classify(topic)
	}
	seq, err := s.Log.Append(topic, req.Key, req.Payload, class)
	if err != nil {
		httpErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	if s.AfterAppend != nil {
		s.AfterAppend(topic, class)
	}
	writeJSON(w, map[string]int64{"site_seq": seq})
}

func (s *Server) handleRead(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	after := queryInt(r, "after", 0)
	limit := int(queryInt(r, "limit", 500))
	waitMS := queryInt(r, "wait_ms", 0)

	deadline := time.Now().Add(time.Duration(waitMS) * time.Millisecond)
	for {
		evs, err := s.Log.Read(topic, after, limit)
		if err != nil {
			httpErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if len(evs) > 0 || waitMS <= 0 || time.Now().After(deadline) {
			max, _ := s.Log.MaxSeq(topic)
			writeJSON(w, map[string]any{"events": evs, "max_seq": max})
			return
		}
		// Long-poll: wait for the next append or the deadline.
		select {
		case <-s.Log.WaitCh(topic):
		case <-time.After(time.Until(deadline)):
		case <-r.Context().Done():
			return
		}
	}
}

type offsetReq struct {
	Seq int64 `json:"seq"`
}

func (s *Server) handleCommitOffset(w http.ResponseWriter, r *http.Request) {
	var req offsetReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpErr(w, http.StatusBadRequest, "bad json")
		return
	}
	if err := s.Log.CommitOffset(r.PathValue("name"), r.PathValue("topic"), req.Seq); err != nil {
		httpErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]string{"ok": "true"})
}

func (s *Server) handleGetOffset(w http.ResponseWriter, r *http.Request) {
	seq, err := s.Log.Offset(r.PathValue("name"), r.PathValue("topic"))
	if err != nil {
		httpErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]int64{"seq": seq})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	topics, err := s.Log.Topics()
	if err != nil {
		httpErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := map[string]any{"site": s.Site, "topics": topics}
	if s.StatusExtra != nil {
		out["sync"] = s.StatusExtra()
	}
	writeJSON(w, out)
}

func queryInt(r *http.Request, name string, def int64) int64 {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func httpErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
