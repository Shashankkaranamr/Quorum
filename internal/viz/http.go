package viz

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/Shashankkaranamr/Quorum/raft"
)

// ControlHeader must accompany every request that changes anything. A web page
// on another origin cannot set a custom header on a cross-site request without
// a CORS preflight, which this server never approves, so the header is what
// stops some other site from making your browser kill your nodes.
const ControlHeader = "X-Quorum-Viz"

// route is one entry of the HTTP surface. The table in routes is the whole of
// it: TestRoutesAreExactlyTheDocumentedControls pins it, so a new way into the
// cluster cannot appear without the test being changed on purpose.
type route struct {
	Pattern string
	Does    string // what it reaches, for the test and for a reader
	handle  func(*Server, http.ResponseWriter, *http.Request)
}

func routes() []route {
	return []route{
		{"GET /", "static frontend from embed.FS", (*Server).static},
		{"GET /api/state", "read: the aggregated picture", (*Server).getState},
		{"GET /api/events", "read: the picture, streamed over server-sent events", (*Server).events},
		{"POST /api/nodes/{id}/kill", "supervisor.Kill: TerminateProcess/SIGKILL", (*Server).kill},
		{"POST /api/nodes/{id}/start", "supervisor.Start: respawn on the same data directory", (*Server).start},
		{"POST /api/nodes/{id}/freeze", "admin Freeze, through supervisor.Freeze", (*Server).freeze},
		{"POST /api/nodes/{id}/thaw", "admin Thaw, through supervisor.Thaw", (*Server).thaw},
		{"POST /api/partition", "admin BlockLinks, through supervisor.Partition", (*Server).partition},
		{"POST /api/heal", "admin Heal, through supervisor.Heal", (*Server).heal},
		{"POST /api/put", "KV Put, through the client library", (*Server).put},
		{"POST /api/load", "KV Put in the background, through the client library", (*Server).load},
	}
}

// Handler serves the frontend, the state stream and the controls.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, rt := range routes() {
		h := rt.handle
		mux.HandleFunc(rt.Pattern, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodPost && !sameOrigin(r) {
				http.Error(w, "cross-site control requests are refused; send "+ControlHeader+": 1 from the same origin",
					http.StatusForbidden)
				return
			}
			h(s, w, r)
		})
	}
	return mux
}

// sameOrigin reports whether a control request came from this server's own
// page: it carries the control header, and any Origin it sends is ours.
func sameOrigin(r *http.Request) bool {
	if r.Header.Get(ControlHeader) != "1" {
		return false
	}
	if o := r.Header.Get("Origin"); o != "" && o != "http://"+r.Host && o != "https://"+r.Host {
		return false
	}
	return true
}

func (s *Server) static(w http.ResponseWriter, r *http.Request) {
	http.FileServerFS(s.assets).ServeHTTP(w, r)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) getState(w http.ResponseWriter, _ *http.Request) { writeJSON(w, s.State()) }

// events streams the picture as server-sent events, one "state" event per
// change, until the browser goes away.
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	states, unsubscribe := s.Subscribe()
	defer unsubscribe()
	keepalive := time.NewTicker(15 * time.Second)
	defer keepalive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case st, ok := <-states:
			if !ok {
				return
			}
			b, err := json.Marshal(st)
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "event: state\ndata: %s\n\n", b); err != nil {
				return
			}
			flusher.Flush()
		case <-keepalive.C:
			if _, err := fmt.Fprint(w, ": keepalive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// nodeID parses and checks the {id} in a path.
func (s *Server) nodeID(r *http.Request) (raft.NodeID, error) {
	n, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%q is not a node id", r.PathValue("id"))
	}
	for _, id := range s.sup.IDs() {
		if uint64(id) == n {
			return id, nil
		}
	}
	return 0, fmt.Errorf("node %d is not in the cluster", n)
}

// act runs one control and answers the request with its outcome.
func (s *Server) act(w http.ResponseWriter, r *http.Request, run func(ctx context.Context) (string, error)) {
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	if err := s.do(ctx, run); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func (s *Server) withNode(w http.ResponseWriter, r *http.Request, run func(ctx context.Context, id raft.NodeID) (string, error)) {
	id, err := s.nodeID(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.act(w, r, func(ctx context.Context) (string, error) { return run(ctx, id) })
}

func (s *Server) kill(w http.ResponseWriter, r *http.Request) {
	s.withNode(w, r, func(_ context.Context, id raft.NodeID) (string, error) {
		pid, _ := s.sup.PID(id)
		if err := s.sup.Kill(id); err != nil {
			return "", err
		}
		return fmt.Sprintf("control|killed node %d (pid %d); the process is gone", id, pid), nil
	})
}

func (s *Server) start(w http.ResponseWriter, r *http.Request) {
	s.withNode(w, r, func(_ context.Context, id raft.NodeID) (string, error) {
		pid, err := s.sup.Start(id)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("control|started node %d (pid %d) on its existing data", id, pid), nil
	})
}

func (s *Server) freeze(w http.ResponseWriter, r *http.Request) {
	var body struct {
		MS int `json:"ms"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body) // optional; zero means until thawed
	s.withNode(w, r, func(ctx context.Context, id raft.NodeID) (string, error) {
		if err := s.sup.Freeze(ctx, id, time.Duration(body.MS)*time.Millisecond); err != nil {
			return "", err
		}
		return fmt.Sprintf("control|froze node %d (its loop is parked; the process is alive)", id), nil
	})
}

func (s *Server) thaw(w http.ResponseWriter, r *http.Request) {
	s.withNode(w, r, func(ctx context.Context, id raft.NodeID) (string, error) {
		rep, err := s.sup.Thaw(ctx, id)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("control|thawed node %d after %dms, %d ticks missed", id, rep.GetFrozenForMs(), rep.GetTicksMissed()), nil
	})
}

func (s *Server) partition(w http.ResponseWriter, r *http.Request) {
	var body struct {
		A      []uint64 `json:"a"`
		B      []uint64 `json:"b"`
		OneWay bool     `json:"oneway"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "partition: "+err.Error(), http.StatusBadRequest)
		return
	}
	a, err := s.ids(body.A)
	if err == nil {
		var b []raft.NodeID
		if b, err = s.ids(body.B); err == nil {
			for _, x := range a {
				for _, y := range b {
					if x == y {
						err = fmt.Errorf("node %d is on both sides", x)
					}
				}
			}
			if err == nil {
				s.act(w, r, func(ctx context.Context) (string, error) {
					if err := s.sup.Partition(ctx, a, b, body.OneWay); err != nil {
						return "", err
					}
					arrow := "<->"
					if body.OneWay {
						arrow = "->"
					}
					return fmt.Sprintf("control|partitioned %v %s %v (transport-level)", a, arrow, b), nil
				})
				return
			}
		}
	}
	http.Error(w, "partition: "+err.Error(), http.StatusBadRequest)
}

func (s *Server) ids(in []uint64) ([]raft.NodeID, error) {
	if len(in) == 0 {
		return nil, errors.New("empty group")
	}
	var out []raft.NodeID
	for _, n := range in {
		ok := false
		for _, id := range s.sup.IDs() {
			ok = ok || uint64(id) == n
		}
		if !ok {
			return nil, fmt.Errorf("node %d is not in the cluster", n)
		}
		out = append(out, raft.NodeID(n))
	}
	return out, nil
}

func (s *Server) heal(w http.ResponseWriter, r *http.Request) {
	s.act(w, r, func(ctx context.Context) (string, error) {
		if err := s.sup.Heal(ctx); err != nil {
			return "", err
		}
		return "control|healed every injected partition", nil
	})
}

func (s *Server) put(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Key == "" {
		http.Error(w, "put: need a JSON body with a non-empty key", http.StatusBadRequest)
		return
	}
	s.act(w, r, func(ctx context.Context) (string, error) {
		cl, err := s.client(ctx)
		if err != nil {
			return "", err
		}
		res, err := cl.Put(ctx, body.Key, []byte(body.Value))
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("control|put %s at index %d", body.Key, res.AppliedIndex), nil
	})
}

func (s *Server) load(w http.ResponseWriter, r *http.Request) {
	var body struct {
		On bool `json:"on"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "load: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := s.SetLoad(r.Context(), body.On); err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}
