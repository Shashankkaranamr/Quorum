package statemachine

import (
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"

	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/kv/v1"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// Result is what applying one entry produced.
//
// For a write it is everything the client's response depends on, including
// whether the response came from the session cache. Phase 5's pending-proposal
// registry hands it to the waiting RPC; tests read it through OnApply.
type Result struct {
	Index raft.Index
	Type  raft.EntryType

	// ClientID is the session the entry belongs to. For an EntrySession it is
	// the id just assigned, which is the entry's own index.
	ClientID uint64
	Seq      uint64

	// Status is STATUS_OK, or STATUS_SESSION_EXPIRED for a command from a
	// client with no session -- in which case nothing was applied.
	Status kvv1.Status

	// Duplicate is true when the command's seq had already been applied, so
	// the response below is the cached one and nothing was applied again.
	Duplicate bool

	// AppliedIndex is where the write took effect: this entry's index for a
	// fresh apply, the original index for a duplicate.
	AppliedIndex raft.Index

	// Existed is DeleteResponse.existed.
	Existed bool
}

// session is one client's deduplication state: the highest seq applied and the
// response it produced, kept to answer a retry of it verbatim.
type session struct {
	lastSeq  uint64
	response cachedResponse
}

type cachedResponse struct {
	appliedIndex raft.Index
	existed      bool
}

// KV is the replicated key-value store and its client session table.
//
// It is a pure function of the entries applied to it: no clock, no randomness,
// no I/O. Two replicas that apply the same entries hold identical state, and
// Snapshot renders that state to identical bytes, which is what lets a test
// compare replicas by hash.
//
// It is not safe for concurrent use. The driver goroutine owns it.
type KV struct {
	applied  raft.Index
	data     map[string][]byte
	sessions map[uint64]*session

	// OnApply, if set, is called with the result of every entry applied.
	OnApply func(Result)

	// last is the result of the most recent ApplyEntry, for LastResult.
	last Result
}

// New returns an empty store.
func New() *KV {
	return &KV{data: map[string][]byte{}, sessions: map[uint64]*session{}}
}

// Apply implements server.StateMachine.
func (m *KV) Apply(entries []raft.Entry) error {
	for _, e := range entries {
		res, err := m.ApplyEntry(e)
		if err != nil {
			return err
		}
		if m.OnApply != nil {
			m.OnApply(res)
		}
	}
	return nil
}

// ApplyEntry applies one committed entry.
//
// An entry that cannot be decoded is an error, not something to skip. Every
// replica would fail on it identically, so skipping would be deterministic --
// but only the kvservice proposes, so an undecodable command is a bug, and
// papering over it would hide the bug behind a replica that silently ignored a
// write the client was told had committed.
func (m *KV) ApplyEntry(e raft.Entry) (Result, error) {
	if e.Index != m.applied+1 {
		return Result{}, fmt.Errorf("statemachine: applying index %d after %d; entries must arrive "+
			"in order with none missing", e.Index, m.applied)
	}
	res := Result{Index: e.Index, Type: e.Type, Status: kvv1.Status_STATUS_OK}

	switch e.Type {
	case raft.EntryNoOp:
		// Nothing to apply; it exists so a new leader can commit in its term.

	case raft.EntrySession:
		var cmd kvv1.RegisterClientCommand
		if err := proto.Unmarshal(e.Data, &cmd); err != nil {
			return Result{}, fmt.Errorf("statemachine: session entry %d does not decode: %w", e.Index, err)
		}
		// The client id IS the index: unique cluster-wide with no coordination.
		m.sessions[uint64(e.Index)] = &session{}
		res.ClientID = uint64(e.Index)

	case raft.EntryNormal:
		var cmd kvv1.Command
		if err := proto.Unmarshal(e.Data, &cmd); err != nil {
			return Result{}, fmt.Errorf("statemachine: entry %d does not decode as a command: %w", e.Index, err)
		}
		if err := m.applyCommand(e.Index, &cmd, &res); err != nil {
			return Result{}, err
		}

	default:
		return Result{}, fmt.Errorf("statemachine: entry %d has type %s, which this state machine "+
			"does not apply", e.Index, e.Type)
	}

	m.applied = e.Index
	m.last = res
	return res, nil
}

// LastResult is the Result of the most recently applied entry. It implements
// server.ResultReporter, which is how the driver loop hands each write's
// outcome to the RPC waiting for it without the loop knowing what a key-value
// store is.
func (m *KV) LastResult() any { return m.last }

func (m *KV) applyCommand(idx raft.Index, cmd *kvv1.Command, res *Result) error {
	res.ClientID, res.Seq = cmd.GetClientId(), cmd.GetSeq()

	s, ok := m.sessions[cmd.GetClientId()]
	if !ok {
		res.Status = kvv1.Status_STATUS_SESSION_EXPIRED
		return nil
	}
	if cmd.GetSeq() <= s.lastSeq {
		// A retry. The single-slot cache holds the response to lastSeq; with
		// one request in flight per session (DESIGN.md §4) a retry can only
		// be of that request.
		res.Duplicate = true
		res.AppliedIndex = s.response.appliedIndex
		res.Existed = s.response.existed
		return nil
	}

	switch op := cmd.GetOp().(type) {
	case *kvv1.Command_Put:
		m.data[op.Put.GetKey()] = append([]byte(nil), op.Put.GetValue()...)
	case *kvv1.Command_Delete:
		_, res.Existed = m.data[op.Delete.GetKey()]
		delete(m.data, op.Delete.GetKey())
	default:
		return fmt.Errorf("statemachine: command at index %d carries no operation", idx)
	}
	res.AppliedIndex = idx
	s.lastSeq = cmd.GetSeq()
	s.response = cachedResponse{appliedIndex: idx, existed: res.Existed}
	return nil
}

// Get returns the value stored under key.
//
// It reads local state only. Whether that state is fresh enough to serve is the
// caller's question -- ReadIndex, in phase 5 -- not this package's.
func (m *KV) Get(key string) ([]byte, bool) {
	v, ok := m.data[key]
	return v, ok
}

// Len is the number of keys stored.
func (m *KV) Len() int { return len(m.data) }

// AppliedIndex is the index of the last entry applied.
func (m *KV) AppliedIndex() raft.Index { return m.applied }

// Session reports a client's last applied seq, and whether the client has a
// session at all.
func (m *KV) Session(clientID uint64) (lastSeq uint64, ok bool) {
	s, ok := m.sessions[clientID]
	if !ok {
		return 0, false
	}
	return s.lastSeq, true
}

// Snapshot renders the whole state -- data and sessions -- as of AppliedIndex.
//
// The encoding is deterministic: keys and client ids are sorted and the
// protobuf is marshalled deterministically, so equal states give equal bytes.
func (m *KV) Snapshot() ([]byte, raft.Index, error) {
	snap := &kvv1.StateMachineSnapshot{AppliedIndex: uint64(m.applied)}
	for _, k := range slices.Sorted(maps.Keys(m.data)) {
		snap.Data = append(snap.Data, &kvv1.KeyValue{Key: k, Value: m.data[k]})
	}
	for _, id := range slices.Sorted(maps.Keys(m.sessions)) {
		s := m.sessions[id]
		snap.Sessions = append(snap.Sessions, &kvv1.Session{
			ClientId: id,
			LastSeq:  s.lastSeq,
			LastResponse: &kvv1.CachedResponse{
				AppliedIndex: uint64(s.response.appliedIndex),
				Existed:      s.response.existed,
			},
		})
	}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(snap)
	if err != nil {
		return nil, 0, fmt.Errorf("statemachine: encode snapshot: %w", err)
	}
	return b, m.applied, nil
}

// Restore replaces the whole state with a snapshot's.
//
// The snapshot must describe the state at exactly snap.Meta.Index. A mismatch
// means the image and the log position it was filed under disagree, and
// restoring it would put the replica at a state no other replica passed
// through.
func (m *KV) Restore(snap raft.Snapshot) error {
	var img kvv1.StateMachineSnapshot
	if err := proto.Unmarshal(snap.Data, &img); err != nil {
		return fmt.Errorf("statemachine: snapshot at %d does not decode: %w", snap.Meta.Index, err)
	}
	if got := raft.Index(img.GetAppliedIndex()); got != snap.Meta.Index {
		return fmt.Errorf("statemachine: snapshot filed at index %d describes the state at %d",
			snap.Meta.Index, got)
	}
	data := make(map[string][]byte, len(img.GetData()))
	for _, kv := range img.GetData() {
		data[kv.GetKey()] = kv.GetValue()
	}
	sessions := make(map[uint64]*session, len(img.GetSessions()))
	for _, s := range img.GetSessions() {
		sessions[s.GetClientId()] = &session{
			lastSeq: s.GetLastSeq(),
			response: cachedResponse{
				appliedIndex: raft.Index(s.GetLastResponse().GetAppliedIndex()),
				existed:      s.GetLastResponse().GetExisted(),
			},
		}
	}
	m.applied, m.data, m.sessions = snap.Meta.Index, data, sessions
	return nil
}

// Hash is a digest of the complete state, sessions included. Two replicas with
// the same hash hold the same data and would deduplicate the same retries.
func (m *KV) Hash() ([32]byte, error) {
	b, _, err := m.Snapshot()
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}
