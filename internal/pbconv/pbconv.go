package pbconv

import (
	"fmt"

	raftv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/raft/v1"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// This package is the only place the consensus core's types meet protobuf.
//
// Keeping the conversion here rather than in package raft is what lets the
// purity test check "the core performs no I/O" by reading one package's import
// list: the protobuf runtime pulls in reflect, sync and os, so a core that
// imported it could not make that claim.
//
// The same encoding is used for the network and for the write-ahead log, so
// there is exactly one format to get right and exactly one to fuzz.

// EntryTypeToProto converts a core entry type.
//
// The mapping is written out rather than cast, even though the numeric values
// agree, because they are written to disk: a silent renumbering would make
// every previously written write-ahead log unreadable. TestEntryTypeValuesMatch
// asserts the two enumerations stay aligned.
func EntryTypeToProto(t raft.EntryType) (raftv1.EntryType, error) {
	switch t {
	case raft.EntryNormal:
		return raftv1.EntryType_ENTRY_TYPE_NORMAL, nil
	case raft.EntryNoOp:
		return raftv1.EntryType_ENTRY_TYPE_NOOP, nil
	case raft.EntrySession:
		return raftv1.EntryType_ENTRY_TYPE_SESSION, nil
	case raft.EntryConfig:
		return raftv1.EntryType_ENTRY_TYPE_CONFIG, nil
	case raft.EntryUnspecified:
		return raftv1.EntryType_ENTRY_TYPE_UNSPECIFIED, fmt.Errorf("pbconv: entry type is unspecified")
	default:
		return raftv1.EntryType_ENTRY_TYPE_UNSPECIFIED, fmt.Errorf("pbconv: unknown entry type %d", uint8(t))
	}
}

// EntryTypeFromProto converts a wire entry type.
func EntryTypeFromProto(t raftv1.EntryType) (raft.EntryType, error) {
	switch t {
	case raftv1.EntryType_ENTRY_TYPE_NORMAL:
		return raft.EntryNormal, nil
	case raftv1.EntryType_ENTRY_TYPE_NOOP:
		return raft.EntryNoOp, nil
	case raftv1.EntryType_ENTRY_TYPE_SESSION:
		return raft.EntrySession, nil
	case raftv1.EntryType_ENTRY_TYPE_CONFIG:
		return raft.EntryConfig, nil
	default:
		return raft.EntryUnspecified, fmt.Errorf("pbconv: unknown wire entry type %d", int32(t))
	}
}

// EntryToProto converts one log entry.
func EntryToProto(e raft.Entry) (*raftv1.Entry, error) {
	t, err := EntryTypeToProto(e.Type)
	if err != nil {
		return nil, fmt.Errorf("entry %d@%d: %w", e.Index, e.Term, err)
	}
	return &raftv1.Entry{
		Term:  uint64(e.Term),
		Index: uint64(e.Index),
		Type:  t,
		Data:  e.Data,
	}, nil
}

// EntryFromProto converts one log entry back.
func EntryFromProto(p *raftv1.Entry) (raft.Entry, error) {
	if p == nil {
		return raft.Entry{}, fmt.Errorf("pbconv: nil entry")
	}
	t, err := EntryTypeFromProto(p.GetType())
	if err != nil {
		return raft.Entry{}, fmt.Errorf("entry %d@%d: %w", p.GetIndex(), p.GetTerm(), err)
	}
	return raft.Entry{
		Term:  raft.Term(p.GetTerm()),
		Index: raft.Index(p.GetIndex()),
		Type:  t,
		Data:  p.GetData(),
	}, nil
}

// EntriesToProto converts a batch.
func EntriesToProto(ents []raft.Entry) ([]*raftv1.Entry, error) {
	if len(ents) == 0 {
		return nil, nil
	}
	out := make([]*raftv1.Entry, len(ents))
	for i, e := range ents {
		p, err := EntryToProto(e)
		if err != nil {
			return nil, err
		}
		out[i] = p
	}
	return out, nil
}

// EntriesFromProto converts a batch back.
func EntriesFromProto(ps []*raftv1.Entry) ([]raft.Entry, error) {
	if len(ps) == 0 {
		return nil, nil
	}
	out := make([]raft.Entry, len(ps))
	for i, p := range ps {
		e, err := EntryFromProto(p)
		if err != nil {
			return nil, err
		}
		out[i] = e
	}
	return out, nil
}

// HardStateToProto converts the durable state.
func HardStateToProto(hs raft.HardState) *raftv1.HardState {
	return &raftv1.HardState{
		Term:     uint64(hs.Term),
		VotedFor: uint64(hs.VotedFor),
		Commit:   uint64(hs.Commit),
	}
}

// HardStateFromProto converts it back.
func HardStateFromProto(p *raftv1.HardState) raft.HardState {
	if p == nil {
		return raft.HardState{}
	}
	return raft.HardState{
		Term:     raft.Term(p.GetTerm()),
		VotedFor: raft.NodeID(p.GetVotedFor()),
		Commit:   raft.Index(p.GetCommit()),
	}
}

// MessageToProto converts a Raft message to its wire form.
//
// The core uses a flat struct with a type tag; the wire uses a oneof. The
// switch here is the whole reason both can exist: the flat struct is easier to
// construct and pattern-match in the algorithm, and the oneof is what makes the
// schema self-describing and safely extensible.
func MessageToProto(m raft.Message) (*raftv1.Message, error) {
	out := &raftv1.Message{
		From: uint64(m.From),
		To:   uint64(m.To),
		Term: uint64(m.Term),
	}

	switch m.Type {
	case raft.MsgRequestVote:
		out.Body = &raftv1.Message_RequestVote{RequestVote: &raftv1.RequestVote{
			LastLogIndex: uint64(m.LastLogIndex),
			LastLogTerm:  uint64(m.LastLogTerm),
		}}

	case raft.MsgRequestVoteResp:
		out.Body = &raftv1.Message_RequestVoteResponse{
			RequestVoteResponse: &raftv1.RequestVoteResponse{VoteGranted: m.VoteGranted},
		}

	case raft.MsgAppendEntries:
		ents, err := EntriesToProto(m.Entries)
		if err != nil {
			return nil, err
		}
		out.Body = &raftv1.Message_AppendEntries{AppendEntries: &raftv1.AppendEntries{
			PrevLogIndex: uint64(m.PrevLogIndex),
			PrevLogTerm:  uint64(m.PrevLogTerm),
			Entries:      ents,
			LeaderCommit: uint64(m.LeaderCommit),
		}}

	case raft.MsgAppendEntriesResp:
		out.Body = &raftv1.Message_AppendEntriesResponse{
			AppendEntriesResponse: &raftv1.AppendEntriesResponse{
				Success:       m.Success,
				MatchIndex:    uint64(m.MatchIndex),
				ConflictTerm:  uint64(m.ConflictTerm),
				ConflictIndex: uint64(m.ConflictIndex),
			},
		}

	case raft.MsgInstallSnapshot, raft.MsgInstallSnapshotResp:
		return nil, fmt.Errorf("pbconv: %s is not implemented until phase 4", m.Type)

	default:
		return nil, fmt.Errorf("pbconv: unknown message type %s", m.Type)
	}
	return out, nil
}

// MessageFromProto converts a wire message back.
func MessageFromProto(p *raftv1.Message) (raft.Message, error) {
	if p == nil {
		return raft.Message{}, fmt.Errorf("pbconv: nil message")
	}

	m := raft.Message{
		From: raft.NodeID(p.GetFrom()),
		To:   raft.NodeID(p.GetTo()),
		Term: raft.Term(p.GetTerm()),
	}

	switch b := p.GetBody().(type) {
	case *raftv1.Message_RequestVote:
		m.Type = raft.MsgRequestVote
		m.LastLogIndex = raft.Index(b.RequestVote.GetLastLogIndex())
		m.LastLogTerm = raft.Term(b.RequestVote.GetLastLogTerm())

	case *raftv1.Message_RequestVoteResponse:
		m.Type = raft.MsgRequestVoteResp
		m.VoteGranted = b.RequestVoteResponse.GetVoteGranted()

	case *raftv1.Message_AppendEntries:
		m.Type = raft.MsgAppendEntries
		ents, err := EntriesFromProto(b.AppendEntries.GetEntries())
		if err != nil {
			return raft.Message{}, err
		}
		m.PrevLogIndex = raft.Index(b.AppendEntries.GetPrevLogIndex())
		m.PrevLogTerm = raft.Term(b.AppendEntries.GetPrevLogTerm())
		m.Entries = ents
		m.LeaderCommit = raft.Index(b.AppendEntries.GetLeaderCommit())

	case *raftv1.Message_AppendEntriesResponse:
		m.Type = raft.MsgAppendEntriesResp
		m.Success = b.AppendEntriesResponse.GetSuccess()
		m.MatchIndex = raft.Index(b.AppendEntriesResponse.GetMatchIndex())
		m.ConflictTerm = raft.Term(b.AppendEntriesResponse.GetConflictTerm())
		m.ConflictIndex = raft.Index(b.AppendEntriesResponse.GetConflictIndex())

	case *raftv1.Message_InstallSnapshot, *raftv1.Message_InstallSnapshotResponse:
		return raft.Message{}, fmt.Errorf("pbconv: InstallSnapshot is not implemented until phase 4")

	default:
		return raft.Message{}, fmt.Errorf("pbconv: message from %d carries no recognized body",
			p.GetFrom())
	}
	return m, nil
}
