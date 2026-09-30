package admin

import (
	"context"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
	raftv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/raft/v1"
	"github.com/Shashankkaranamr/Quorum/internal/server"
	"github.com/Shashankkaranamr/Quorum/internal/statemachine"
	"github.com/Shashankkaranamr/Quorum/internal/transport/grpcx"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// Config is what the service needs to know about its node.
type Config struct {
	ID    raft.NodeID
	Peers []raft.NodeID // every node, including this one

	// Tick is the length of one logical tick, to turn tick counts into
	// milliseconds for display.
	Tick time.Duration

	// ReachableTicks is how recently a leader must have heard from a
	// follower for the link to count as reachable. The election timeout's
	// lower bound is the natural choice: silence that long is what makes a
	// follower campaign.
	ReachableTicks int
}

// Service implements the Admin API for one node.
//
// Status comes from the loop's published snapshot and never goes through the
// loop, so it is answered even while the node is frozen -- which is exactly
// when an operator most wants to see it. Freeze and thaw are the only calls
// that reach the loop, over its control channel.
type Service struct {
	adminv1.UnimplementedAdminServer

	cfg     Config
	loop    *server.Loop
	trans   *grpcx.Transport
	started time.Time
}

// New creates the service.
func New(cfg Config, loop *server.Loop, trans *grpcx.Transport) *Service {
	return &Service{cfg: cfg, loop: loop, trans: trans, started: time.Now()}
}

// Register adds the Admin service to a gRPC server.
func (s *Service) Register(r grpc.ServiceRegistrar) { adminv1.RegisterAdminServer(r, s) }

var roles = map[raft.Role]adminv1.Role{
	raft.Follower:  adminv1.Role_ROLE_FOLLOWER,
	raft.Candidate: adminv1.Role_ROLE_CANDIDATE,
	raft.Leader:    adminv1.Role_ROLE_LEADER,
}

func (s *Service) status(tailLimit uint32) *adminv1.NodeStatus {
	snap := s.loop.Snapshot()
	st := snap.Status
	out := &adminv1.NodeStatus{
		NodeId:           uint64(st.ID),
		Role:             roles[st.Role],
		Term:             uint64(st.Term),
		VotedFor:         uint64(st.VotedFor),
		LeaderId:         uint64(st.Leader),
		CommitIndex:      uint64(st.CommitIndex),
		LastApplied:      uint64(st.LastApplied),
		LastLogIndex:     uint64(st.LastLogIndex),
		LastLogTerm:      uint64(st.LastLogTerm),
		SnapshotIndex:    uint64(st.SnapshotIndex),
		Frozen:           snap.Frozen,
		UptimeMs:         time.Since(s.started).Milliseconds(),
		ObservedAtUnixMs: snap.PublishedAt.UnixMilli(),
		Metrics: &adminv1.Metrics{
			FsyncCount:     snap.Fsyncs,
			FsyncP99Micros: uint64(snap.FsyncP99.Microseconds()),
			// The worst backlog seen, not the instantaneous one: the
			// snapshot is published after the loop has caught up, when
			// the current backlog is zero by construction.
			TickLagTicks:      snap.Metrics.MaxTickLagTicks,
			ProposalsAccepted: snap.ProposalsAccepted,
			ProposalsRejected: snap.ProposalsRejected,
			MessagesSent:      snap.Metrics.MessagesSent,
			MessagesReceived:  snap.MessagesReceived,
			ElectionsStarted:  st.ElectionsStarted,
			SnapshotsTaken:    snap.Metrics.SnapshotsTaken,
		},
	}

	out2, in := s.trans.Blocked()
	for _, p := range s.cfg.Peers {
		if p == s.cfg.ID {
			continue
		}
		bOut, bIn := slices.Contains(out2, p), slices.Contains(in, p)
		blocked := bOut || bIn
		pv := &adminv1.PeerView{NodeId: uint64(p), BlockedByInjection: blocked, BlockedOutbound: bOut,
			BlockedInbound: bIn, MsSinceLastContact: -1, Reachable: !blocked}
		// Only a leader hears from every peer regularly, so only a leader
		// can say how recently it did. Elsewhere reachability is what the
		// injector knows, and ms_since_last_contact is -1: unknown.
		if pr, ok := st.Progress[p]; ok && st.Role == raft.Leader {
			pv.NextIndex, pv.MatchIndex = uint64(pr.NextIndex), uint64(pr.MatchIndex)
			pv.MsSinceLastContact = (time.Duration(pr.TicksSinceContact) * s.cfg.Tick).Milliseconds()
			pv.Reachable = !blocked && pr.TicksSinceContact < s.cfg.ReachableTicks
		}
		out.Peers = append(out.Peers, pv)
	}

	tail := snap.LogTail
	if n := int(tailLimit); n < len(tail) {
		tail = tail[len(tail)-n:]
	}
	for _, e := range tail {
		out.LogTail = append(out.LogTail, &adminv1.LogEntryView{
			Index:     uint64(e.Index),
			Term:      uint64(e.Term),
			Type:      raftv1.EntryType(e.Type),
			Summary:   statemachine.Describe(e),
			Committed: e.Index <= st.CommitIndex,
			Applied:   e.Index <= st.LastApplied,
		})
	}
	return out
}

// GetStatus implements adminv1.AdminServer.
func (s *Service) GetStatus(_ context.Context, req *adminv1.GetStatusRequest) (*adminv1.NodeStatus, error) {
	return s.status(req.GetLogTailLimit()), nil
}

// minWatchInterval keeps a watcher from asking for updates faster than a
// display could use them. It costs the loop nothing either way -- the status
// is a published snapshot -- but it bounds the traffic.
const minWatchInterval = 50 * time.Millisecond

// WatchStatus implements adminv1.AdminServer: the status, resent at an
// interval, until the client goes away.
func (s *Service) WatchStatus(req *adminv1.WatchStatusRequest, stream grpc.ServerStreamingServer[adminv1.NodeStatus]) error {
	every := max(time.Duration(req.GetMinIntervalMs())*time.Millisecond, minWatchInterval)
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		if err := stream.Send(s.status(req.GetLogTailLimit())); err != nil {
			return err
		}
		select {
		case <-stream.Context().Done():
			return nil
		case <-ticker.C:
		}
	}
}

// BlockLinks implements adminv1.AdminServer.
func (s *Service) BlockLinks(_ context.Context, req *adminv1.BlockLinksRequest) (*adminv1.BlockLinksResponse, error) {
	if req.GetInboundOnly() && req.GetOutboundOnly() {
		return nil, status.Error(codes.InvalidArgument, "inbound_only and outbound_only are exclusive; set neither to cut both directions")
	}
	var peers []raft.NodeID
	for _, id := range req.GetPeerIds() {
		p := raft.NodeID(id)
		if p == s.cfg.ID || !slices.Contains(s.cfg.Peers, p) {
			return nil, status.Errorf(codes.InvalidArgument, "node %d is not a peer of node %d", id, s.cfg.ID)
		}
		peers = append(peers, p)
	}
	if !req.GetInboundOnly() {
		s.trans.BlockOutbound(peers...)
	}
	if !req.GetOutboundOnly() {
		s.trans.BlockInbound(peers...)
	}
	return &adminv1.BlockLinksResponse{BlockedPeerIds: s.blockedIDs()}, nil
}

func (s *Service) blockedIDs() []uint64 {
	out, in := s.trans.Blocked()
	var ids []uint64
	for _, p := range slices.Compact(slices.Sorted(slices.Values(append(out, in...)))) {
		ids = append(ids, uint64(p))
	}
	return ids
}

// Heal implements adminv1.AdminServer.
func (s *Service) Heal(context.Context, *adminv1.HealRequest) (*adminv1.HealResponse, error) {
	was := s.blockedIDs()
	s.trans.Heal()
	return &adminv1.HealResponse{UnblockedPeerIds: was}, nil
}

// Freeze implements adminv1.AdminServer.
func (s *Service) Freeze(ctx context.Context, req *adminv1.FreezeRequest) (*adminv1.FreezeResponse, error) {
	auto := time.Duration(req.GetAutoThawAfterMs()) * time.Millisecond
	if err := s.loop.Freeze(ctx, auto); err != nil {
		return nil, status.Errorf(codes.Unavailable, "admin: freeze: %v", err)
	}
	return &adminv1.FreezeResponse{}, nil
}

// Thaw implements adminv1.AdminServer.
func (s *Service) Thaw(ctx context.Context, _ *adminv1.ThawRequest) (*adminv1.ThawResponse, error) {
	r, err := s.loop.Thaw(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "admin: thaw: %v", err)
	}
	return &adminv1.ThawResponse{FrozenForMs: uint64(r.FrozenFor.Milliseconds()), TicksMissed: r.TicksMissed}, nil
}
