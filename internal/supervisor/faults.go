package supervisor

import (
	"context"
	"fmt"
	"time"

	adminv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/admin/v1"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// The faults below go through each node's admin API. They live here, beside
// the process faults, so that quorumctl, the visualizer and the integration
// tests inject faults through exactly one implementation.

func (s *Supervisor) withAdmin(id raft.NodeID, fn func(adminv1.AdminClient) error) error {
	a, conn, err := s.Admin(id)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	return fn(a)
}

// Partition cuts the links between groups a and b. Both ends of every link are
// told, so the cut holds even if one side already has a stream open: the
// sender stops sending and the receiver refuses what arrives. With oneway,
// only the links from a to b are cut.
func (s *Supervisor) Partition(ctx context.Context, a, b []raft.NodeID, oneway bool) error {
	block := func(on raft.NodeID, peers []raft.NodeID, req *adminv1.BlockLinksRequest) error {
		for _, p := range peers {
			req.PeerIds = append(req.PeerIds, uint64(p))
		}
		return s.withAdmin(on, func(adm adminv1.AdminClient) error {
			_, err := adm.BlockLinks(ctx, req)
			return err
		})
	}
	for _, id := range a {
		if err := block(id, b, &adminv1.BlockLinksRequest{OutboundOnly: oneway}); err != nil {
			return fmt.Errorf("supervisor: partition: node %d: %w", id, err)
		}
	}
	for _, id := range b {
		if err := block(id, a, &adminv1.BlockLinksRequest{InboundOnly: oneway}); err != nil {
			return fmt.Errorf("supervisor: partition: node %d: %w", id, err)
		}
	}
	return nil
}

// Heal removes every injected link fault on every running node.
func (s *Supervisor) Heal(ctx context.Context) error {
	for _, id := range s.IDs() {
		if _, ok := s.PID(id); !ok {
			continue
		}
		err := s.withAdmin(id, func(a adminv1.AdminClient) error {
			_, err := a.Heal(ctx, &adminv1.HealRequest{})
			return err
		})
		if err != nil {
			return fmt.Errorf("supervisor: heal node %d: %w", id, err)
		}
	}
	return nil
}

// Freeze parks a node's event loop, thawing it automatically after autoThaw if
// that is non-zero. Cooperative, not SIGSTOP; see server.Loop.Freeze.
func (s *Supervisor) Freeze(ctx context.Context, id raft.NodeID, autoThaw time.Duration) error {
	return s.withAdmin(id, func(a adminv1.AdminClient) error {
		_, err := a.Freeze(ctx, &adminv1.FreezeRequest{AutoThawAfterMs: uint32(autoThaw.Milliseconds())})
		return err
	})
}

// Thaw resumes a frozen node and reports what the freeze cost it.
func (s *Supervisor) Thaw(ctx context.Context, id raft.NodeID) (*adminv1.ThawResponse, error) {
	var r *adminv1.ThawResponse
	err := s.withAdmin(id, func(a adminv1.AdminClient) error {
		var err error
		r, err = a.Thaw(ctx, &adminv1.ThawRequest{})
		return err
	})
	return r, err
}
