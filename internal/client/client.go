package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kvv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/kv/v1"
	"github.com/Shashankkaranamr/Quorum/raft"
)

var (
	// ErrUnknownOutcome means a write was sent, at least one attempt failed in
	// a way that leaves its fate unknown, and the context expired before a
	// retry could settle it. The write may or may not have been applied --
	// but it was applied at most once, and a later write from this client can
	// never be overtaken by it.
	ErrUnknownOutcome = errors.New("client: outcome unknown; the write may or may not have been applied")

	// ErrSessionExpired means the cluster has no session for this client, so
	// the write was not applied. Register again to continue.
	ErrSessionExpired = errors.New("client: session expired; the write was not applied")
)

// Config configures a Client.
type Config struct {
	// Addrs maps every node to its gRPC address.
	Addrs map[raft.NodeID]string

	// AttemptTimeout bounds one RPC. Zero means 3 seconds, a little above the
	// server's own request timeout so the server's answer normally arrives
	// first.
	AttemptTimeout time.Duration

	// Backoff is the pause before retrying the same or the next node after a
	// failure that did not come with a leader hint. Zero means 25ms.
	Backoff time.Duration

	// InitialLeader is where the first request goes. Zero means the lowest
	// node id. A wrong guess costs one redirect.
	InitialLeader raft.NodeID
}

// Client talks to a Quorum cluster on behalf of one session.
//
// A Client is not safe for concurrent use: a session carries one request at a
// time, because the server's response cache holds one entry per client
// (DESIGN.md §4). Concurrent callers each need their own Client.
type Client struct {
	cfg   Config
	ids   []raft.NodeID
	conns map[raft.NodeID]*grpc.ClientConn
	kvs   map[raft.NodeID]kvv1.KVClient

	leader raft.NodeID
	id     uint64
	seq    uint64

	attempts int
}

// Dial creates a client. Connections are made lazily.
func Dial(cfg Config) (*Client, error) {
	if len(cfg.Addrs) == 0 {
		return nil, errors.New("client: no node addresses")
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = 3 * time.Second
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = 25 * time.Millisecond
	}
	c := &Client{cfg: cfg, conns: map[raft.NodeID]*grpc.ClientConn{}, kvs: map[raft.NodeID]kvv1.KVClient{}}
	for id, addr := range cfg.Addrs {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			_ = c.Close()
			return nil, fmt.Errorf("client: node %d at %s: %w", id, addr, err)
		}
		c.ids = append(c.ids, id)
		c.conns[id] = conn
		c.kvs[id] = kvv1.NewKVClient(conn)
	}
	slices.Sort(c.ids)
	c.leader = c.ids[0]
	if c.kvs[cfg.InitialLeader] != nil {
		c.leader = cfg.InitialLeader
	}
	return c, nil
}

// Close releases the connections.
func (c *Client) Close() error {
	var errs []error
	for _, conn := range c.conns {
		errs = append(errs, conn.Close())
	}
	return errors.Join(errs...)
}

// ID is the session's client id, zero until Register succeeds.
func (c *Client) ID() uint64 { return c.id }

// Leader is the node the client currently believes leads.
func (c *Client) Leader() raft.NodeID { return c.leader }

// LastAttempts is how many RPCs the most recent call made, including the one
// that succeeded. In a stable cluster it is 1, or 2 after a redirect.
func (c *Client) LastAttempts() int { return c.attempts }

// outcome classifies one attempt.
type outcome int

const (
	done       outcome = iota // a definitive answer
	redirect                  // NOT_LEADER: definitely not applied; go where the hint says
	ambiguous                 // may have been applied; retry with the same seq
	unanswered                // not applied (reads), or nothing learned; try elsewhere
)

func classify(st kvv1.Status) outcome {
	switch st {
	case kvv1.Status_STATUS_OK, kvv1.Status_STATUS_SESSION_EXPIRED:
		return done
	case kvv1.Status_STATUS_NOT_LEADER:
		return redirect
	case kvv1.Status_STATUS_LOST_LEADERSHIP, kvv1.Status_STATUS_TIMEOUT:
		return ambiguous
	default:
		return unanswered
	}
}

// do runs attempt against the believed leader until it gets a definitive
// answer or ctx ends, following leader hints and rotating through the nodes
// when there is none. A failed RPC -- the node died, the connection dropped --
// counts as ambiguous: the request may have reached the leader and committed
// before the reply was lost.
func (c *Client) do(ctx context.Context, write bool,
	attempt func(ctx context.Context, kv kvv1.KVClient) (kvv1.Status, *kvv1.LeaderHint, error)) (kvv1.Status, error) {
	c.attempts = 0
	maybeApplied := false
	target := c.leader
	for {
		c.attempts++
		actx, cancel := context.WithTimeout(ctx, c.cfg.AttemptTimeout)
		st, hint, err := attempt(actx, c.kvs[target])
		cancel()

		o := ambiguous
		if err == nil {
			o = classify(st)
		}
		switch o {
		case done:
			c.leader = target
			return st, nil
		case redirect:
			if h := raft.NodeID(hint.GetNodeId()); h != raft.None && h != target && c.kvs[h] != nil {
				// A hint to a different node costs no backoff: that is
				// what makes a redirect one extra round trip.
				target = h
				c.leader = h
				continue
			}
		case ambiguous:
			maybeApplied = maybeApplied || write
		}

		target = c.next(target)
		select {
		case <-ctx.Done():
			if maybeApplied {
				return 0, ErrUnknownOutcome
			}
			return 0, ctx.Err()
		case <-time.After(c.cfg.Backoff):
		}
	}
}

func (c *Client) next(id raft.NodeID) raft.NodeID {
	i := slices.Index(c.ids, id)
	return c.ids[(i+1)%len(c.ids)]
}

// Register obtains a session. Registration itself is not deduplicated: a retry
// after an ambiguous failure may register a second session, which costs one
// log entry and is otherwise harmless.
func (c *Client) Register(ctx context.Context) error {
	var id uint64
	_, err := c.do(ctx, false, func(ctx context.Context, kv kvv1.KVClient) (kvv1.Status, *kvv1.LeaderHint, error) {
		r, err := kv.RegisterClient(ctx, &kvv1.RegisterClientRequest{})
		id = r.GetClientId()
		return r.GetStatus(), r.GetLeaderHint(), err
	})
	if err != nil {
		return fmt.Errorf("client: register: %w", err)
	}
	c.id, c.seq = id, 0
	return nil
}

// PutResult is a write's outcome.
type PutResult struct {
	AppliedIndex raft.Index

	// Duplicate is true when a retry of this very request found it already
	// applied, and the answer came from the session cache.
	Duplicate bool
}

// Put writes key. Every retry of this call carries the same seq, so the write
// is applied at most once however many attempts it takes.
func (c *Client) Put(ctx context.Context, key string, value []byte) (PutResult, error) {
	if c.id == 0 {
		return PutResult{}, errors.New("client: not registered")
	}
	c.seq++
	req := &kvv1.PutRequest{ClientId: c.id, Seq: c.seq, Key: key, Value: value}
	var resp *kvv1.PutResponse
	st, err := c.do(ctx, true, func(ctx context.Context, kv kvv1.KVClient) (kvv1.Status, *kvv1.LeaderHint, error) {
		r, err := kv.Put(ctx, req)
		resp = r
		return r.GetStatus(), r.GetLeaderHint(), err
	})
	if err != nil {
		return PutResult{}, err
	}
	if st == kvv1.Status_STATUS_SESSION_EXPIRED {
		return PutResult{}, ErrSessionExpired
	}
	return PutResult{AppliedIndex: raft.Index(resp.GetAppliedIndex()), Duplicate: resp.GetDuplicate()}, nil
}

// DeleteResult is a delete's outcome.
type DeleteResult struct {
	AppliedIndex raft.Index
	Duplicate    bool
	Existed      bool
}

// Delete removes key, with the same at-most-once guarantee as Put.
func (c *Client) Delete(ctx context.Context, key string) (DeleteResult, error) {
	if c.id == 0 {
		return DeleteResult{}, errors.New("client: not registered")
	}
	c.seq++
	req := &kvv1.DeleteRequest{ClientId: c.id, Seq: c.seq, Key: key}
	var resp *kvv1.DeleteResponse
	st, err := c.do(ctx, true, func(ctx context.Context, kv kvv1.KVClient) (kvv1.Status, *kvv1.LeaderHint, error) {
		r, err := kv.Delete(ctx, req)
		resp = r
		return r.GetStatus(), r.GetLeaderHint(), err
	})
	if err != nil {
		return DeleteResult{}, err
	}
	if st == kvv1.Status_STATUS_SESSION_EXPIRED {
		return DeleteResult{}, ErrSessionExpired
	}
	return DeleteResult{AppliedIndex: raft.Index(resp.GetAppliedIndex()), Duplicate: resp.GetDuplicate(),
		Existed: resp.GetExisted()}, nil
}

// GetResult is a read's outcome.
type GetResult struct {
	Found bool
	Value []byte

	// ReadIndex is the log index the read was linearized at.
	ReadIndex raft.Index
}

// Get reads key linearizably. Reads have no effect, so any failure is simply
// retried; a read never returns a value its leader could not confirm.
func (c *Client) Get(ctx context.Context, key string) (GetResult, error) {
	var resp *kvv1.GetResponse
	_, err := c.do(ctx, false, func(ctx context.Context, kv kvv1.KVClient) (kvv1.Status, *kvv1.LeaderHint, error) {
		r, err := kv.Get(ctx, &kvv1.GetRequest{ClientId: c.id, Key: key})
		resp = r
		return r.GetStatus(), r.GetLeaderHint(), err
	})
	if err != nil {
		return GetResult{}, err
	}
	return GetResult{Found: resp.GetFound(), Value: resp.GetValue(), ReadIndex: raft.Index(resp.GetReadIndex())}, nil
}
