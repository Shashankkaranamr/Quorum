package kvservice

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	kvv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/kv/v1"
	"github.com/Shashankkaranamr/Quorum/internal/server"
	"github.com/Shashankkaranamr/Quorum/internal/statemachine"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// DefaultRequestTimeout bounds how long one request waits to commit, or to be
// confirmed, before the service answers TIMEOUT or NO_QUORUM.
const DefaultRequestTimeout = 2 * time.Second

// Hooks are fault-injection points for tests. Leave them nil anywhere else.
type Hooks struct {
	// BeforeRespond runs after a write has been applied and before its
	// response is returned. Returning an error drops the response and fails
	// the RPC instead -- exactly the ambiguous failure the session table
	// exists for: the write happened, and the client was not told.
	BeforeRespond func(method string, index raft.Index) error
}

// Service implements the client-facing KV API over one node's driver loop.
type Service struct {
	kvv1.UnimplementedKVServer

	loop    *server.Loop
	kv      *statemachine.KV
	addrs   map[raft.NodeID]string
	timeout time.Duration
	hooks   Hooks
}

// New creates the service. kv must be the state machine loop applies to; the
// service reads it only from inside loop.Read, on the loop goroutine. addrs
// maps node ids to the gRPC addresses a client should be redirected to.
func New(loop *server.Loop, kv *statemachine.KV, addrs map[raft.NodeID]string,
	timeout time.Duration, hooks Hooks) *Service {
	if timeout <= 0 {
		timeout = DefaultRequestTimeout
	}
	return &Service{loop: loop, kv: kv, addrs: addrs, timeout: timeout, hooks: hooks}
}

// Register adds the KV service to a gRPC server.
func (s *Service) Register(r grpc.ServiceRegistrar) { kvv1.RegisterKVServer(r, s) }

func (s *Service) hint(id raft.NodeID) *kvv1.LeaderHint {
	if id == raft.None {
		return nil
	}
	return &kvv1.LeaderHint{NodeId: uint64(id), GrpcAddr: s.addrs[id]}
}

// writeOutcome maps a proposal's error onto the response contract (DESIGN.md
// §4). The one distinction that matters most: NOT_LEADER means nothing was
// proposed, while LOST_LEADERSHIP and TIMEOUT mean the write may or may not
// have happened, so the client must retry with the SAME seq.
func (s *Service) writeOutcome(err error) (kvv1.Status, *kvv1.LeaderHint, error) {
	var nl server.NotLeaderError
	switch {
	case errors.As(err, &nl):
		return kvv1.Status_STATUS_NOT_LEADER, s.hint(nl.Leader), nil
	case errors.Is(err, server.ErrLostLeadership):
		return kvv1.Status_STATUS_LOST_LEADERSHIP, nil, nil
	case errors.Is(err, context.DeadlineExceeded):
		return kvv1.Status_STATUS_TIMEOUT, nil, nil
	case errors.Is(err, context.Canceled):
		return 0, nil, status.FromContextError(err).Err()
	default:
		return 0, nil, status.Errorf(codes.Unavailable, "kvservice: %v", err)
	}
}

// apply proposes one entry and returns the state machine's result for it.
func (s *Service) apply(ctx context.Context, method string, typ raft.EntryType, data []byte) (
	statemachine.Result, kvv1.Status, *kvv1.LeaderHint, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	a, err := s.loop.Propose(ctx, typ, data)
	if err != nil {
		st, hint, rpcErr := s.writeOutcome(err)
		return statemachine.Result{}, st, hint, rpcErr
	}
	res, _ := a.Result.(statemachine.Result)
	if s.hooks.BeforeRespond != nil {
		if err := s.hooks.BeforeRespond(method, a.Index); err != nil {
			return statemachine.Result{}, 0, nil, status.Errorf(codes.Unavailable, "kvservice: %v", err)
		}
	}
	return res, res.Status, nil, nil
}

// RegisterClient implements kvv1.KVServer. The client id is the index the
// registration committed at.
func (s *Service) RegisterClient(ctx context.Context, req *kvv1.RegisterClientRequest) (*kvv1.RegisterClientResponse, error) {
	res, st, hint, err := s.apply(ctx, "RegisterClient", raft.EntrySession, statemachine.EncodeRegister(req.GetNonce()))
	if err != nil {
		return nil, err
	}
	return &kvv1.RegisterClientResponse{Status: st, ClientId: res.ClientID, LeaderHint: hint}, nil
}

// Put implements kvv1.KVServer.
func (s *Service) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	data := statemachine.EncodePut(req.GetClientId(), req.GetSeq(), req.GetKey(), req.GetValue())
	res, st, hint, err := s.apply(ctx, "Put", raft.EntryNormal, data)
	if err != nil {
		return nil, err
	}
	return &kvv1.PutResponse{Status: st, AppliedIndex: uint64(res.AppliedIndex),
		Duplicate: res.Duplicate, LeaderHint: hint}, nil
}

// Delete implements kvv1.KVServer.
func (s *Service) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	data := statemachine.EncodeDelete(req.GetClientId(), req.GetSeq(), req.GetKey())
	res, st, hint, err := s.apply(ctx, "Delete", raft.EntryNormal, data)
	if err != nil {
		return nil, err
	}
	return &kvv1.DeleteResponse{Status: st, AppliedIndex: uint64(res.AppliedIndex),
		Duplicate: res.Duplicate, Existed: res.Existed, LeaderHint: hint}, nil
}

type getResult struct {
	value []byte
	found bool
}

// Get implements kvv1.KVServer with ReadIndex: the value is read from the
// local state machine only after a quorum has confirmed this node still leads
// and the state machine has caught up to the read index. A leader that cannot
// confirm answers NO_QUORUM; it never answers from memory alone.
func (s *Service) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	key := req.GetKey()
	idx, v, err := s.loop.Read(ctx, func() any {
		val, ok := s.kv.Get(key)
		return getResult{value: append([]byte(nil), val...), found: ok}
	})
	var nl server.NotLeaderError
	switch {
	case err == nil:
		r := v.(getResult)
		return &kvv1.GetResponse{Status: kvv1.Status_STATUS_OK, Found: r.found, Value: r.value,
			ReadIndex: uint64(idx)}, nil
	case errors.As(err, &nl):
		return &kvv1.GetResponse{Status: kvv1.Status_STATUS_NOT_LEADER, LeaderHint: s.hint(nl.Leader)}, nil
	case errors.Is(err, server.ErrReadNotConfirmed), errors.Is(err, context.DeadlineExceeded):
		return &kvv1.GetResponse{Status: kvv1.Status_STATUS_NO_QUORUM}, nil
	case errors.Is(err, context.Canceled):
		return nil, status.FromContextError(err).Err()
	default:
		return nil, status.Errorf(codes.Unavailable, "kvservice: %v", err)
	}
}
