// Package server is the driver: the single goroutine that owns a raft.Node and
// the only place in the system where I/O ordering is decided.
//
// The loop:
//
//	select {
//	case <-ticker.C:       n.Tick()
//	case m := <-recvCh:    n.Step(m)
//	case p := <-proposeCh: n.Propose(p)
//	}
//	rd := n.Ready()
//	storage.Append(rd.Entries)            // 1. buffered
//	storage.SetHardState(rd.HardState)    // 2. buffered
//	storage.Sync()                        // 3. the fsync, one per Ready batch
//	transport.Send(rd.Messages)           // 4. never before (3) returns
//	sm.Apply(rd.CommittedEntries)         // 5. never before (3) returns
//	n.Advance()                           // 6.
//
// Steps 1 through 6 are extracted into a function shared by the real driver and
// the deterministic simulator, so the ordering itself is covered by the fast
// tests rather than only by the end-to-end suite.
//
// The ordering is a correctness requirement, not a performance choice: no
// message may leave the process before Sync returns, because a granted vote and
// an accepted AppendEntries are both durable promises. etcd relaxes this for
// follower appends as a throughput optimization. Quorum does not, and documents
// the cost instead of taking the shortcut. Phase 3 adds a storage test double
// that fails the test if a Send is observed before its corresponding Sync.
//
// Known hazard, stated up front rather than discovered later: because one
// goroutine handles ticks, messages and durability, a slow fsync delays ticks
// and can stall elections. That is the same symptom as the classic "held a lock
// across I/O" bug, relocated into a single auditable loop where it can be
// measured. Tick lag and fsync latency are therefore exported as metrics from
// the first day the loop exists.
//
// Phase 2 fills this package in. It is currently a documented stub.
package server
