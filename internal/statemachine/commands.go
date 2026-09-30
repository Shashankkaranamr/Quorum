package statemachine

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/kv/v1"
	"github.com/Shashankkaranamr/Quorum/raft"
)

// The encoders below produce entry payloads. They live beside the decoder so the
// request a client sends and the command the state machine applies cannot drift
// apart: deduplication only works if the (client_id, seq) pair survives the
// round trip unchanged.

// EncodeRegister is the payload of an EntrySession.
func EncodeRegister(nonce []byte) []byte {
	return mustMarshal(&kvv1.RegisterClientCommand{Nonce: nonce})
}

// EncodePut is the payload of an EntryNormal that writes key.
func EncodePut(clientID, seq uint64, key string, value []byte) []byte {
	return mustMarshal(&kvv1.Command{
		ClientId: clientID,
		Seq:      seq,
		Op:       &kvv1.Command_Put{Put: &kvv1.PutOp{Key: key, Value: value}},
	})
}

// EncodeDelete is the payload of an EntryNormal that deletes key.
func EncodeDelete(clientID, seq uint64, key string) []byte {
	return mustMarshal(&kvv1.Command{
		ClientId: clientID,
		Seq:      seq,
		Op:       &kvv1.Command_Delete{Delete: &kvv1.DeleteOp{Key: key}},
	})
}

// mustMarshal panics on failure, which for these fixed, fully populated
// messages means a broken protobuf runtime rather than bad input.
func mustMarshal(m proto.Message) []byte {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic("statemachine: marshal " + string(m.ProtoReflect().Descriptor().FullName()) + ": " + err.Error())
	}
	return b
}

// Describe renders an entry in a line, for status displays and logs: what the
// entry does, not its bytes. It never fails; an undecodable payload is shown
// as its size.
func Describe(e raft.Entry) string {
	switch e.Type {
	case raft.EntryNoOp:
		return "no-op"
	case raft.EntrySession:
		return fmt.Sprintf("register client %d", e.Index)
	case raft.EntryNormal:
		var cmd kvv1.Command
		if err := proto.Unmarshal(e.Data, &cmd); err != nil {
			return fmt.Sprintf("%d undecodable bytes", len(e.Data))
		}
		who := fmt.Sprintf("(client %d seq %d)", cmd.GetClientId(), cmd.GetSeq())
		switch op := cmd.GetOp().(type) {
		case *kvv1.Command_Put:
			v := string(op.Put.GetValue())
			if len(v) > 24 {
				v = v[:24] + "..."
			}
			return fmt.Sprintf("put %s = %q %s", op.Put.GetKey(), v, who)
		case *kvv1.Command_Delete:
			return fmt.Sprintf("delete %s %s", op.Delete.GetKey(), who)
		}
		return "empty command " + who
	default:
		return e.Type.String()
	}
}
