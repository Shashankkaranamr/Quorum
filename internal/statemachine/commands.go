package statemachine

import (
	"google.golang.org/protobuf/proto"

	kvv1 "github.com/Shashankkaranamr/Quorum/gen/quorum/kv/v1"
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
