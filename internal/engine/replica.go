package engine

import "github.com/davifmn/matching-engine/internal/book"

// Replica rebuilds one instrument's book from its journal records. It decodes and applies
// each command with the same code as the engine, so it produces exactly the engine's
// events, with the same sequence numbers. A drop copy uses it to follow a running engine
// from its journal (docs/adr/0005-drop-copy.md).
//
// A Replica is not safe for concurrent use.
type Replica struct {
	book *book.Book
	seq  uint64
}

// NewReplica returns a replica of an empty book, before the first journal record.
func NewReplica() *Replica { return &Replica{book: book.New()} }

// Apply decodes the next journal record and applies it. It returns the command's sequence
// number (1 for the first record, as in Result.Seq), the command, and its events, which
// are only valid until the next call.
func (r *Replica) Apply(payload []byte) (seq uint64, c Command, events []book.Event, err error) {
	c, err = decodeCommand(payload)
	if err != nil {
		return 0, Command{}, nil, err
	}
	r.seq++
	return r.seq, c, apply(r.book, &c), nil
}

// Seq is the number of commands applied so far.
func (r *Replica) Seq() uint64 { return r.seq }

// Digest hashes the replica's book; it equals the engine's Digest at the same Seq.
func (r *Replica) Digest() uint64 { return r.book.Digest() }
