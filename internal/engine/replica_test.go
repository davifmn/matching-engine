package engine

import (
	"path/filepath"
	"slices"
	"testing"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/flow"
	"github.com/davifmn/matching-engine/internal/journal"
)

// A replica fed with the journal must see the very events the engine returned, in order,
// with the same sequence numbers, and end with the same book.
func TestReplicaSeesExactlyTheEnginesEvents(t *testing.T) {
	dir := t.TempDir()
	e := open(t, dir, "X")

	type step struct {
		seq    uint64
		events []book.Event
	}
	var want []step
	g := flow.New(7, 10_000, flow.DefaultMix)
	live := flow.NewLive()
	for i := range 3000 {
		op := g.Next(live.Len())
		var res Result
		var err error
		switch op.Kind {
		case flow.Passive, flow.Aggressive:
			o := limit(op.Side, op.Price, op.Qty)
			o.Owner = uint64(1000 + i%7) // several accounts, so self-trade prevention also happens
			res, err = e.Submit(ctx, "X", o)
		case flow.Market:
			res, err = e.Submit(ctx, "X", book.Order{Owner: 2000, Side: op.Side, Type: book.Market, TIF: book.IOC, Qty: op.Qty})
		case flow.Cancel:
			res, err = e.Cancel(ctx, "X", live.At(op.Pick))
		case flow.Modify:
			res, err = e.Modify(ctx, "X", live.At(op.Pick), g.Mid(), op.Qty+1)
		}
		if err != nil {
			t.Fatal(err)
		}
		live.Apply(res.Events)
		want = append(want, step{res.Seq, res.Events})
	}
	digest, err := e.Digest(ctx, "X")
	if err != nil {
		t.Fatal(err)
	}

	r := NewReplica()
	var got []step
	trades := 0
	// Read in small slices, as a follower does while the engine keeps writing.
	var offset int64
	for {
		next, err := journal.ReadFrom(filepath.Join(dir, "X.wal"), offset, 100, func(p []byte) error {
			seq, _, events, err := r.Apply(p)
			for _, ev := range events {
				if ev.Kind == book.EventTrade {
					trades++
				}
			}
			got = append(got, step{seq, slices.Clone(events)})
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if next == offset {
			break
		}
		offset = next
	}

	if len(got) != len(want) {
		t.Fatalf("replica applied %d commands, engine ran %d", len(got), len(want))
	}
	for i := range want {
		if got[i].seq != want[i].seq || !slices.Equal(got[i].events, want[i].events) {
			t.Fatalf("command %d differs:\nreplica seq %d %v\nengine  seq %d %v",
				i, got[i].seq, got[i].events, want[i].seq, want[i].events)
		}
	}
	if trades == 0 {
		t.Fatal("the flow produced no trades; the test proves nothing")
	}
	if r.Seq() != digest.Seq || r.Digest() != digest.Digest {
		t.Fatalf("replica at seq %d digest %x, engine at seq %d digest %x", r.Seq(), r.Digest(), digest.Seq, digest.Digest)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReplicaRejectsGarbage(t *testing.T) {
	if _, _, _, err := NewReplica().Apply([]byte("not a command")); err == nil {
		t.Fatal("expected a decoding error")
	}
}
