package engine

import (
	"encoding/binary"
	"fmt"

	"github.com/davifmn/matching-engine/internal/book"
)

// Kind of command.
type Kind uint8

const (
	KindSubmit Kind = iota + 1
	KindCancel
	KindModify
)

func (k Kind) String() string {
	switch k {
	case KindSubmit:
		return "submit"
	case KindCancel:
		return "cancel"
	case KindModify:
		return "modify"
	}
	return fmt.Sprintf("Kind(%d)", k)
}

// Command is what the journal stores: everything needed to replay the book exactly.
// The matching itself never reads the clock, so replaying the commands in order rebuilds
// the same book, queue positions included.
type Command struct {
	Kind  Kind
	Order book.Order   // KindSubmit; the engine assigns Order.ID
	ID    book.OrderID // KindCancel and KindModify
	Price int64        // KindModify
	Qty   int64        // KindModify
	Time  int64        // unix nanoseconds when the engine sequenced the command (audit only)
}

// Binary layout, version 1, little endian, 48 bytes:
//
//	0 version | 1 kind | 2 side | 3 type | 4 tif | 5 stp | 6 post-only | 7 reserved
//	8 order id | 16 owner | 24 price | 32 quantity | 40 time
const (
	commandVersion = 1
	commandSize    = 48
)

func (c *Command) appendBinary(dst []byte) []byte {
	id, price, qty := c.ID, c.Price, c.Qty
	if c.Kind == KindSubmit {
		id, price, qty = c.Order.ID, c.Order.Price, c.Order.Qty
	}
	var post byte
	if c.Order.PostOnly {
		post = 1
	}
	dst = append(dst, commandVersion, byte(c.Kind), byte(c.Order.Side), byte(c.Order.Type),
		byte(c.Order.TIF), byte(c.Order.STP), post, 0)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(id))
	dst = binary.LittleEndian.AppendUint64(dst, c.Order.Owner)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(price))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(qty))
	return binary.LittleEndian.AppendUint64(dst, uint64(c.Time))
}

func decodeCommand(p []byte) (Command, error) {
	if len(p) != commandSize {
		return Command{}, fmt.Errorf("command of %d bytes, want %d", len(p), commandSize)
	}
	if p[0] != commandVersion {
		return Command{}, fmt.Errorf("command version %d not supported", p[0])
	}
	u := func(off int) uint64 { return binary.LittleEndian.Uint64(p[off:]) }
	id, owner, price, qty := book.OrderID(u(8)), u(16), int64(u(24)), int64(u(32))
	c := Command{Kind: Kind(p[1]), Time: int64(u(40))}
	switch c.Kind {
	case KindSubmit:
		c.Order = book.Order{
			ID: id, Owner: owner, Side: book.Side(p[2]), Type: book.Type(p[3]), TIF: book.TimeInForce(p[4]),
			STP: book.STP(p[5]), PostOnly: p[6] == 1, Price: price, Qty: qty,
		}
	case KindCancel:
		c.ID = id
	case KindModify:
		c.ID, c.Price, c.Qty = id, price, qty
	default:
		return Command{}, fmt.Errorf("unknown command kind %d", p[1])
	}
	return c, nil
}
