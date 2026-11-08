// Package book is a limit order book for one instrument, with price-time priority.
//
// A Book is not safe for concurrent use. The engine gives each instrument its own
// goroutine (the single-writer principle, docs/adr/0001-single-writer.md), so the
// matching path takes no locks.
//
// Prices are integer ticks and quantities integer lots (docs/adr/0003-fixed-point.md).
// Converting decimals from the API into ticks and lots is the job of package instrument.
package book

import "fmt"

// OrderID identifies an order. The engine assigns IDs; 0 is never a valid ID.
type OrderID uint64

// Side of an order.
type Side uint8

const (
	Buy Side = iota + 1
	Sell
)

func (s Side) String() string {
	switch s {
	case Buy:
		return "buy"
	case Sell:
		return "sell"
	}
	return fmt.Sprintf("Side(%d)", s)
}

// Opposite returns the side an order of side s trades against.
func (s Side) Opposite() Side {
	if s == Buy {
		return Sell
	}
	return Buy
}

// Type of an order.
type Type uint8

const (
	Limit  Type = iota + 1 // trades at its price or better
	Market                 // trades at any price; never rests on the book
)

func (t Type) String() string {
	switch t {
	case Limit:
		return "limit"
	case Market:
		return "market"
	}
	return fmt.Sprintf("Type(%d)", t)
}

// TimeInForce says what happens to the part of an order that does not trade at once.
type TimeInForce uint8

const (
	GTC TimeInForce = iota + 1 // good till canceled: the remainder rests on the book
	IOC                        // immediate or cancel: the remainder is canceled
	FOK                        // fill or kill: the order fills completely at once, or is rejected
)

func (t TimeInForce) String() string {
	switch t {
	case GTC:
		return "gtc"
	case IOC:
		return "ioc"
	case FOK:
		return "fok"
	}
	return fmt.Sprintf("TimeInForce(%d)", t)
}

// STP is the self-trade prevention mode of an incoming order. Two orders with the same
// non-zero Owner never trade with each other; the mode of the incoming (taker) order
// decides which one is canceled.
type STP uint8

const (
	CancelNewest STP = iota // the default: cancel the rest of the incoming order
	CancelOldest            // cancel the resting order and keep matching
	CancelBoth              // cancel both
)

func (s STP) String() string {
	switch s {
	case CancelNewest:
		return "cancel_newest"
	case CancelOldest:
		return "cancel_oldest"
	case CancelBoth:
		return "cancel_both"
	}
	return fmt.Sprintf("STP(%d)", s)
}

// Order is a new order sent to Book.Submit.
type Order struct {
	ID       OrderID
	Owner    uint64 // account; 0 means anonymous, and STP does not apply
	Side     Side
	Type     Type
	TIF      TimeInForce
	Price    int64 // limit price in ticks; ignored for market orders
	Qty      int64 // quantity in lots
	PostOnly bool  // GTC limit only: reject the order instead of letting it take liquidity
	STP      STP
}

// OrderInfo describes an order resting on the book.
type OrderInfo struct {
	ID       OrderID
	Owner    uint64
	Side     Side
	Price    int64
	Qty      int64 // open (leaves) quantity
	PostOnly bool
	STP      STP
}

// PriceLevel is the aggregated quantity at one price.
type PriceLevel struct {
	Price  int64
	Qty    int64
	Orders int
}

// LevelChange is the new total quantity at a price after an operation. Qty 0 means the
// level is gone. Applying the changes in order to the previous depth gives the new depth,
// which is what incremental market data needs.
type LevelChange struct {
	Side  Side
	Price int64
	Qty   int64
}

// EventKind is the type of an Event.
type EventKind uint8

const (
	EventAccepted EventKind = iota + 1 // a new order passed validation
	EventRejected                      // a request was refused; nothing changed
	EventTrade                         // the incoming order traded with a resting one
	EventRested                        // the remainder of the order now rests on the book
	EventCanceled                      // an order, or its remainder, was canceled
	EventModified                      // a resting order got a new price or quantity
)

func (k EventKind) String() string {
	switch k {
	case EventAccepted:
		return "accepted"
	case EventRejected:
		return "rejected"
	case EventTrade:
		return "trade"
	case EventRested:
		return "rested"
	case EventCanceled:
		return "canceled"
	case EventModified:
		return "modified"
	}
	return fmt.Sprintf("EventKind(%d)", k)
}

// Reason explains a rejection or a cancellation.
type Reason uint8

const (
	ReasonNone Reason = iota
	ReasonInvalidID
	ReasonInvalidSide
	ReasonInvalidType
	ReasonInvalidTIF // includes market GTC orders and post-only orders that are not GTC limit
	ReasonInvalidSTP
	ReasonInvalidPrice
	ReasonInvalidQty
	ReasonDuplicateID
	ReasonUnknownOrder
	ReasonPostOnlyWouldCross
	ReasonFOKNotFilled
	ReasonNoLiquidity // the remainder of an IOC or market order found nothing more to trade with
	ReasonSelfTrade
	ReasonCanceledByUser
)

var reasonNames = [...]string{
	ReasonNone:               "none",
	ReasonInvalidID:          "invalid_id",
	ReasonInvalidSide:        "invalid_side",
	ReasonInvalidType:        "invalid_type",
	ReasonInvalidTIF:         "invalid_time_in_force",
	ReasonInvalidSTP:         "invalid_self_trade_prevention",
	ReasonInvalidPrice:       "invalid_price",
	ReasonInvalidQty:         "invalid_quantity",
	ReasonDuplicateID:        "duplicate_order_id",
	ReasonUnknownOrder:       "unknown_order",
	ReasonPostOnlyWouldCross: "post_only_would_cross",
	ReasonFOKNotFilled:       "fill_or_kill_not_filled",
	ReasonNoLiquidity:        "no_liquidity",
	ReasonSelfTrade:          "self_trade_prevented",
	ReasonCanceledByUser:     "canceled_by_user",
}

func (r Reason) String() string {
	if int(r) < len(reasonNames) {
		return reasonNames[r]
	}
	return fmt.Sprintf("Reason(%d)", r)
}

// Event reports one thing that happened during Submit, Cancel or Modify.
//
// For a trade, Order/Side/Leaves/Owner describe the incoming (taker) order and
// Maker/MakerLeaves/MakerOwner the resting one; Price is the maker's price.
type Event struct {
	Kind        EventKind
	Reason      Reason  // Rejected and Canceled only
	Order       OrderID // the order the event is about; the taker, for trades
	Side        Side
	Price       int64 // limit, trade, resting or new price; 0 for market orders
	Qty         int64 // order, traded, rested, canceled or new quantity
	Leaves      int64 // open quantity of Order after the event
	Maker       OrderID
	MakerLeaves int64
	// Owner and MakerOwner are the accounts of the two sides of a trade (0 = anonymous).
	// The book reports them so that whoever settles the trade, a drop copy, needs no
	// order history of its own.
	Owner      uint64
	MakerOwner uint64
}

func (e Event) String() string {
	switch e.Kind {
	case EventTrade:
		return fmt.Sprintf("trade taker=%d %s %d@%d leaves=%d maker=%d makerLeaves=%d accounts=%d/%d",
			e.Order, e.Side, e.Qty, e.Price, e.Leaves, e.Maker, e.MakerLeaves, e.Owner, e.MakerOwner)
	case EventRejected, EventCanceled:
		return fmt.Sprintf("%s id=%d %s %d@%d leaves=%d reason=%s",
			e.Kind, e.Order, e.Side, e.Qty, e.Price, e.Leaves, e.Reason)
	}
	return fmt.Sprintf("%s id=%d %s %d@%d leaves=%d", e.Kind, e.Order, e.Side, e.Qty, e.Price, e.Leaves)
}

// validate checks an order on its own, without looking at the book.
func validate(o *Order) Reason {
	switch {
	case o.ID == 0:
		return ReasonInvalidID
	case o.Side != Buy && o.Side != Sell:
		return ReasonInvalidSide
	case o.Type != Limit && o.Type != Market:
		return ReasonInvalidType
	case o.TIF != GTC && o.TIF != IOC && o.TIF != FOK:
		return ReasonInvalidTIF
	case o.Type == Market && o.TIF == GTC:
		return ReasonInvalidTIF
	case o.PostOnly && (o.Type != Limit || o.TIF != GTC):
		return ReasonInvalidTIF
	case o.STP > CancelBoth:
		return ReasonInvalidSTP
	case o.Type == Limit && o.Price <= 0:
		return ReasonInvalidPrice
	case o.Qty <= 0:
		return ReasonInvalidQty
	}
	return ReasonNone
}

// acceptable reports whether an order of side s with limit price limit may trade at price.
func acceptable(s Side, limit, price int64) bool {
	if s == Buy {
		return price <= limit
	}
	return price >= limit
}
