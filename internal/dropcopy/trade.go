// Package dropcopy publishes every trade of the engine, with the accounts of both sides,
// for settlement (docs/adr/0005-drop-copy.md).
//
// It does not run inside the engine. A Follower tails an instrument's journal, replays
// each command into its own copy of the book (engine.Replica) and turns the trade events
// into messages. The journal is the source of truth: every acknowledged command is in it,
// so the drop copy cannot miss a trade, and replaying it always yields the same trades
// with the same ids.
package dropcopy

import (
	"fmt"
	"strconv"
	"time"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/engine"
	"github.com/davifmn/matching-engine/internal/instrument"
)

// Trade is one message on the trades topic, in the format settlement-ledger consumes
// (its docs/integration-matching-engine.md). Prices and quantities are decimal strings,
// as in the engine's API, so no floating point is involved anywhere.
type Trade struct {
	TradeID       string `json:"trade_id"`
	Symbol        string `json:"symbol"`
	Price         string `json:"price"`
	Quantity      string `json:"quantity"`
	Currency      string `json:"currency"`
	BuyerAccount  string `json:"buyer_account"`
	SellerAccount string `json:"seller_account"`
	ExecutedAt    string `json:"executed_at"`
	Seq           uint64 `json:"seq"` // the command's position in the journal; consumers may ignore it
}

// Trades turns the events of one command into trade messages.
//
// The id of a trade is <symbol>-<seq>-<n>, where seq is the command's sequence number in
// the journal and n numbers the trades of that command from 1. Replaying the journal
// gives the same ids, so consumers can deduplicate on trade_id after a restart.
//
// A trade with an anonymous side (account 0) cannot be settled; it is counted, not
// published, and keeps its number so the ids of the other trades do not shift.
func Trades(spec instrument.Spec, seq uint64, c engine.Command, events []book.Event) (trades []Trade, anonymous int) {
	executedAt := time.Unix(0, c.Time).UTC().Format(time.RFC3339Nano)
	n := 0
	for _, e := range events {
		if e.Kind != book.EventTrade {
			continue
		}
		n++
		buyer, seller := e.Owner, e.MakerOwner // the taker bought
		if e.Side == book.Sell {
			buyer, seller = seller, buyer
		}
		if buyer == 0 || seller == 0 {
			anonymous++
			continue
		}
		trades = append(trades, Trade{
			TradeID:       fmt.Sprintf("%s-%d-%d", spec.Symbol, seq, n),
			Symbol:        spec.Symbol,
			Price:         spec.Price(e.Price),
			Quantity:      spec.Qty(e.Qty),
			Currency:      spec.Currency,
			BuyerAccount:  strconv.FormatUint(buyer, 10),
			SellerAccount: strconv.FormatUint(seller, 10),
			ExecutedAt:    executedAt,
			Seq:           seq,
		})
	}
	return trades, anonymous
}
