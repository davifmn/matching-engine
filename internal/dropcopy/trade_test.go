package dropcopy

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/davifmn/matching-engine/internal/book"
	"github.com/davifmn/matching-engine/internal/engine"
	"github.com/davifmn/matching-engine/internal/instrument"
)

var sap = instrument.Spec{
	Symbol:   "SAP",
	TickSize: instrument.MustDecimal("0.01"),
	LotSize:  instrument.MustDecimal("1"),
	Currency: "EUR",
}

var at = time.Date(2026, 11, 8, 10, 15, 30, 123456789, time.UTC).UnixNano()

func tradeEvent(takerSide book.Side, price, qty int64, taker, maker uint64) book.Event {
	return book.Event{Kind: book.EventTrade, Side: takerSide, Price: price, Qty: qty, Owner: taker, MakerOwner: maker}
}

// The golden file is the contract with settlement-ledger, which keeps an identical copy
// (src/test/resources/contract/trade-executed.json) and checks that it settles it.
func TestTheMessageMatchesTheContract(t *testing.T) {
	trades, _ := Trades(sap, 42, engine.Command{Time: at}, []book.Event{
		{Kind: book.EventAccepted},
		tradeEvent(book.Buy, 13415, 300, 1001, 1002),
	})
	if len(trades) != 1 {
		t.Fatalf("got %d trades", len(trades))
	}
	got, err := json.Marshal(trades[0])
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/trade-executed.golden.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bytes.TrimSpace(want)) {
		t.Fatalf("message changed; if on purpose, update both copies of the contract\n got: %s\nwant: %s", got, want)
	}
}

func TestBuyerAndSellerFollowTheTakersSide(t *testing.T) {
	trades, _ := Trades(sap, 7, engine.Command{Time: at}, []book.Event{
		tradeEvent(book.Sell, 100, 1, 2001, 3001), // taker sold to the maker
	})
	if trades[0].BuyerAccount != "3001" || trades[0].SellerAccount != "2001" {
		t.Fatalf("buyer %s seller %s, want 3001 and 2001", trades[0].BuyerAccount, trades[0].SellerAccount)
	}
}

func TestIdsNumberTheTradesOfACommandAndSkipAnonymousOnes(t *testing.T) {
	trades, anonymous := Trades(sap, 9, engine.Command{Time: at}, []book.Event{
		{Kind: book.EventAccepted},
		tradeEvent(book.Buy, 100, 1, 1001, 1002),
		tradeEvent(book.Buy, 101, 1, 1001, 0), // the maker was anonymous
		tradeEvent(book.Buy, 102, 1, 1001, 1003),
		{Kind: book.EventRested},
	})
	if anonymous != 1 || len(trades) != 2 {
		t.Fatalf("%d trades, %d anonymous", len(trades), anonymous)
	}
	if trades[0].TradeID != "SAP-9-1" || trades[1].TradeID != "SAP-9-3" {
		t.Fatalf("ids %s, %s: the anonymous trade must keep its number", trades[0].TradeID, trades[1].TradeID)
	}
}
