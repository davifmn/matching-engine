package instrument

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestParseDecimal(t *testing.T) {
	for _, c := range []struct {
		in   string
		want string
		err  error
	}{
		{"34.15", "34.15", nil},
		{"0.0001", "0.0001", nil},
		{"100", "100", nil},
		{"007.50", "7.50", nil},
		{"0", "0", nil},
		{"999999999999999999", "999999999999999999", nil},
		{"1.000000001", "1.000000001", nil},
		{"", "", ErrSyntax},
		{".5", "", ErrSyntax},
		{"5.", "", ErrSyntax},
		{"-1", "", ErrSyntax},
		{"+1", "", ErrSyntax},
		{"1e3", "", ErrSyntax},
		{"1.2.3", "", ErrSyntax},
		{" 1", "", ErrSyntax},
		{"1,5", "", ErrSyntax},
		{"0.0000000001", "", ErrRange},        // 10 decimal places
		{"1000000000000000000", "", ErrRange}, // 19 digits
	} {
		d, err := ParseDecimal(c.in)
		if !errors.Is(err, c.err) {
			t.Errorf("ParseDecimal(%q) error = %v, want %v", c.in, err, c.err)
			continue
		}
		if err == nil && d.String() != c.want {
			t.Errorf("ParseDecimal(%q) = %s, want %s", c.in, d, c.want)
		}
	}
}

func TestTicksAndLots(t *testing.T) {
	petr := Spec{Symbol: "PETR4", TickSize: MustDecimal("0.01"), LotSize: MustDecimal("100")}
	win := Spec{Symbol: "WINZ26", TickSize: MustDecimal("5"), LotSize: MustDecimal("1")}
	btc := Spec{Symbol: "BTC-USD", TickSize: MustDecimal("0.01"), LotSize: MustDecimal("0.0001")}

	cases := []struct {
		spec  Spec
		conv  func(Spec, string) (int64, error)
		in    string
		want  int64
		err   error
		label string
	}{
		{petr, Spec.Ticks, "34.15", 3415, nil, "price in cents"},
		{petr, Spec.Ticks, "34.1", 3410, nil, "fewer decimals than the tick"},
		{petr, Spec.Ticks, "34.155", 0, ErrNotMultiple, "finer than the tick"},
		{petr, Spec.Ticks, "0.00", 0, ErrNotPositive, "zero price"},
		{petr, Spec.Lots, "300", 3, nil, "round lots"},
		{petr, Spec.Lots, "350", 0, ErrNotMultiple, "odd lot"},
		{win, Spec.Ticks, "128435", 25687, nil, "index points, tick 5"},
		{win, Spec.Ticks, "128437", 0, ErrNotMultiple, "not a multiple of 5"},
		{btc, Spec.Lots, "0.5", 5000, nil, "fractional lots"},
		{btc, Spec.Lots, "0.00005", 0, ErrNotMultiple, "below the lot size"},
		{btc, Spec.Ticks, "abc", 0, ErrSyntax, "garbage"},
		{btc, Spec.Ticks, "999999999999999999", 0, ErrRange, "overflows at tick scale"},
	}
	for _, c := range cases {
		got, err := c.conv(c.spec, c.in)
		if !errors.Is(err, c.err) || got != c.want {
			t.Errorf("%s: %s(%q) = %d, %v; want %d, %v", c.label, c.spec.Symbol, c.in, got, err, c.want, c.err)
		}
	}

	if got := petr.Price(3410); got != "34.10" {
		t.Errorf("Price(3410) = %s", got)
	}
	if got := petr.Qty(3); got != "300" {
		t.Errorf("Qty(3) = %s", got)
	}
	if got := btc.Qty(5); got != "0.0005" {
		t.Errorf("Qty(5) = %s", got)
	}
	if got := win.Price(25687); got != "128435" {
		t.Errorf("Price(25687) = %s", got)
	}
}

func TestLoad(t *testing.T) {
	dir := t.TempDir()
	write := func(body string) string {
		p := filepath.Join(dir, "i.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	specs, err := Load(write(`[{"symbol":"PETR4","tick_size":"0.01","lot_size":"100"}]`))
	if err != nil || len(specs) != 1 || specs[0].TickSize.String() != "0.01" {
		t.Fatalf("Load = %+v, %v", specs, err)
	}
	for name, body := range map[string]string{
		"bad symbol": `[{"symbol":"../etc","tick_size":"0.01","lot_size":"1"}]`,
		"lowercase":  `[{"symbol":"petr4","tick_size":"0.01","lot_size":"1"}]`,
		"zero tick":  `[{"symbol":"A","tick_size":"0","lot_size":"1"}]`,
		"bad number": `[{"symbol":"A","tick_size":"-1","lot_size":"1"}]`,
		"duplicate":  `[{"symbol":"A","tick_size":"1","lot_size":"1"},{"symbol":"A","tick_size":"1","lot_size":"1"}]`,
		"empty":      `[]`,
	} {
		if _, err := Load(write(body)); err == nil {
			t.Errorf("%s: Load accepted %s", name, body)
		}
	}
}

func TestCurrencyIsOptionalButMustBeAnISOCode(t *testing.T) {
	base := Spec{Symbol: "SAP", TickSize: MustDecimal("0.01"), LotSize: MustDecimal("1")}
	for currency, ok := range map[string]bool{"": true, "EUR": true, "eur": false, "EURO": false, "E1R": false} {
		s := base
		s.Currency = currency
		if err := s.Validate(); (err == nil) != ok {
			t.Errorf("currency %q: Validate() = %v", currency, err)
		}
	}
}

func TestTheShippedConfigurationLoads(t *testing.T) {
	specs, err := Load("../../config/instruments.json")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range specs {
		if s.Currency == "" {
			t.Errorf("%s has no currency", s.Symbol)
		}
	}
}
