// Package instrument describes tradable instruments and converts, exactly, between the
// decimal strings of the API and the integer ticks and lots of the order book. There is no
// floating point anywhere on this path (docs/adr/0003-fixed-point.md).
package instrument

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
)

var (
	ErrSyntax      = errors.New("not a plain decimal number")
	ErrRange       = errors.New("number out of range")
	ErrNotMultiple = errors.New("not a multiple of the increment")
	ErrNotPositive = errors.New("must be greater than zero")
)

const (
	maxScale  = 9  // nine decimal places
	maxDigits = 18 // fits in an int64 with room to spare
)

var pow10 = [...]int64{1, 10, 100, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18}

// Decimal is an exact non-negative decimal: units × 10^-scale.
type Decimal struct {
	units int64
	scale uint8
}

// ParseDecimal accepts plain decimals such as "34.15", "0.0001" or "100": digits with at
// most one point, no sign, no exponent, at most 18 digits and 9 decimal places.
func ParseDecimal(s string) (Decimal, error) {
	intPart, frac, hasPoint := strings.Cut(s, ".")
	if intPart == "" || (hasPoint && frac == "") {
		return Decimal{}, fmt.Errorf("%q: %w", s, ErrSyntax)
	}
	digits := intPart + frac
	for _, c := range digits {
		if c < '0' || c > '9' {
			return Decimal{}, fmt.Errorf("%q: %w", s, ErrSyntax)
		}
	}
	digits = strings.TrimLeft(digits, "0")
	if len(frac) > maxScale || len(digits) > maxDigits {
		return Decimal{}, fmt.Errorf("%q: %w", s, ErrRange)
	}
	var units int64
	if digits != "" {
		var err error
		if units, err = strconv.ParseInt(digits, 10, 64); err != nil {
			return Decimal{}, fmt.Errorf("%q: %w", s, ErrRange)
		}
	}
	return Decimal{units: units, scale: uint8(len(frac))}, nil
}

// MustDecimal is ParseDecimal for constants; it panics on error.
func MustDecimal(s string) Decimal {
	d, err := ParseDecimal(s)
	if err != nil {
		panic(err)
	}
	return d
}

// String formats d with exactly its scale, e.g. "34.10".
func (d Decimal) String() string {
	s := strconv.FormatInt(d.units, 10)
	if d.scale == 0 {
		return s
	}
	if pad := int(d.scale) + 1 - len(s); pad > 0 {
		s = strings.Repeat("0", pad) + s
	}
	return s[:len(s)-int(d.scale)] + "." + s[len(s)-int(d.scale):]
}

// IsZero reports whether d is 0.
func (d Decimal) IsZero() bool { return d.units == 0 }

func (d Decimal) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

func (d *Decimal) UnmarshalText(b []byte) error {
	v, err := ParseDecimal(string(b))
	if err != nil {
		return err
	}
	*d = v
	return nil
}

func mulPow10(v int64, k uint8) (int64, error) {
	p := pow10[k]
	if v > math.MaxInt64/p {
		return 0, ErrRange
	}
	return v * p, nil
}

// steps returns v / step when it is a whole number.
func steps(v, step Decimal) (int64, error) {
	scale := max(v.scale, step.scale)
	a, err := mulPow10(v.units, scale-v.scale)
	if err != nil {
		return 0, err
	}
	t, err := mulPow10(step.units, scale-step.scale)
	if err != nil {
		return 0, err
	}
	if a%t != 0 {
		return 0, ErrNotMultiple
	}
	return a / t, nil
}

// Spec is an instrument's trading rules.
type Spec struct {
	Symbol      string  `json:"symbol"`
	Description string  `json:"description,omitempty"`
	TickSize    Decimal `json:"tick_size"` // price increment
	LotSize     Decimal `json:"lot_size"`  // quantity increment
	// Currency of the price (ISO 4217), for whoever settles the trades. Matching itself
	// does not use it, so it is optional.
	Currency string `json:"currency,omitempty"`
}

var (
	symbolRE   = regexp.MustCompile(`^[A-Z0-9][A-Z0-9._-]{0,19}$`)
	currencyRE = regexp.MustCompile(`^[A-Z]{3}$`)
)

// Validate checks the symbol (it also names the journal file, so it must be a safe file
// name) and the increments.
func (s Spec) Validate() error {
	if !symbolRE.MatchString(s.Symbol) {
		return fmt.Errorf("symbol %q: use 1-20 of A-Z 0-9 . _ -, starting with a letter or digit", s.Symbol)
	}
	if s.TickSize.IsZero() || s.LotSize.IsZero() {
		return fmt.Errorf("%s: tick_size and lot_size must be greater than zero", s.Symbol)
	}
	if s.Currency != "" && !currencyRE.MatchString(s.Currency) {
		return fmt.Errorf("%s: currency %q is not a three-letter ISO 4217 code", s.Symbol, s.Currency)
	}
	return nil
}

// Ticks converts a price such as "34.15" into ticks.
func (s Spec) Ticks(price string) (int64, error) { return s.toSteps(price, s.TickSize, "price") }

// Lots converts a quantity such as "300" into lots.
func (s Spec) Lots(qty string) (int64, error) { return s.toSteps(qty, s.LotSize, "quantity") }

func (s Spec) toSteps(value string, step Decimal, what string) (int64, error) {
	d, err := ParseDecimal(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", what, err)
	}
	n, err := steps(d, step)
	switch {
	case errors.Is(err, ErrNotMultiple):
		return 0, fmt.Errorf("%s %s: %w %s", what, value, err, step)
	case err != nil:
		return 0, fmt.Errorf("%s %s: %w", what, value, err)
	case n == 0:
		return 0, fmt.Errorf("%s: %w", what, ErrNotPositive)
	}
	return n, nil
}

// Price formats ticks as a decimal price.
func (s Spec) Price(ticks int64) string { return times(ticks, s.TickSize) }

// Qty formats lots as a decimal quantity.
func (s Spec) Qty(lots int64) string { return times(lots, s.LotSize) }

func times(n int64, step Decimal) string {
	if n < 0 || (step.units != 0 && n > math.MaxInt64/step.units) {
		return "overflow"
	}
	return Decimal{units: n * step.units, scale: step.scale}.String()
}

// Load reads a JSON array of specs and checks them.
func Load(path string) ([]Spec, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var specs []Spec
	if err := json.Unmarshal(raw, &specs); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("%s: no instruments", path)
	}
	seen := map[string]bool{}
	for _, s := range specs {
		if err := s.Validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if seen[s.Symbol] {
			return nil, fmt.Errorf("%s: duplicate symbol %s", path, s.Symbol)
		}
		seen[s.Symbol] = true
	}
	return specs, nil
}
