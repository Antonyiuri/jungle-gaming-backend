package domain

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"strconv"
)

// Money is an immutable amount in hundredths of its currency.
// Its zero Go value is invalid; use Zero to create a monetary zero.
type Money struct {
	minorUnits int64
	currency   string
}

func NewMoney(minor int64, currency string) (Money, error) {
	m := Money{minorUnits: minor, currency: currency}
	if err := m.Validate(); err != nil {
		return Money{}, err
	}
	return m, nil
}

func Zero(currency string) (Money, error) { return NewMoney(0, currency) }

// ParseMoney accepts unsigned canonical decimals with exactly two places.
// Signed internal values must be constructed with NewMoney.
func ParseMoney(amount, currency string) (Money, error) {
	if _, err := Zero(currency); err != nil {
		return Money{}, err
	}
	if len(amount) < 4 || amount[len(amount)-3] != '.' {
		return Money{}, fail("INVALID_MONEY", "amount must have exactly two decimal places")
	}
	if len(amount) > 4 && amount[0] == '0' {
		return Money{}, fail("INVALID_MONEY", "leading zeros are not allowed")
	}
	var minor int64
	for i := 0; i < len(amount); i++ {
		if i == len(amount)-3 {
			continue
		}
		if amount[i] < '0' || amount[i] > '9' {
			return Money{}, fail("INVALID_MONEY", "amount must contain unsigned decimal digits")
		}
		digit := int64(amount[i] - '0')
		if minor > (math.MaxInt64-digit)/10 {
			return Money{}, fail("INVALID_MONEY", "amount exceeds the supported range")
		}
		minor = minor*10 + digit
	}
	return NewMoney(minor, currency)
}

func (m Money) MinorUnits() int64 { return m.minorUnits }
func (m Money) Currency() string  { return m.currency }

func (m Money) Validate() error {
	switch m.currency {
	case "BRL", "USD", "EUR":
		return nil
	case "":
		return fail("INVALID_MONEY", "money must have a currency")
	default:
		return fail("UNSUPPORTED_CURRENCY", "currency is not supported")
	}
}

func (m Money) Amount() string {
	negative := m.minorUnits < 0
	var magnitude uint64
	if negative {
		magnitude = uint64(-(m.minorUnits + 1)) + 1
	} else {
		magnitude = uint64(m.minorUnits)
	}
	fraction := strconv.FormatUint(magnitude%100, 10)
	if len(fraction) == 1 {
		fraction = "0" + fraction
	}
	result := strconv.FormatUint(magnitude/100, 10) + "." + fraction
	if negative {
		return "-" + result
	}
	return result
}

func (m Money) compatible(other Money) error {
	if err := m.Validate(); err != nil {
		return err
	}
	if err := other.Validate(); err != nil {
		return err
	}
	if m.currency != other.currency {
		return fail("CURRENCY_MISMATCH", "money currencies must match")
	}
	return nil
}

func (m Money) Add(other Money) (Money, error) {
	if err := m.compatible(other); err != nil {
		return Money{}, err
	}
	a, b := m.minorUnits, other.minorUnits
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return Money{}, fail("MONEY_OVERFLOW", "addition exceeds the supported range")
	}
	return NewMoney(a+b, m.currency)
}

func (m Money) Subtract(other Money) (Money, error) {
	if err := m.compatible(other); err != nil {
		return Money{}, err
	}
	a, b := m.minorUnits, other.minorUnits
	if (b > 0 && a < math.MinInt64+b) || (b < 0 && a > math.MaxInt64+b) {
		return Money{}, fail("MONEY_OVERFLOW", "subtraction exceeds the supported range")
	}
	return NewMoney(a-b, m.currency)
}

func (m Money) Negate() (Money, error) {
	if err := m.Validate(); err != nil {
		return Money{}, err
	}
	if m.minorUnits == math.MinInt64 {
		return Money{}, fail("MONEY_OVERFLOW", "minimum amount cannot be negated")
	}
	return NewMoney(-m.minorUnits, m.currency)
}

func (m Money) Compare(other Money) (int, error) {
	if err := m.compatible(other); err != nil {
		return 0, err
	}
	if m.minorUnits < other.minorUnits {
		return -1, nil
	}
	if m.minorUnits > other.minorUnits {
		return 1, nil
	}
	return 0, nil
}

func (m Money) MarshalJSON() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	}{m.Amount(), m.currency})
}

func (m *Money) UnmarshalJSON(data []byte) error {
	if m == nil {
		return fail("INVALID_MONEY", "money destination is nil")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return fail("INVALID_MONEY", "money must be an object")
	}
	fields := make(map[string]string, 2)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return fail("INVALID_MONEY", "invalid money object")
		}
		key, ok := token.(string)
		if !ok || (key != "amount" && key != "currency") {
			return fail("INVALID_MONEY", "unknown money field")
		}
		if _, exists := fields[key]; exists {
			return fail("INVALID_MONEY", "duplicate money field")
		}
		var value string
		if err := decoder.Decode(&value); err != nil {
			return fail("INVALID_MONEY", "money fields must be strings")
		}
		fields[key] = value
	}
	if token, err = decoder.Token(); err != nil || token != json.Delim('}') {
		return fail("INVALID_MONEY", "invalid money object")
	}
	if _, err = decoder.Token(); err != io.EOF {
		return fail("INVALID_MONEY", "unexpected data after money")
	}
	parsed, err := ParseMoney(fields["amount"], fields["currency"])
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
