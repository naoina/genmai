package genmai

import (
	"database/sql/driver"
	"fmt"
	"math/big"
)

type (
	Float32 float32
	Float64 float64
)

// Rat is an wrapper of the Rat of math/big.
// However, Rat implements the sql Scanner interface.
type Rat struct {
	*big.Rat
}

// NewRat returns a new Rat.
// This is the similar to NewRat of math/big.
func NewRat(a, b int64) *Rat {
	return &Rat{
		Rat: big.NewRat(a, b),
	}
}

// Scan implements the database/sql Scanner interface.
func (rat *Rat) Scan(src interface{}) (err error) {
	rat.Rat = new(big.Rat)
	switch t := src.(type) {
	case string:
		_, err = fmt.Sscan(t, rat.Rat)
	case []byte:
		_, err = fmt.Sscan(string(t), rat.Rat)
	case float64:
		rat.Rat.SetFloat64(t)
	default:
		_, err = fmt.Sscan(fmt.Sprint(t), rat.Rat)
	}
	return err
}

// Value implements the database/sql/driver Valuer interface.
func (rat Rat) Value() (driver.Value, error) {
	return rat.FloatString(decimalScale), nil
}
