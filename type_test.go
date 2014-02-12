package genmai

import (
	"database/sql"
	"math/big"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestNewRat(t *testing.T) {
	actual := NewRat(1, 3)
	expected := &Rat{Rat: big.NewRat(1, 3)}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func TestRat_Scan(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`CREATE TABLE test_table (
			id integer,
			rstr numeric,
			rreal real
		);`,
		`INSERT INTO test_table (id, rstr, rreal) VALUES (1, '0.3', '0.4')`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	rstr := new(Rat)
	rreal := new(Rat)
	row := db.QueryRow(`SELECT rstr, rreal FROM test_table`)
	if err := row.Scan(rstr, rreal); err != nil {
		t.Fatal(err)
	}
	for _, v := range []struct {
		r     *Rat
		float float64
	}{{rstr, 0.3}, {rreal, 0.4}} {
		actual := v.r
		expected := &Rat{Rat: new(big.Rat).SetFloat64(v.float)}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%v expects %q, but %q", v.float, expected, actual)
		}
	}
}

func TestRat_Value(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{
		`CREATE TABLE test_table (
			id integer,
			r numeric
		);`,
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	r := &Rat{Rat: big.NewRat(3, 10)}
	if _, err := db.Exec(`INSERT INTO test_table (id, r) VALUES (1, ?);`, r); err != nil {
		t.Fatal(err)
	}
	row := db.QueryRow(`SELECT r FROM test_table`)
	var s string
	if err := row.Scan(&s); err != nil {
		t.Fatal(err)
	}
	actual := s
	expected := "0.3"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}
