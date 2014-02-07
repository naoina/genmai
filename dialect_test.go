package genmai

import (
	"reflect"
	"testing"
)

func Test_SQLite3Dialect_Name(t *testing.T) {
	d := &SQLite3Dialect{}
	actual := d.Name()
	expected := "sqlite3"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func Test_SQLite3Dialect_Quote(t *testing.T) {
	d := &SQLite3Dialect{}
	for _, v := range []struct {
		s, expected string
	}{
		{``, `""`},
		{`test`, `"test"`},
		{`"test"`, `"""test"""`},
		{`test"bar"baz`, `"test""bar""baz"`},
	} {
		actual := d.Quote(v.s)
		expected := v.expected
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Input %q expects %q, but %q", v.s, expected, actual)
		}
	}
}

func Test_SQLite3Dialect_PlaceHolder(t *testing.T) {
	d := &SQLite3Dialect{}
	actual := d.PlaceHolder(0)
	expected := "?"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func Test_MySQLDialect_Name(t *testing.T) {
	d := &MySQLDialect{}
	actual := d.Name()
	expected := "mysql"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}

func Test_MySQLDialect_Quote(t *testing.T) {
	d := &MySQLDialect{}
	for _, v := range []struct {
		s, expected string
	}{
		{"", "``"},
		{"test", "`test`"},
		{"`test`", "```test```"},
		{"test`bar`baz", "`test``bar``baz`"},
	} {
		actual := d.Quote(v.s)
		expected := v.expected
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Input %q expects %q, but %q", v.s, expected, actual)
		}
	}
}

func Test_MySQLDialect_PlaceHolder(t *testing.T) {
	d := &MySQLDialect{}
	actual := d.PlaceHolder(0)
	expected := "?"
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %q, but %q", expected, actual)
	}
}
