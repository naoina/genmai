package genmai

import (
	"reflect"
	"testing"
)

func Test_ToInterfaceSlice(t *testing.T) {
	actual := ToInterfaceSlice([]string{"1", "hoge", "foo"})
	expected := []interface{}{"1", "hoge", "foo"}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expect %[1]q(type %[1]T), but %[2]q(type %[2]T)", expected, actual)
	}
}

func TestColumnName(t *testing.T) {
	for _, v := range []struct {
		tableName  string
		columnName string
		expected   string
	}{
		{`test_table`, `*`, `"test_table".*`},
		{`test_table`, `test_column`, `"test_table"."test_column"`},
		{``, `test_column`, `"test_column"`},
		{``, `*`, `*`},
	} {
		actual := ColumnName(&SQLite3Dialect{}, v.tableName, v.columnName)
		expected := v.expected
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}
}

func TestIsUnexportedField(t *testing.T) {
	// test for bug case less than Go1.3.
	func() {
		type b struct{}
		type C struct {
			b
		}
		v := reflect.TypeOf(C{}).Field(0)
		actual := IsUnexportedField(v)
		expected := true
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("IsUnexportedField(%q) => %v, want %v", v, actual, expected)
		}
	}()

	// test for correct case.
	func() {
		type B struct{}
		type C struct {
			B
		}
		v := reflect.TypeOf(C{}).Field(0)
		actual := IsUnexportedField(v)
		expected := false
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("IsUnexportedField(%q) => %v, want %v", v, actual, expected)
		}
	}()
}
