package genmai

import (
	"reflect"
	"testing"
)

func TestToCamelCase(t *testing.T) {
	for v, expected := range map[string]string{
		"genmai":   "Genmai",
		"GenmaI":   "GenmaI",
		"genma_i":  "GenmaI",
		"G_en_mai": "GEnMai",
		"g_En_maI": "GEnMaI",
	} {
		actual := ToCamelCase(v)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%v: Expect %v, but %v", v, expected, actual)
		}
	}
}

func TestToSnakeCase(t *testing.T) {
	for v, expected := range map[string]string{
		"genmai":  "genmai",
		"Genmai":  "genmai",
		"genmaI":  "genma_i",
		"gEnmAi":  "g_enm_ai",
		"gen_mai": "gen_mai",
	} {
		actual := ToSnakeCase(v)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("%v: Expect %v, but %v", v, expected, actual)
		}
	}
}

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
	} {
		actual := ColumnName(&SQLite3Dialect{}, v.tableName, v.columnName)
		expected := v.expected
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Expect %q, but %q", expected, actual)
		}
	}
}
